import os, json, logging, uuid
from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from redis import Redis

from .operations_evidence import router as operations_evidence
from .database import Base, SessionLocal, engine, get_db
from .models import AnalysisJob, AuditLog, ConsensusReview, ManholeAsset, RepairZone, Resurvey, SurveyBatch, SurveyPoint, User, WorkNotice
from .schemas import BatchIn, ConsensusReviewIn, LoginIn, NoticeIn, ResurveyIn, SurveyPointIn
from .security import create_token, current_user, require_roles, verify_password
from .services.audit import record
from .services.cluster import build_input
from .services.consensus import expire_review, pair_key
from .services.geometry import bounds, overlaps
from .middlewares.audit import RequestAuditMiddleware
from .middlewares.rate_limit import SimpleRateLimitMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manhole-settlement")
app = FastAPI(title="城市排水井盖沉降热区", version="1.0.0")
app.include_router(operations_evidence)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])
app.add_middleware(RequestAuditMiddleware)
app.add_middleware(SimpleRateLimitMiddleware)
redis_client = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

@app.on_event("startup")
def startup():
    Base.metadata.create_all(engine)
    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE analysis_jobs ADD COLUMN IF NOT EXISTS job_type VARCHAR(20) NOT NULL DEFAULT 'cluster'"))
    if os.getenv("SEED_DEMO", "true").lower() == "true":
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:
            logger.exception("seed_demo failed during startup")

@app.get("/health")
def health():
    return {"status": "ok", "service": "manhole-settlement-api"}

def iso(value):
    return value.isoformat() if value else None

def user_view(user):
    return {"id": user.id, "username": user.username, "display_name": user.display_name, "role": user.role}

def asset_view(a):
    return {"id": a.id, "asset_code": a.asset_code, "street_name": a.street_name, "x": a.x, "y": a.y, "coordinate_reference": a.coordinate_reference, "cover_type": a.cover_type, "depth_mm": a.depth_mm, "status": a.status, "last_inspected_at": iso(a.last_inspected_at)}

def point_view(p):
    return {"id": p.id, "point_code": p.point_code, "batch_id": p.batch_id, "manhole_id": p.manhole_id, "asset_code": p.manhole.asset_code if p.manhole else "", "street_name": p.manhole.street_name if p.manhole else "", "x": p.x, "y": p.y, "elevation_mm": p.elevation_mm, "settlement_mm": p.settlement_mm, "coordinate_reference": p.coordinate_reference, "measured_at": iso(p.measured_at), "quality_status": p.quality_status, "cluster_label": p.cluster_label, "local_density": p.local_density}

def zone_view(z, db):
    notice = db.get(WorkNotice, z.conflict_notice_id) if z.conflict_notice_id else None
    batch = db.get(SurveyBatch, z.batch_id)
    point_rows = db.execute(select(SurveyPoint).where(SurveyPoint.id.in_(z.point_ids or []))).scalars().all() if z.point_ids else []
    return {"id": z.id, "zone_code": z.zone_code, "batch_id": z.batch_id, "batch_code": batch.batch_code if batch else "", "cluster_label": z.cluster_label, "name": z.name, "status": z.status, "point_ids": z.point_ids or [], "point_codes": [p.point_code for p in point_rows], "center_x": z.center_x, "center_y": z.center_y, "average_settlement_mm": z.average_settlement_mm, "max_settlement_mm": z.max_settlement_mm, "local_density": z.local_density, "boundary": z.boundary, "original_cluster_evidence": z.original_cluster_evidence, "split_from_id": z.split_from_id, "conflict_notice_id": z.conflict_notice_id, "split_reason": z.split_reason, "conflict_road": notice.road_name if notice else None}

def notice_view(n):
    return {"id": n.id, "notice_code": n.notice_code, "zone_id": n.zone_id, "road_name": n.road_name, "contractor": n.contractor, "permit_no": n.permit_no, "starts_at": iso(n.starts_at), "ends_at": iso(n.ends_at), "boundary": n.boundary, "status": n.status, "lane_closure_required": n.lane_closure_required, "conflict_note": n.conflict_note}

def resurvey_view(r):
    return {"id": r.id, "record_code": r.record_code, "zone_id": r.zone_id, "measured_at": iso(r.measured_at), "settlement_before_mm": r.settlement_before_mm, "settlement_after_mm": r.settlement_after_mm, "delta_mm": r.delta_mm, "pass_fail": r.pass_fail, "status": r.status, "verified_by": r.verified_by, "notes": r.notes}

def piece_view(p):
    return {"id": p.id, "piece_code": p.piece_code, "asset_codes": p.asset_codes or [], "manhole_ids": p.manhole_ids or [], "point_count": p.point_count, "team_a_average_settlement_mm": p.team_a_average_settlement_mm, "team_b_average_settlement_mm": p.team_b_average_settlement_mm, "team_a_max_settlement_mm": p.team_a_max_settlement_mm, "team_b_max_settlement_mm": p.team_b_max_settlement_mm, "pass_fail": p.pass_fail, "fail_reason": p.fail_reason}

def review_view(r, db):
    batch_a = db.get(SurveyBatch, r.batch_a_id)
    batch_b = db.get(SurveyBatch, r.batch_b_id)
    return {"id": r.id, "review_code": r.review_code, "batch_a_id": r.batch_a_id, "batch_b_id": r.batch_b_id, "batch_a_code": batch_a.batch_code if batch_a else "", "batch_b_code": batch_b.batch_code if batch_b else "", "batch_a_instrument": batch_a.instrument if batch_a else "", "batch_b_instrument": batch_b.instrument if batch_b else "", "status": r.status, "summary": r.summary or {}, "expire_reason": r.expire_reason, "created_at": iso(r.created_at), "confirmed_at": iso(r.confirmed_at), "expired_at": iso(r.expired_at), "pieces": [piece_view(p) for p in r.pieces]}

def consensus_manhole_states(db):
    reviews = db.execute(select(ConsensusReview)).scalars().all()
    pieces_by_review = {r.id: list(r.pieces) for r in reviews}
    states = {}
    def mark(manhole_id, state, review_code, reason):
        if manhole_id not in states:
            states[manhole_id] = {"manhole_id": manhole_id, "state": state, "review_code": review_code, "reason": reason}
    active = [r for r in reviews if r.status == "active"]
    for r in active:
        for p in pieces_by_review.get(r.id, []):
            if p.pass_fail == "pass":
                for mid in p.manhole_ids or []:
                    mark(mid, "confirmed", r.review_code, "两队复算均达标")
    for r in active:
        for p in pieces_by_review.get(r.id, []):
            if p.pass_fail != "pass":
                for mid in p.manhole_ids or []:
                    mark(mid, "pending", r.review_code, p.fail_reason or "复算未两边达标")
    for r in reviews:
        if r.status != "expired":
            continue
        for p in pieces_by_review.get(r.id, []):
            for mid in p.manhole_ids or []:
                mark(mid, "pending", r.review_code, "共识已过期，待换批复核")
    if not states:
        return []
    assets = {a.id: a for a in db.execute(select(ManholeAsset).where(ManholeAsset.id.in_(states.keys()))).scalars().all()}
    return sorted(([{**s, "asset_code": assets[m].asset_code if m in assets else str(m), "street_name": assets[m].street_name if m in assets else ""} for m, s in states.items()]), key=lambda x: x["asset_code"])

@app.post("/api/auth/login")
def login(payload: LoginIn, db: Session = Depends(get_db)):
    user = db.execute(select(User).where(User.username == payload.username)).scalar_one_or_none()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="账号或密码错误")
    return {"access_token": create_token(user), "token_type": "bearer", "user": user_view(user)}

@app.get("/api/me")
def me(user: User = Depends(current_user)):
    return user_view(user)

@app.get("/api/dashboard")
def dashboard(db: Session = Depends(get_db), user: User = Depends(current_user)):
    batches = db.execute(select(SurveyBatch).order_by(SurveyBatch.measured_at.desc())).scalars().all()
    assets = db.execute(select(ManholeAsset).order_by(ManholeAsset.asset_code)).scalars().all()
    points = db.execute(select(SurveyPoint).order_by(SurveyPoint.id)).scalars().all()
    zones = db.execute(select(RepairZone).order_by(RepairZone.id)).scalars().all()
    notices = db.execute(select(WorkNotice).order_by(WorkNotice.id)).scalars().all()
    resurveys = db.execute(select(Resurvey).order_by(Resurvey.id.desc())).scalars().all()
    jobs = db.execute(select(AnalysisJob).order_by(AnalysisJob.id.desc())).scalars().all()
    reviews = db.execute(select(ConsensusReview).order_by(ConsensusReview.id.desc())).scalars().all()
    manhole_states = consensus_manhole_states(db)
    return {"user": user_view(user), "stats": {"assets": len(assets), "points": len(points), "zones": len(zones), "open_zones": sum(z.status not in ["closed", "split"] for z in zones), "notices": len(notices), "resurveys": len(resurveys), "consensus_confirmed": sum(1 for m in manhole_states if m["state"] == "confirmed"), "consensus_pending": sum(1 for m in manhole_states if m["state"] == "pending")}, "batches": [{"id": b.id, "batch_code": b.batch_code, "measured_at": iso(b.measured_at), "instrument": b.instrument, "coordinate_reference": b.coordinate_reference, "status": b.status, "notes": b.notes, "point_count": len(b.points)} for b in batches], "assets": [asset_view(a) for a in assets], "points": [point_view(p) for p in points], "zones": [zone_view(z, db) for z in zones], "notices": [notice_view(n) for n in notices], "resurveys": [resurvey_view(r) for r in resurveys], "jobs": [{"id": j.id, "job_code": j.job_code, "batch_id": j.batch_id, "job_type": j.job_type or "cluster", "status": j.status, "result": j.result, "error": j.error, "finished_at": iso(j.finished_at)} for j in jobs], "consensus": {"reviews": [review_view(r, db) for r in reviews], "manholes": manhole_states}}

@app.get("/api/survey-batches/{batch_id}")
def batch_detail(batch_id: int, db: Session = Depends(get_db), user: User = Depends(current_user)):
    batch = db.get(SurveyBatch, batch_id)
    if not batch:
        raise HTTPException(404, "测量批次不存在")
    return {"id": batch.id, "batch_code": batch.batch_code, "measured_at": iso(batch.measured_at), "instrument": batch.instrument, "coordinate_reference": batch.coordinate_reference, "status": batch.status, "notes": batch.notes, "points": [point_view(p) for p in batch.points]}

@app.post("/api/survey-batches")
def create_batch(payload: BatchIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    if db.execute(select(SurveyBatch).where(SurveyBatch.batch_code == payload.batch_code)).scalar_one_or_none():
        raise HTTPException(409, "批次编号已存在")
    batch = SurveyBatch(batch_code=payload.batch_code, measured_at=payload.measured_at, instrument=payload.instrument, coordinate_reference=payload.coordinate_reference, notes=payload.notes, status="open")
    db.add(batch); record(db, user.id, "create_batch", "SurveyBatch", payload.batch_code, {"coordinate_reference": payload.coordinate_reference}); db.commit(); db.refresh(batch)
    return {"id": batch.id, "batch_code": batch.batch_code, "status": batch.status}

@app.post("/api/survey-batches/{batch_id}/points")
def add_point(batch_id: int, payload: SurveyPointIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    batch = db.get(SurveyBatch, batch_id)
    asset = db.get(ManholeAsset, payload.manhole_id)
    if not batch or not asset:
        raise HTTPException(404, "批次或井盖资产不存在")
    if batch.status == "withdrawn":
        raise HTTPException(409, "批次已撤回，不能追加测量点")
    if batch.status in ["clustered", "queued"]:
        raise HTTPException(409, "批次已进入聚类流程，不能追加测量点")
    if db.execute(select(SurveyPoint).where(SurveyPoint.point_code == payload.point_code)).scalar_one_or_none():
        raise HTTPException(409, "测量点编号已存在")
    point = SurveyPoint(batch_id=batch_id, **payload.model_dump())
    db.add(point); record(db, user.id, "add_survey_point", "SurveyPoint", payload.point_code, {"batch_code": batch.batch_code}); db.commit(); db.refresh(point)
    return point_view(point)

@app.post("/api/survey-points/{point_id}/correct-datum")
def correct_datum(point_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    point = db.get(SurveyPoint, point_id)
    if not point:
        raise HTTPException(404, "测量点不存在")
    batch = db.get(SurveyBatch, point.batch_id)
    if batch.status == "withdrawn":
        raise HTTPException(409, "批次已撤回，不能修正坐标基准")
    old = point.coordinate_reference; point.coordinate_reference = batch.coordinate_reference; point.quality_status = "datum_corrected"
    if batch.status == "blocked": batch.status = "ready"
    record(db, user.id, "correct_coordinate_reference", "SurveyPoint", point.point_code, {"from": old, "to": point.coordinate_reference}); db.commit()
    return {"point_code": point.point_code, "coordinate_reference": point.coordinate_reference, "batch_status": batch.status}

@app.post("/api/survey-batches/{batch_id}/cluster")
def enqueue_cluster(batch_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    batch = db.get(SurveyBatch, batch_id)
    if not batch:
        raise HTTPException(404, "测量批次不存在")
    if batch.status == "withdrawn":
        raise HTTPException(409, "批次已撤回，不能发起聚类")
    points = build_input(db, batch)
    refs = {batch.coordinate_reference, *(p["coordinate_reference"] for p in points)}
    if len(refs) != 1:
        batch.status = "blocked"
        record(db, user.id, "block_cluster_datum_conflict", "SurveyBatch", batch.batch_code, {"references": sorted(refs)})
        db.commit()
        conflicts = [p["point_code"] for p in points if p["coordinate_reference"] != batch.coordinate_reference]
        raise HTTPException(status_code=409, detail={"code": "DATUM_CONFLICT", "message": "测量点存在坐标基准冲突，禁止聚类", "point_codes": conflicts, "batch_status": batch.status})
    stale_reviews = db.execute(select(ConsensusReview).where(ConsensusReview.active_pair_key.isnot(None), or_(ConsensusReview.batch_a_id == batch.id, ConsensusReview.batch_b_id == batch.id))).scalars().all()
    for review in stale_reviews:
        expire_review(review, f"测量批次 {batch.batch_code} 重新聚类，共识过期待复核")
    job = AnalysisJob(job_code=f"JOB-{batch.batch_code}-{uuid.uuid4().hex[:10]}", batch_id=batch.id, status="queued", input_summary={"points": points, "params": {"eps_m": 26.0, "min_samples": 3}})
    batch.status = "queued"; db.add(job); record(db, user.id, "enqueue_spatial_cluster", "AnalysisJob", job.job_code, {"batch_code": batch.batch_code, "point_count": len(points), "expired_reviews": [r.review_code for r in stale_reviews]}); db.commit(); db.refresh(job)
    redis_client.rpush("manhole_settlement_jobs", str(job.id))
    return {"job_id": job.id, "job_code": job.job_code, "status": job.status}

@app.post("/api/survey-batches/{batch_id}/withdraw")
def withdraw_batch(batch_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    batch = db.get(SurveyBatch, batch_id)
    if not batch:
        raise HTTPException(404, "测量批次不存在")
    if batch.status == "withdrawn":
        raise HTTPException(409, "批次已撤回")
    batch.status = "withdrawn"
    reviews = db.execute(select(ConsensusReview).where(ConsensusReview.active_pair_key.isnot(None), or_(ConsensusReview.batch_a_id == batch.id, ConsensusReview.batch_b_id == batch.id))).scalars().all()
    for review in reviews:
        expire_review(review, f"测量批次 {batch.batch_code} 已撤回，共识过期")
    record(db, user.id, "withdraw_survey_batch", "SurveyBatch", batch.batch_code, {"expired_reviews": [r.review_code for r in reviews]})
    db.commit()
    return {"batch_code": batch.batch_code, "status": batch.status, "expired_reviews": len(reviews)}

@app.get("/api/analysis-jobs/{job_id}")
def job_detail(job_id: int, db: Session = Depends(get_db), user: User = Depends(current_user)):
    job = db.get(AnalysisJob, job_id)
    if not job: raise HTTPException(404, "分析任务不存在")
    return {"id": job.id, "job_code": job.job_code, "job_type": job.job_type or "cluster", "status": job.status, "result": job.result, "error": job.error, "finished_at": iso(job.finished_at)}

@app.get("/api/consensus-reviews")
def consensus_reviews(db: Session = Depends(get_db), user: User = Depends(current_user)):
    reviews = db.execute(select(ConsensusReview).order_by(ConsensusReview.id.desc())).scalars().all()
    return {"reviews": [review_view(r, db) for r in reviews], "manholes": consensus_manhole_states(db)}

@app.post("/api/consensus-reviews")
def create_consensus_review(payload: ConsensusReviewIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    if payload.batch_a_id == payload.batch_b_id:
        raise HTTPException(422, "需选择两个不同的测量批次")
    batch_a = db.get(SurveyBatch, payload.batch_a_id)
    batch_b = db.get(SurveyBatch, payload.batch_b_id)
    if not batch_a or not batch_b:
        raise HTTPException(404, "测量批次不存在")
    for batch in (batch_a, batch_b):
        if batch.status == "withdrawn":
            raise HTTPException(409, f"批次 {batch.batch_code} 已撤回，需换批复核")
        if batch.status != "clustered":
            raise HTTPException(409, f"批次 {batch.batch_code} 尚未完成聚类，不能复核")
    key = pair_key(batch_a.id, batch_b.id)
    existing = db.execute(select(ConsensusReview).where(ConsensusReview.active_pair_key == key)).scalar_one_or_none()
    if existing:
        return {"review": review_view(existing, db), "deduplicated": True, "job": None}
    code = f"CR-{batch_a.batch_code.replace('SURV-2026-', '')}-{batch_b.batch_code.replace('SURV-2026-', '')}-{uuid.uuid4().hex[:8]}"
    review = ConsensusReview(review_code=code, batch_a_id=batch_a.id, batch_b_id=batch_b.id, pair_key=key, active_pair_key=key, status="queued", created_by=user.id)
    db.add(review)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        existing = db.execute(select(ConsensusReview).where(ConsensusReview.active_pair_key == key)).scalar_one()
        return {"review": review_view(existing, db), "deduplicated": True, "job": None}
    job = AnalysisJob(job_code=f"JOB-{code}", batch_id=batch_a.id, job_type="consensus", status="queued", input_summary={"review_id": review.id, "batch_a_id": batch_a.id, "batch_b_id": batch_b.id, "batch_a_code": batch_a.batch_code, "batch_b_code": batch_b.batch_code, "params": {"eps_m": 26.0, "min_samples": 3, "settlement_threshold_mm": 8.0, "min_piece_manholes": 2}})
    db.add(job)
    record(db, user.id, "enqueue_consensus_review", "ConsensusReview", code, {"batch_a": batch_a.batch_code, "batch_b": batch_b.batch_code})
    db.commit(); db.refresh(job)
    redis_client.rpush("manhole_settlement_jobs", str(job.id))
    return {"review": review_view(review, db), "deduplicated": False, "job": {"id": job.id, "job_code": job.job_code, "status": job.status}}

@app.get("/api/zones")
def zones(db: Session = Depends(get_db), user: User = Depends(current_user)):
    return [zone_view(z, db) for z in db.execute(select(RepairZone).order_by(RepairZone.id)).scalars().all()]

@app.post("/api/zones/{zone_id}/split")
def split_zone(zone_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status != "needs_split": raise HTTPException(409, "当前片区没有待拆分的道路边界冲突")
    points = db.execute(select(SurveyPoint).where(SurveyPoint.id.in_(zone.point_ids))).scalars().all()
    notice = db.get(WorkNotice, zone.conflict_notice_id)
    ordered = sorted(points, key=lambda p: (p.x, p.y))
    split_x = (notice.boundary["min_x"] + notice.boundary["max_x"]) / 2 if notice else ordered[len(ordered)//2].x
    left = [p for p in ordered if p.x <= split_x]
    right = [p for p in ordered if p.x > split_x]
    if not left or not right:
        half = max(1, len(ordered)//2); left, right = ordered[:half], ordered[half:]
    zone.status = "split"; zone.split_reason = f"依据 {notice.road_name if notice else '道路边界'} 在 x={round(split_x, 2)} 拆分，保留原始聚类证据"
    children = []
    for idx, members in enumerate([left, right], 1):
        b = bounds([{ "x": p.x, "y": p.y } for p in members])
        child = RepairZone(zone_code=f"{zone.zone_code}-S{idx}", batch_id=zone.batch_id, cluster_label=zone.cluster_label, name=f"{zone.name} / 边界片 {idx}", status="ready_for_notice", point_ids=[p.id for p in members], center_x=round(sum(p.x for p in members)/len(members),2), center_y=round(sum(p.y for p in members)/len(members),2), average_settlement_mm=round(sum(p.settlement_mm for p in members)/len(members),2), max_settlement_mm=max(p.settlement_mm for p in members), local_density=zone.local_density, boundary=b, original_cluster_evidence={"split_from": zone.zone_code, "split_axis": "x", "split_x": split_x, "source_evidence": zone.original_cluster_evidence}, split_from_id=zone.id)
        db.add(child); children.append(child)
    record(db, user.id, "split_zone_by_road_boundary", "RepairZone", zone.zone_code, {"children": [f"{zone.zone_code}-S1", f"{zone.zone_code}-S2"], "split_x": split_x})
    db.commit()
    return {"source_zone": zone.zone_code, "children": [c.zone_code for c in children], "status": zone.status, "split_reason": zone.split_reason}

@app.post("/api/zones/{zone_id}/work-notices")
def create_notice(zone_id: int, payload: NoticeIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status not in ["proposed", "ready_for_notice"]: raise HTTPException(409, "当前片区不能发起施工联动")
    notice = WorkNotice(notice_code=f"WN-{zone.zone_code}-{int(datetime.now().timestamp())}", zone_id=zone.id, road_name=payload.road_name, contractor=payload.contractor, permit_no=payload.permit_no, starts_at=payload.starts_at, ends_at=payload.ends_at, boundary=zone.boundary, status="submitted", lane_closure_required=payload.lane_closure_required, created_by=user.id)
    db.add(notice); zone.status = "notice_sent"; record(db, user.id, "submit_work_notice", "WorkNotice", notice.notice_code, {"zone_code": zone.zone_code}); db.commit(); db.refresh(notice)
    return notice_view(notice)

@app.post("/api/work-notices/{notice_id}/acknowledge")
def acknowledge_notice(notice_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    notice = db.get(WorkNotice, notice_id)
    if not notice: raise HTTPException(404, "施工联动单不存在")
    if notice.status != "submitted": raise HTTPException(409, "施工联动单不在待确认状态")
    notice.status = "active"; record(db, user.id, "acknowledge_work_notice", "WorkNotice", notice.notice_code, {"status": "active"}); db.commit()
    return notice_view(notice)

@app.post("/api/zones/{zone_id}/start-repair")
def start_repair(zone_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    notice = db.execute(select(WorkNotice).where(WorkNotice.zone_id == zone.id, WorkNotice.status == "active")).scalar_one_or_none()
    if not notice: raise HTTPException(409, "施工联动单未激活，不能开始维修")
    zone.status = "repairing"; record(db, user.id, "start_repair", "RepairZone", zone.zone_code, {"notice_code": notice.notice_code}); db.commit()
    return zone_view(zone, db)

@app.post("/api/zones/{zone_id}/resurvey")
def submit_resurvey(zone_id: int, payload: ResurveyIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status != "repairing": raise HTTPException(409, "片区未进入维修中，不能提交复测")
    after = round(payload.settlement_after_mm, 2); before = zone.average_settlement_mm; delta = round(before - after, 2); pass_fail = "pass" if after <= 8.0 else "fail"
    record_code = f"RS-{zone.zone_code}-{int(datetime.now().timestamp())}"
    resurvey = Resurvey(record_code=record_code, zone_id=zone.id, measured_at=payload.measured_at, settlement_before_mm=before, settlement_after_mm=after, delta_mm=delta, pass_fail=pass_fail, status="submitted", notes=payload.notes)
    db.add(resurvey); zone.status = "awaiting_resurvey"; record(db, user.id, "submit_resurvey", "Resurvey", record_code, {"zone_code": zone.zone_code, "pass_fail": pass_fail}); db.commit(); db.refresh(resurvey)
    return resurvey_view(resurvey)

@app.post("/api/resurveys/{resurvey_id}/verify")
def verify_resurvey(resurvey_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    resurvey = db.get(Resurvey, resurvey_id)
    if not resurvey: raise HTTPException(404, "复测记录不存在")
    if resurvey.status != "submitted": raise HTTPException(409, "复测记录不在待核验状态")
    zone = db.get(RepairZone, resurvey.zone_id)
    resurvey.verified_by = user.id
    if resurvey.pass_fail == "pass":
        resurvey.status = "verified"; zone.status = "closed"
        notice = db.execute(select(WorkNotice).where(WorkNotice.zone_id == zone.id)).scalar_one_or_none()
        if notice: notice.status = "completed"
    else:
        resurvey.status = "rejected"; zone.status = "repairing"
    record(db, user.id, "verify_resurvey", "Resurvey", resurvey.record_code, {"result": resurvey.pass_fail, "zone_status": zone.status}); db.commit()
    return {"resurvey": resurvey_view(resurvey), "zone": zone_view(zone, db)}

@app.get("/api/audit-logs")
def audit_logs(db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor", "viewer"))):
    rows = db.execute(select(AuditLog).order_by(AuditLog.id.desc()).limit(80)).scalars().all()
    return [{"id": r.id, "action": r.action, "entity_type": r.entity_type, "entity_id": r.entity_id, "detail": r.detail, "created_at": iso(r.created_at)} for r in rows]
