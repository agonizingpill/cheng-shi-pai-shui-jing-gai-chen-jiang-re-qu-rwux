"""两队测量批次的共识复核。

按井盖取两队原热区交集，交集井盖各用本队坐标重新 DBSCAN 聚类，
再按两队簇标签交叉拆片；每片分别用两批测量值复算均值，
两边都达到沉降达标线且片内井盖数足够的片才登记为共识确认片。
"""
from datetime import datetime, timezone

import numpy as np
from sklearn.cluster import DBSCAN
from sqlalchemy import select

from ..models import AnalysisJob, ConsensusPiece, ConsensusReview, ManholeAsset, RepairZone, SurveyBatch, SurveyPoint
from .audit import record

EPS_METERS = 26.0
MIN_SAMPLES = 3
MIN_PIECE_MANHOLES = 2
SETTLEMENT_THRESHOLD_MM = 8.0


def pair_key(batch_a_id, batch_b_id):
    low, high = sorted((int(batch_a_id), int(batch_b_id)))
    return f"{low}:{high}"


def expire_review(review, reason):
    review.status = "expired"
    review.expire_reason = reason
    review.expired_at = datetime.now(timezone.utc)
    review.active_pair_key = None


def zone_manhole_ids(db, batch_id):
    zones = db.execute(select(RepairZone).where(RepairZone.batch_id == batch_id, RepairZone.status != "split")).scalars().all()
    point_ids = [pid for zone in zones for pid in (zone.point_ids or [])]
    if not point_ids:
        return set()
    points = db.execute(select(SurveyPoint).where(SurveyPoint.id.in_(point_ids))).scalars().all()
    return {p.manhole_id for p in points}


def batch_points_by_manhole(db, batch_id, manhole_ids):
    points = db.execute(select(SurveyPoint).where(SurveyPoint.batch_id == batch_id, SurveyPoint.manhole_id.in_(manhole_ids))).scalars().all()
    return {p.manhole_id: p for p in points}


def fail_review(review, job, message, shared_count=None):
    if review and review.status == "queued":
        review.status = "failed"
        review.active_pair_key = None
        review.summary = {"shared_manholes": shared_count} if shared_count is not None else (review.summary or {})
    job.status = "failed"
    job.error = message
    job.finished_at = datetime.now(timezone.utc)


def run_consensus(db, job_id):
    job = db.get(AnalysisJob, job_id)
    if not job:
        return
    summary = job.input_summary or {}
    review = db.get(ConsensusReview, summary.get("review_id"))
    batch_a = db.get(SurveyBatch, summary.get("batch_a_id"))
    batch_b = db.get(SurveyBatch, summary.get("batch_b_id"))
    try:
        job.status = "running"
        db.commit()
        withdrawn = [b.batch_code for b in (batch_a, batch_b) if b is None or b.status == "withdrawn"]
        if withdrawn:
            # 撤回与出片同时提交时撤回优先：旧任务不得发布共识结果
            reason = f"测量批次 {'、'.join(withdrawn)} 已撤回，共识结果不予发布"
            if review and review.status in ("queued", "active"):
                expire_review(review, reason)
            job.status = "failed"
            job.error = reason
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return
        if not review or review.status != "queued":
            job.status = "failed"
            job.error = "共识登记已失效，任务结果不予发布"
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return
        shared = sorted(zone_manhole_ids(db, batch_a.id) & zone_manhole_ids(db, batch_b.id))
        if len(shared) < MIN_SAMPLES:
            fail_review(review, job, f"两队原热区按井盖取交集仅 {len(shared)} 枚，不足 {MIN_SAMPLES} 枚，无法成片", len(shared))
            db.commit()
            return
        points_a = batch_points_by_manhole(db, batch_a.id, shared)
        points_b = batch_points_by_manhole(db, batch_b.id, shared)
        labels_a = DBSCAN(eps=EPS_METERS, min_samples=MIN_SAMPLES).fit_predict(np.array([[points_a[m].x, points_a[m].y] for m in shared], dtype=float))
        labels_b = DBSCAN(eps=EPS_METERS, min_samples=MIN_SAMPLES).fit_predict(np.array([[points_b[m].x, points_b[m].y] for m in shared], dtype=float))
        groups = {}
        for manhole_id, label_a, label_b in zip(shared, labels_a, labels_b):
            if label_a == -1 or label_b == -1:
                continue  # 任一队复算为噪声的井盖不成片
            groups.setdefault((int(label_a), int(label_b)), []).append(manhole_id)
        if not groups:
            fail_review(review, job, "交集井盖在两队各自坐标复算下均无法成片", len(shared))
            db.commit()
            return
        assets = {a.id: a for a in db.execute(select(ManholeAsset).where(ManholeAsset.id.in_(shared))).scalars().all()}
        confirmed, pending = set(), set()
        for index, ((label_a, label_b), members) in enumerate(sorted(groups.items()), 1):
            members = sorted(members)
            side_a = [points_a[m] for m in members]
            side_b = [points_b[m] for m in members]
            avg_a = round(sum(p.settlement_mm for p in side_a) / len(side_a), 2)
            avg_b = round(sum(p.settlement_mm for p in side_b) / len(side_b), 2)
            reasons = []
            if len(members) < MIN_PIECE_MANHOLES:
                reasons.append(f"片内仅 {len(members)} 枚井盖，不足 {MIN_PIECE_MANHOLES} 枚")
            if avg_a < SETTLEMENT_THRESHOLD_MM:
                reasons.append(f"{batch_a.batch_code} 复算均值 {avg_a} mm 低于 {SETTLEMENT_THRESHOLD_MM} mm 达标线")
            if avg_b < SETTLEMENT_THRESHOLD_MM:
                reasons.append(f"{batch_b.batch_code} 复算均值 {avg_b} mm 低于 {SETTLEMENT_THRESHOLD_MM} mm 达标线")
            pass_fail = "fail" if reasons else "pass"
            (pending if reasons else confirmed).update(members)
            db.add(ConsensusPiece(
                piece_code=f"{review.review_code}-P{index}", review_id=review.id,
                cluster_labels={"batch_a": label_a, "batch_b": label_b},
                manhole_ids=members, asset_codes=[assets[m].asset_code for m in members],
                point_count=len(members),
                team_a_average_settlement_mm=avg_a, team_b_average_settlement_mm=avg_b,
                team_a_max_settlement_mm=round(max(p.settlement_mm for p in side_a), 2),
                team_b_max_settlement_mm=round(max(p.settlement_mm for p in side_b), 2),
                pass_fail=pass_fail, fail_reason="；".join(reasons) or None,
            ))
        pending -= confirmed
        review.status = "active"
        review.confirmed_at = datetime.now(timezone.utc)
        review.summary = {
            "shared_manholes": len(shared), "piece_count": len(groups),
            "confirmed_manholes": len(confirmed), "pending_manholes": len(pending),
            "settlement_threshold_mm": SETTLEMENT_THRESHOLD_MM, "min_piece_manholes": MIN_PIECE_MANHOLES,
        }
        job.result = {"review_code": review.review_code, **review.summary}
        job.status = "succeeded"
        job.finished_at = datetime.now(timezone.utc)
        record(db, None, "publish_consensus_review", "ConsensusReview", review.review_code, review.summary)
        db.commit()
    except Exception as exc:
        if review and review.status == "queued":
            review.status = "failed"
            review.active_pair_key = None
        job.status = "failed"
        job.error = str(exc)
        job.finished_at = datetime.now(timezone.utc)
        db.commit()
