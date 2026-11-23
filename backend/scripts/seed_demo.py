from datetime import datetime, timezone
from sqlalchemy import select
from src.database import Base, SessionLocal, engine
from src.models import User, ManholeAsset, SurveyBatch, SurveyPoint, WorkNotice
from src.security import hash_password

def dt(day, hour, minute=0):
    return datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)

def get_or_create(db, model, where, values):
    row = db.execute(select(model).filter_by(**where)).scalar_one_or_none()
    if row:
        return row, False
    row = model(**where, **values); db.add(row); db.flush(); return row, True

def seed():
    Base.metadata.create_all(engine)
    db = SessionLocal()
    try:
        for username, display, password, role in [
            ("operator", "林岚", "operator123", "operator"),
            ("supervisor", "顾航", "supervisor123", "supervisor"),
            ("viewer", "周宁", "viewer123", "viewer"),
        ]:
            get_or_create(db, User, {"username": username}, {"display_name": display, "password_hash": hash_password(password), "role": role})
        assets = []
        for i in range(1, 21):
            if i <= 10:
                x, y = 1010 + (i % 5) * 8, 1980 + (i // 5) * 12
                street = "南河路"
            else:
                x, y = 1110 + ((i - 11) % 5) * 8, 1980 + ((i - 11) // 5) * 12
                street = "西环路"
            asset, _ = get_or_create(db, ManholeAsset, {"asset_code": f"MH-{i:03d}"}, {"street_name": street, "x": x, "y": y, "coordinate_reference": "WGS84-METER", "cover_type": "球墨铸铁" if i % 2 else "复合井盖", "depth_mm": 2100 + i * 12, "status": "active", "last_inspected_at": dt(20, 7)})
            assets.append(asset)
        batch, _ = get_or_create(db, SurveyBatch, {"batch_code": "SURV-2026-1121-AM"}, {"measured_at": dt(21, 7, 30), "instrument": "Leica MS60 / RUN-1121-AM", "coordinate_reference": "WGS84-METER", "status": "open", "notes": "南河路与西环路交界区晨间复测"})
        points = db.execute(select(SurveyPoint).where(SurveyPoint.batch_id == batch.id)).scalars().all()
        if not points:
            for i, asset in enumerate(assets, 1):
                settlement = round(9.4 + (i % 4) * 1.7, 2) if i <= 10 else round(13.0 + (i % 5) * 1.9, 2)
                ref = "CGCS2000" if i == 20 else "WGS84-METER"
                db.add(SurveyPoint(point_code=f"SP-1121-{i:02d}", batch_id=batch.id, manhole_id=asset.id, x=asset.x, y=asset.y, elevation_mm=round(101230.0 - settlement, 2), settlement_mm=settlement, coordinate_reference=ref, measured_at=dt(21, 7, 30), quality_status="datum_conflict" if i == 20 else "checked"))
        batch_b, _ = get_or_create(db, SurveyBatch, {"batch_code": "SURV-2026-1121-PM"}, {"measured_at": dt(21, 15, 30), "instrument": "Leica TS16 / RUN-1121-PM", "coordinate_reference": "WGS84-METER", "status": "open", "notes": "乙队同路段下午复测，南河路东段断面偏移、西段沉降读数偏低"})
        if not db.execute(select(SurveyPoint).where(SurveyPoint.batch_id == batch_b.id)).scalars().first():
            for i, asset in enumerate(assets, 1):
                if 6 <= i <= 10:
                    x, y = asset.x + 40.5, asset.y + 31.5
                    settlement = round(4.9 + (i % 3) * 0.9, 2)
                else:
                    x = asset.x + 1.2 + (i % 3) * 0.8
                    y = asset.y - 0.9 + (i % 2) * 1.4
                    settlement = round(9.6 + (i % 4) * 1.6, 2) if i <= 10 else round(12.8 + (i % 5) * 1.8, 2)
                db.add(SurveyPoint(point_code=f"SP-1121-PM-{i:02d}", batch_id=batch_b.id, manhole_id=asset.id, x=round(x, 2), y=round(y, 2), elevation_mm=round(101230.0 - settlement, 2), settlement_mm=settlement, coordinate_reference="WGS84-METER", measured_at=dt(21, 15, 30), quality_status="checked"))
        batch_c, _ = get_or_create(db, SurveyBatch, {"batch_code": "SURV-2026-1122-AM"}, {"measured_at": dt(22, 7, 30), "instrument": "Leica TS16 / RUN-1122-AM", "coordinate_reference": "WGS84-METER", "status": "open", "notes": "乙队次日换批复测，用于替代撤回批次"})
        if not db.execute(select(SurveyPoint).where(SurveyPoint.batch_id == batch_c.id)).scalars().first():
            for i, asset in enumerate(assets, 1):
                settlement = round(9.5 + (i % 4) * 1.5, 2) if i <= 10 else round(12.9 + (i % 5) * 1.7, 2)
                db.add(SurveyPoint(point_code=f"SP-1122-{i:02d}", batch_id=batch_c.id, manhole_id=asset.id, x=round(asset.x + 0.6 + (i % 2) * 0.5, 2), y=round(asset.y - 0.4 + (i % 3) * 0.3, 2), elevation_mm=round(101230.0 - settlement, 2), settlement_mm=settlement, coordinate_reference="WGS84-METER", measured_at=dt(22, 7, 30), quality_status="checked"))
        supervisor = db.execute(select(User).where(User.username == "supervisor")).scalar_one()
        get_or_create(db, WorkNotice, {"notice_code": "WN-XH-20261121"}, {"zone_id": None, "road_name": "西环路雨污分流施工段", "contractor": "城建二公司市政一队", "permit_no": "SG-2026-XH-041", "starts_at": dt(21, 8), "ends_at": dt(25, 18), "boundary": {"min_x": 1098, "max_x": 1155, "min_y": 1968, "max_y": 2020}, "status": "active", "lane_closure_required": True, "conflict_note": "施工围挡占用西环路北侧慢车道", "created_by": supervisor.id})
        db.commit()
        print("seed_demo: users=3 assets=20 batches=3 points=60 notices=1")
    finally:
        db.close()

if __name__ == "__main__":
    seed()
