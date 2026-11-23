import os, time
from pathlib import Path
from redis import Redis
from .database import Base, SessionLocal, engine
from .models import AnalysisJob
from .services.cluster import cluster_batch
from .services.consensus import run_consensus

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
SHARED_DIR = Path(os.getenv("SHARED_DIR", "/shared"))
queue = Redis.from_url(REDIS_URL, decode_responses=True)

def dispatch(db, job_id):
    job = db.get(AnalysisJob, job_id)
    if not job:
        return
    if job.job_type == "consensus":
        run_consensus(db, job_id)
    else:
        cluster_batch(db, job_id)

def main():
    Base.metadata.create_all(engine)
    SHARED_DIR.mkdir(parents=True, exist_ok=True)
    while True:
        (SHARED_DIR / "worker-heartbeat").touch()
        item = queue.blpop("manhole_settlement_jobs", timeout=3)
        if item:
            db = SessionLocal()
            try:
                dispatch(db, int(item[1]))
            finally:
                db.close()
        time.sleep(1)

if __name__ == "__main__":
    main()
