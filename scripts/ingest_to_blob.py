import os
import sys
import hashlib
import uuid
from datetime import datetime, timezone
import csv
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
LOGS_DIR = os.path.join(ROOT, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOGS_DIR, "ingestion_log.csv")

def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def append_log(rows):
    header = ["run_id","file_name","blob_path","bytes","md5","status","error","ts_utc"]
    exists = os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    load_dotenv()
    conn = os.getenv("AZURE_CONN_STR")
    container = os.getenv("CONTAINER_NAME", "financial-data")
    if not conn:
        print("Missing AZURE_CONN_STR", file=sys.stderr)
        sys.exit(1)
    bsc = BlobServiceClient.from_connection_string(conn)
    client = bsc.get_container_client(container)
    run_id = str(uuid.uuid4())
    today = datetime.now(timezone.utc)
    prefix = f"raw/{today.year:04d}/{today.month:02d}/{today.day:02d}"
    results = []
    for fn in os.listdir(RAW_DIR):
        if not fn.lower().endswith(".csv"):
            continue
        local_path = os.path.join(RAW_DIR, fn)
        blob_path = f"{prefix}/{fn}"
        size = os.path.getsize(local_path)
        digest = md5_file(local_path)
        status = "success"
        err = ""
        try:
            with open(local_path, "rb") as data:
                client.upload_blob(name=blob_path, data=data, overwrite=True)
        except Exception as e:
            status = "failed"
            err = str(e)
        results.append({
            "run_id": run_id,
            "file_name": fn,
            "blob_path": blob_path,
            "bytes": size,
            "md5": digest,
            "status": status,
            "error": err,
            "ts_utc": datetime.now(timezone.utc).isoformat()
        })
    append_log(results)
    print(f"Ingestion completed for run_id={run_id}")

if __name__ == "__main__":
    main()
