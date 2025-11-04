import os, io, json, argparse
from datetime import datetime, timezone
from typing import List, Dict, Any

from dotenv import load_dotenv
import boto3
import pandas as pd

# --- Carga .env.dev de la raíz (si existe) ---
for p in ["./.env.dev", "./.env"]:
    if os.path.exists(p):
        load_dotenv(p)
        break

S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_REGION     = os.getenv("S3_REGION", "us-east-1")
AWS_ACCESS    = os.getenv("MINIO_ROOT_USER", "admin")
AWS_SECRET    = os.getenv("MINIO_ROOT_PASSWORD", "adminadmin")
BUCKET_RAW    = os.getenv("S3_BUCKET_RAW", "dp-raw")
BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "dp-bronze")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS,
    aws_secret_access_key=AWS_SECRET,
    region_name=S3_REGION,
)

def list_keys(prefix: str) -> List[str]:
    keys: List[str] = []
    token = None
    while True:
        resp = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix=prefix, ContinuationToken=token) if token else \
               s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix=prefix)
        for it in resp.get("Contents", []):
            if it["Key"].endswith(".jsonl"):
                keys.append(it["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def read_jsonl(key: str) -> List[Dict[str, Any]]:
    obj = s3.get_object(Bucket=BUCKET_RAW, Key=key)
    data = obj["Body"].read()
    rows: List[Dict[str, Any]] = []
    for line in data.splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            rows.append({"raw_text": line.decode("utf-8", errors="ignore")})
    return rows

def to_parquet_upload(df: pd.DataFrame, bronze_key_prefix: str) -> str:
    # WINDOWS-SAFE: escribir Parquet a memoria y subirlo directo (sin archivos temporales)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    key = f"{bronze_key_prefix}/part-{datetime.now(timezone.utc).strftime('%H%M%S%f')}.parquet"
    s3.put_object(Bucket=BUCKET_BRONZE, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
    return key

def normalize_web2(date_str: str):
    prefix = f"web2/date={date_str}/"
    keys = list_keys(prefix)
    if not keys:
        return []
    records = []
    for k in keys:
        for r in read_jsonl(k):
            utm = r.get("utm",{}) or {}
            client = r.get("client",{}) or {}
            ids = r.get("ids",{}) or {}
            device = r.get("device",{}) or {}
            rec = {
                "event_id": r.get("event_id"),
                "ts": r.get("ts"),
                "type": r.get("type"),
                "url": r.get("url"),
                "referrer": r.get("referrer"),
                "utm_source": utm.get("source",""),
                "utm_medium": utm.get("medium",""),
                "utm_campaign": utm.get("campaign",""),
                "utm_content": utm.get("content",""),
                "utm_term": utm.get("term",""),
                "client_ua": client.get("ua",""),
                "client_lang": client.get("lang",""),
                "ids_cookie": ids.get("cookie",""),
                "ids_ga": ids.get("ga",""),
                "ids_uid": ids.get("uid"),
                "ids_email_sha256": ids.get("email_sha256"),
                "device_os": device.get("os",""),
                "device_browser": device.get("browser",""),
                "device_device": device.get("device",""),
                "properties_json": json.dumps(r.get("properties",{}) or {}, ensure_ascii=False),
                "_raw_key": k,
            }
            records.append(rec)
    if not records:
        return []
    df = pd.DataFrame.from_records(records)
    bronze_prefix = f"web2/date={date_str}"
    return [to_parquet_upload(df, bronze_prefix)]

def normalize_chain_mempool(date_str: str):
    prefix = f"chain/mempool/date={date_str}/"
    keys = list_keys(prefix)
    if not keys:
        return []
    rows = []
    for k in keys:
        for r in read_jsonl(k):
            data = r.get("data", {}) or {}
            rows.append({
                "txid": data.get("txid"),
                "vsize": data.get("vsize"),
                "fee": data.get("fee"),
                "value": data.get("value"),
                "first_seen": data.get("time") or r.get("fetched_at"),
                "fetched_at": r.get("fetched_at"),
                "raw_json": json.dumps(r, ensure_ascii=False),
                "_raw_key": k,
            })
    if not rows:
        return []
    df = pd.DataFrame.from_records(rows)
    bronze_prefix = f"chain_mempool/date={date_str}"
    return [to_parquet_upload(df, bronze_prefix)]

def normalize_chain_blocks(date_str: str):
    prefix = f"chain/blocks/date={date_str}/"
    keys = list_keys(prefix)
    if not keys:
        return []
    rows = []
    for k in keys:
        for r in read_jsonl(k):
            data = r.get("data", {}) or {}
            rows.append({
                "height": data.get("height"),
                "id": data.get("id"),
                "timestamp": data.get("timestamp") or data.get("time"),
                "tx_count": data.get("tx_count") or data.get("tx_count_approx"),
                "size": data.get("size"),
                "weight": data.get("weight"),
                "raw_json": json.dumps(r, ensure_ascii=False),
                "_raw_key": k,
            })
    if not rows:
        return []
    df = pd.DataFrame.from_records(rows)
    bronze_prefix = f"chain_blocks/date={date_str}"
    return [to_parquet_upload(df, bronze_prefix)]

def main():
    import sys
    # Fecha por defecto: hoy UTC (timezone-aware, sin deprecations)
    date_str = None
    if len(sys.argv) > 2 and sys.argv[1] == "--date":
        date_str = sys.argv[2]
    if not date_str:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Asegura bucket bronze
    try:
        s3.head_bucket(Bucket=BUCKET_BRONZE)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_BRONZE)

    written = []
    written += normalize_web2(date_str)
    written += normalize_chain_mempool(date_str)
    written += normalize_chain_blocks(date_str)

    if written:
        print("BRONZE OK →")
        for w in written:
            print(" ", w)
    else:
        print("No se encontraron archivos en dp-raw para la fecha", date_str)

if __name__ == "__main__":
    main()
