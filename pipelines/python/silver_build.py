import os, io, json, sys
from datetime import datetime, timezone
from typing import List
from dotenv import load_dotenv
import boto3
import pandas as pd

for p in ["./.env.dev", "./.env"]:
    if os.path.exists(p):
        load_dotenv(p); break

S3_ENDPOINT    = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_REGION      = os.getenv("S3_REGION", "us-east-1")
AWS_ACCESS     = os.getenv("MINIO_ROOT_USER", "admin")
AWS_SECRET     = os.getenv("MINIO_ROOT_PASSWORD", "adminadmin")
BUCKET_BRONZE  = os.getenv("S3_BUCKET_BRONZE", "dp-bronze")
BUCKET_SILVER  = os.getenv("S3_BUCKET_SILVER", "dp-silver")

s3 = boto3.client("s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS,
    aws_secret_access_key=AWS_SECRET,
    region_name=S3_REGION,
)

def ensure_bucket(bucket: str):
    try: s3.head_bucket(Bucket=bucket)
    except Exception: s3.create_bucket(Bucket=bucket)

def list_parquet(prefix: str) -> List[str]:
    keys: List[str] = []; token = None
    while True:
        resp = s3.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=prefix, ContinuationToken=token) if token else \
               s3.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=prefix)
        for it in resp.get("Contents", []):
            if it["Key"].endswith(".parquet"): keys.append(it["Key"])
        if resp.get("IsTruncated"): token = resp.get("NextContinuationToken")
        else: break
    return keys

def read_parquet(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET_BRONZE, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    return pd.read_parquet(buf)

def to_parquet_silver(df: pd.DataFrame, silver_prefix: str) -> str:
    ensure_bucket(BUCKET_SILVER)
    buf = io.BytesIO(); df.to_parquet(buf, index=False); buf.seek(0)
    key = f"{silver_prefix}/part-{datetime.now(timezone.utc).strftime('%H%M%S%f')}.parquet"
    s3.put_object(Bucket=BUCKET_SILVER, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
    return key

from typing import Optional, Union
def parse_ts(x: Union[str,int,float,None]) -> Optional[str]:
    if x is None: return None
    try:
        if isinstance(x, (int,float)):
            return datetime.fromtimestamp(float(x), tz=timezone.utc).isoformat()
        return datetime.fromisoformat(str(x).replace("Z","+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return None

def build_web2(date_str: str) -> List[str]:
    prefix = f"web2/date={date_str}/"; ks = list_parquet(prefix)
    if not ks: return []
    df = pd.concat([read_parquet(k) for k in ks], ignore_index=True)
    df["event_id"] = df["event_id"].astype("string")
    df["ts"] = df["ts"].apply(parse_ts).astype("string")
    df["type"] = df["type"].astype("string")
    for c in ["url","referrer","utm_source","utm_medium","utm_campaign","utm_content","utm_term",
              "client_ua","client_lang","ids_cookie","ids_ga","device_os","device_browser","device_device"]:
        if c in df.columns: df[c] = df[c].fillna("").astype("string")
    if "event_id" in df.columns:
        df = df.sort_values("ts").drop_duplicates(subset=["event_id"], keep="last")
    try:
        from urllib.parse import urlparse
        parsed = df["url"].apply(lambda u: urlparse(u) if isinstance(u,str) else None)
        df["url_host"] = parsed.apply(lambda p: p.netloc if p else "")
        df["url_path"] = parsed.apply(lambda p: p.path if p else "")
    except Exception:
        df["url_host"] = ""; df["url_path"] = ""
    return [to_parquet_silver(df, f"web2/date={date_str}")]

def build_chain_mempool(date_str: str) -> List[str]:
    ks = list_parquet(f"chain_mempool/date={date_str}/")
    if not ks: return []
    df = pd.concat([read_parquet(k) for k in ks], ignore_index=True)
    for c in ["vsize","fee","value"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df["txid"] = df["txid"].astype("string")
    if "first_seen" in df.columns: df["first_seen"] = df["first_seen"].apply(parse_ts).astype("string")
    if "fetched_at" in df.columns: df["fetched_at"] = df["fetched_at"].apply(parse_ts).astype("string")
    if "fee" in df.columns and "vsize" in df.columns:
        df["fee_rate_sat_vb"] = (df["fee"].astype("float") / df["vsize"].astype("float")).replace([float("inf")], None)
    df = df.sort_values(df.columns.tolist()).drop_duplicates(subset=["txid"], keep="last")
    return [to_parquet_silver(df, f"chain_mempool/date={date_str}")]

def build_chain_blocks(date_str: str) -> List[str]:
    ks = list_parquet(f"chain_blocks/date={date_str}/")
    if not ks: return []
    df = pd.concat([read_parquet(k) for k in ks], ignore_index=True)
    for c in ["height","tx_count","size","weight"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    if "timestamp" in df.columns: df["timestamp"] = df["timestamp"].apply(parse_ts).astype("string")
    df["id"] = df["id"].astype("string")
    if "height" in df.columns:
        df = df.sort_values("timestamp").drop_duplicates(subset=["height"], keep="last")
    return [to_parquet_silver(df, f"chain_blocks/date={date_str}")]

def main():
    # --date opcional
    date_arg = None
    if len(sys.argv) > 2 and sys.argv[1] == "--date":
        date_arg = sys.argv[2]
    date_str = date_arg or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    ensure_bucket(BUCKET_SILVER)
    written: List[str] = []
    try: written += build_web2(date_str)
    except Exception as e: print("[silver] web2 skip:", e)
    try: written += build_chain_mempool(date_str)
    except Exception as e: print("[silver] mempool skip:", e)
    try: written += build_chain_blocks(date_str)
    except Exception as e: print("[silver] blocks skip:", e)

    if written:
        print("SILVER OK →"); [print(" ", k) for k in written]
    else:
        print("No se escribió nada en silver para", date_str)

if __name__ == "__main__":
    main()
