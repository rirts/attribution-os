import os, io, json, math, hashlib
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from collections import defaultdict

from dotenv import load_dotenv
import boto3
import pandas as pd

# --- Carga .env.dev / .env ---
for p in ["./.env.dev", "./.env"]:
    if os.path.exists(p):
        load_dotenv(p); break

S3_ENDPOINT     = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_REGION       = os.getenv("S3_REGION", "us-east-1")
AWS_ACCESS      = os.getenv("MINIO_ROOT_USER", "admin")
AWS_SECRET      = os.getenv("MINIO_ROOT_PASSWORD", "adminadmin")
BUCKET_SILVER   = os.getenv("S3_BUCKET_SILVER", "dp-silver")
BUCKET_GOLD     = os.getenv("S3_BUCKET_GOLD", "dp-gold")

# Parámetros de sesión/atribución (puedes cambiarlos luego)
SESSION_TIMEOUT_MIN = int(os.getenv("SESSION_TIMEOUT_MIN", "30"))
LOOKBACK_DAYS       = int(os.getenv("ATTR_LOOKBACK_DAYS", "7"))
TIMEDECAY_HALFLIFE_D = float(os.getenv("ATTR_TIMEDECAY_HALFLIFE_D", "7"))

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS,
    aws_secret_access_key=AWS_SECRET,
    region_name=S3_REGION,
)

# ---------- util S3 ----------
def list_parquet(prefix: str) -> List[str]:
    keys: List[str] = []
    token = None
    while True:
        resp = s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix, ContinuationToken=token) if token else \
               s3.list_objects_v2(Bucket=BUCKET_SILVER, Prefix=prefix)
        for it in resp.get("Contents", []):
            if it["Key"].endswith(".parquet"):
                keys.append(it["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def read_parquet(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET_SILVER, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    return pd.read_parquet(buf)

def write_parquet_gold(df: pd.DataFrame, prefix: str) -> str:
    # asegura bucket
    try: s3.head_bucket(Bucket=BUCKET_GOLD)
    except Exception: s3.create_bucket(Bucket=BUCKET_GOLD)

    buf = io.BytesIO(); df.to_parquet(buf, index=False); buf.seek(0)
    ts = datetime.now(timezone.utc).strftime("%H%M%S%f")
    key = f"{prefix}/part-{ts}.parquet"
    s3.put_object(Bucket=BUCKET_GOLD, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
    return key

# ---------- helpers ----------
def parse_ts(x: Any) -> Optional[pd.Timestamp]:
    if x is None or (isinstance(x, float) and math.isnan(x)): return None
    try:
        return pd.to_datetime(x, utc=True)
    except Exception:
        try:
            return pd.to_datetime(str(x).replace("Z","+00:00"), utc=True)
        except Exception:
            return None

def channel_from_row(r) -> str:
    src = (r.get("utm_source") or "").strip().lower()
    med = (r.get("utm_medium") or "").strip().lower()
    if not src and not med:
        return "direct/none"
    return f"{src or 'unknown'}/{med or 'none'}"

def user_key_from_row(r) -> Optional[str]:
    for k in ["ids_uid", "ids_cookie", "ids_ga"]:
        v = r.get(k)
        if v is not None and str(v).strip() != "":
            return f"{k}:{str(v)}"
    # fallback pobre (no ideal): hash de user agent + lang
    raw = (r.get("client_ua") or "") + "|" + (r.get("client_lang") or "")
    return "ua:"+hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def new_session(prev_ts: Optional[pd.Timestamp], cur_ts: pd.Timestamp) -> bool:
    if prev_ts is None: return True
    return (cur_ts - prev_ts) > pd.Timedelta(minutes=SESSION_TIMEOUT_MIN)

def time_decay_weight(delta_days: float, halflife_d: float) -> float:
    # w = 0.5^(delta/halflife)
    return pow(0.5, max(delta_days, 0.0)/halflife_d)

# ---------- build (lee TODO web2 de silver) ----------
def load_all_web2() -> pd.DataFrame:
    ks = list_parquet("web2/")
    if not ks:
        return pd.DataFrame()
    dfs = [read_parquet(k) for k in ks]
    df = pd.concat(dfs, ignore_index=True)
    # columnas necesarias
    need = ["event_id","ts","type","url","utm_source","utm_medium","utm_campaign","utm_content","utm_term",
            "client_ua","client_lang","ids_cookie","ids_ga","ids_uid","properties_json"]
    for c in need:
        if c not in df.columns: df[c] = None
    df["ts"] = df["ts"].apply(parse_ts)
    df = df.dropna(subset=["ts"])
    df["channel"] = df.apply(channel_from_row, axis=1)
    df["user_key"] = df.apply(user_key_from_row, axis=1)
    # valor de conversión si existe
    vals = []
    for s in df["properties_json"].fillna("{}").astype(str):
        try:
            v = json.loads(s).get("value", 0.0)
            vals.append(float(v) if v is not None else 0.0)
        except Exception:
            vals.append(0.0)
    df["conv_value"] = pd.Series(vals)
    return df

def build_sessions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "session_id","user_key","start_ts","end_ts","n_events","channels","conv_count","conv_value_sum"
        ])
    df = df.sort_values(["user_key","ts"]).reset_index(drop=True)

    sess_ids = []
    start_ts = []
    end_ts = []
    n_events = []
    channels = []
    conv_count = []
    conv_value_sum = []

    current_session = None
    last_ts = None
    current_user = None
    cur_events = 0
    chan_set = set()
    convs = 0
    conv_sum = 0.0

    rows = []
    for _, r in df.iterrows():
        uk = r["user_key"]
        t  = r["ts"]
        if (current_user != uk) or new_session(last_ts, t):
            # flush
            if current_session is not None:
                rows.append([current_session, current_user, st, last_ts, cur_events, ",".join(sorted(chan_set)), convs, conv_sum])
            # start new
            st = t
            base = f"{uk}|{int(st.value)}"
            current_session = hashlib.md5(base.encode("utf-8")).hexdigest()
            current_user = uk
            cur_events = 0
            chan_set = set()
            convs = 0
            conv_sum = 0.0
        # add event
        cur_events += 1
        chan_set.add(r["channel"])
        if str(r["type"]).lower() in ("lead","purchase"):
            convs += 1
            conv_sum += float(r.get("conv_value",0.0) or 0.0)
        last_ts = t
    # flush last
    if current_session is not None:
        rows.append([current_session, current_user, st, last_ts, cur_events, ",".join(sorted(chan_set)), convs, conv_sum])

    sess_df = pd.DataFrame(rows, columns=[
        "session_id","user_key","start_ts","end_ts","n_events","channels","conv_count","conv_value_sum"
    ])
    return sess_df

def build_attribution(df: pd.DataFrame) -> pd.DataFrame:
    # Considera cada conversión y asigna crédito a los touchpoints previos en ventana
    if df.empty:
        return pd.DataFrame(columns=[
            "conv_event_id","conv_ts","conv_value","model","channel","credit"
        ])

    df = df.sort_values(["user_key","ts"]).reset_index(drop=True)

    # Por simplicidad: touchpoints = eventos pageview/click
    is_tp = df["type"].str.lower().isin(["pageview","click"])
    is_conv = df["type"].str.lower().isin(["lead","purchase"])

    rows = []

    for uk, g in df.groupby("user_key"):
        g = g.reset_index(drop=True)
        tps = g[is_tp].copy()
        cvs = g[is_conv].copy()
        if cvs.empty: 
            continue

        for _, conv in cvs.iterrows():
            conv_ts = conv["ts"]
            conv_val = float(conv.get("conv_value", 0.0) or 0.0)
            lookback_start = conv_ts - pd.Timedelta(days=LOOKBACK_DAYS)
            window = tps[(tps["ts"] <= conv_ts) & (tps["ts"] >= lookback_start)]
            if window.empty:
                # atribuir a "direct/none"
                rows.append([conv["event_id"], conv_ts, conv_val, "last_touch", "direct/none", conv_val])
                rows.append([conv["event_id"], conv_ts, conv_val, "linear", "direct/none", conv_val])
                rows.append([conv["event_id"], conv_ts, conv_val, "u_shaped", "direct/none", conv_val])
                rows.append([conv["event_id"], conv_ts, conv_val, "time_decay", "direct/none", conv_val])
                continue

            # LAST-TOUCH
            last_ch = window.iloc[-1]["channel"]
            rows.append([conv["event_id"], conv_ts, conv_val, "last_touch", last_ch, conv_val])

            # LINEAR
            uniq = window["channel"].tolist()
            if len(uniq) > 0:
                per = conv_val / len(uniq) if conv_val else (1.0/len(uniq))
                for ch in uniq:
                    rows.append([conv["event_id"], conv_ts, conv_val, "linear", ch, per])

            # U-SHAPED (40% first, 40% last, 20% resto)
            uniq = window["channel"].tolist()
            n = len(uniq)
            if n == 1:
                rows.append([conv["event_id"], conv_ts, conv_val, "u_shaped", uniq[0], conv_val])
            else:
                first = uniq[0]; last = uniq[-1]; middle = uniq[1:-1]
                first_c = conv_val * 0.4; last_c = conv_val * 0.4
                rows.append([conv["event_id"], conv_ts, conv_val, "u_shaped", first, first_c])
                rows.append([conv["event_id"], conv_ts, conv_val, "u_shaped", last,  last_c])
                if len(middle) > 0:
                    per_mid = (conv_val * 0.2) / len(middle)
                    for ch in middle:
                        rows.append([conv["event_id"], conv_ts, conv_val, "u_shaped", ch, per_mid])

            # TIME-DECAY (half-life en días)
            wts = []
            for ts, ch in zip(window["ts"], window["channel"]):
                delta_days = max((conv_ts - ts).total_seconds()/86400.0, 0.0)
                w = pow(0.5, delta_days / TIMEDECAY_HALFLIFE_D)
                wts.append((ch, w))
            denom = sum(w for _,w in wts) or 1.0
            for ch, w in wts:
                rows.append([conv["event_id"], conv_ts, conv_val, "time_decay", ch, (conv_val * (w/denom))])

    attrib = pd.DataFrame(rows, columns=["conv_event_id","conv_ts","conv_value","model","channel","credit"])
    # compactación: agrupa por conversión/modelo/canal
    if not attrib.empty:
        attrib = attrib.groupby(["conv_event_id","conv_ts","conv_value","model","channel"], as_index=False)["credit"].sum()
    return attrib

def main():
    df = load_all_web2()
    if df.empty:
        print("No hay datos web2 en silver aún."); return

    sessions = build_sessions(df)
    attrib = build_attribution(df)

    # Particionamos por fecha (UTC) del start de sesión / conv
    if not sessions.empty:
        sessions["date"] = sessions["start_ts"].dt.date.astype(str)
        for dt, g in sessions.groupby("date"):
            key = write_parquet_gold(g.drop(columns=["date"]), f"web2_sessions/date={dt}")
            print("GOLD sessions →", key)

    if not attrib.empty:
        attrib["date"] = pd.to_datetime(attrib["conv_ts"], utc=True).dt.date.astype(str)
        for dt, g in attrib.groupby("date"):
            key = write_parquet_gold(g.drop(columns=["date"]), f"web2_attribution/date={dt}")
            print("GOLD attribution →", key)

if __name__ == "__main__":
    main()
