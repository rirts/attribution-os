import dotenv from "dotenv";
import * as path from "path";
import * as fs from "fs";
import { randomUUID } from "crypto";
import { S3Client, PutObjectCommand, HeadBucketCommand, CreateBucketCommand } from "@aws-sdk/client-s3";

// --- .env.dev preferido ---
const candidates = [
  path.resolve(process.cwd(), "../../.env.dev"),
  path.resolve(process.cwd(), "../../.env"),
  path.resolve(process.cwd(), ".env")
];
for (const p of candidates) { if (fs.existsSync(p)) { dotenv.config({ path: p }); break; } }

// --- Config ---
const BUCKET = process.env.S3_BUCKET_RAW || "dp-raw";
const ENDPOINT = process.env.S3_ENDPOINT || "http://localhost:9000";
const REGION = process.env.S3_REGION || "us-east-1";
const ACCESS_KEY = process.env.MINIO_ROOT_USER || "admin";
const SECRET_KEY = process.env.MINIO_ROOT_PASSWORD || "adminadmin";
// API base (tokenless)
const MEMPOOL_API_BASE = (process.env.MEMPOOL_API_BASE || "https://mempool.space/api").replace(/\/+$/,"");

// --- S3 (MinIO) ---
const s3 = new S3Client({
  region: REGION,
  endpoint: ENDPOINT,
  forcePathStyle: true,
  credentials: { accessKeyId: ACCESS_KEY, secretAccessKey: SECRET_KEY },
});

// --- Utils ---
function isoNow(): string { return new Date().toISOString(); }
function ymd(d: Date = new Date()): string { return d.toISOString().slice(0,10); }
async function ensureBucketExists(bucket: string): Promise<void> {
  try { await s3.send(new HeadBucketCommand({ Bucket: bucket })); }
  catch { await s3.send(new CreateBucketCommand({ Bucket: bucket })); }
}
async function putNDJSON(key: string, obj: any): Promise<void> {
  const line = JSON.stringify(obj) + "\n";
  await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: line, ContentType: "application/x-ndjson" }));
}
async function getJson(pathRel: string): Promise<any> {
  const url = MEMPOOL_API_BASE + pathRel;
  const res = await fetch(url);
  if (!res.ok) throw new Error("HTTP " + res.status + " " + url);
  return res.json();
}

// --- Pollers ---
const seenTx = new Set<string>();
async function pollMempoolOnce() {
  try {
    const arr: any[] = await getJson("/mempool/recent"); // últimos txs observados
    const when = isoNow();
    for (const tx of arr || []) {
      const txid = String(tx?.txid || randomUUID());
      if (seenTx.has(txid)) continue;
      seenTx.add(txid);
      const key = "chain/mempool/date=" + ymd() + "/tx_" + txid + ".jsonl";
      await putNDJSON(key, { source: "mempool.space", kind: "mempool_recent", fetched_at: when, data: tx });
      console.log("[onchain] mempool saved:", key);
    }
  } catch (e:any) {
    console.error("[onchain] mempool error:", e.message || e);
  }
}

const seenBlocks = new Set<number>();
async function pollBlocksOnce() {
  try {
    const arr: any[] = await getJson("/blocks"); // últimos ~10 bloques
    const when = isoNow();
    for (const b of arr || []) {
      const height = Number(b?.height ?? -1);
      if (!Number.isFinite(height) || height < 0) continue;
      if (seenBlocks.has(height)) continue;
      seenBlocks.add(height);
      const id = String(b?.id || randomUUID());
      const key = "chain/blocks/date=" + ymd() + "/block_" + height + "_" + id + ".jsonl";
      await putNDJSON(key, { source: "mempool.space", kind: "block", fetched_at: when, data: b });
      console.log("[onchain] block saved:", key);
    }
  } catch (e:any) {
    console.error("[onchain] blocks error:", e.message || e);
  }
}

async function main() {
  await ensureBucketExists(BUCKET);
  console.log("[onchain] writing raw to bucket:", BUCKET);
  // Primera pasada inmediata
  await Promise.all([pollMempoolOnce(), pollBlocksOnce()]);
  // Luego intervalos
  setInterval(pollMempoolOnce, 5000);   // cada 5s
  setInterval(pollBlocksOnce, 60000);   // cada 60s
}
main().catch(err => { console.error(err); process.exit(1); });