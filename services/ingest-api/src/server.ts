import Fastify from "fastify";
import cors from "@fastify/cors";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
import { randomUUID } from "crypto";
import * as path from "path";
import * as fs from "fs";

// --- Carga variables (.env.dev preferido) ---
const candidates = [
  path.resolve(process.cwd(), "../../.env.dev"),
  path.resolve(process.cwd(), "../../.env"),
  path.resolve(process.cwd(), ".env"),
];
for (const p of candidates) {
  if (fs.existsSync(p)) { dotenv.config({ path: p }); break; }
}

const PORT = Number(process.env.INGEST_API_PORT || 8088);
const BUCKET = process.env.S3_BUCKET_RAW || "dp-raw";
const ENDPOINT = process.env.S3_ENDPOINT || "http://localhost:9000";
const REGION = process.env.S3_REGION || "us-east-1";
const ACCESS_KEY = process.env.MINIO_ROOT_USER || "admin";
const SECRET_KEY = process.env.MINIO_ROOT_PASSWORD || "adminadmin";

// --- S3 (MinIO) ---
const s3 = new S3Client({
  region: REGION,
  endpoint: ENDPOINT,
  forcePathStyle: true,
  credentials: { accessKeyId: ACCESS_KEY, secretAccessKey: SECRET_KEY },
});

// --- Utilidades ---
function isoNow(): string { return new Date().toISOString(); }
function ymd(d: Date = new Date()): string { return d.toISOString().slice(0,10); }
function isString(x: any): x is string { return typeof x === "string"; }
function toStr(x: any, def = ""): string { return isString(x) ? x : def; }

const ALLOWED_TYPES = new Set(["pageview","click","lead","purchase"]);

function normalizeEvent(body: any) {
  const now = isoNow();

  const type = toStr(body?.type);
  if (!ALLOWED_TYPES.has(type)) throw new Error("invalid type");

  const url = toStr(body?.url);
  try { new URL(url); } catch { throw new Error("invalid url"); }

  const ts = toStr(body?.ts) || now;
  const tsIso = new Date(ts).toISOString(); // normaliza (si inválido, lanza)
  const event_id = toStr(body?.event_id) || randomUUID();

  const utm = {
    source: toStr(body?.utm?.source),
    medium: toStr(body?.utm?.medium),
    campaign: toStr(body?.utm?.campaign),
    content: toStr(body?.utm?.content),
    term: toStr(body?.utm?.term),
  };
  const client = {
    ip: toStr(body?.client?.ip),
    ua: toStr(body?.client?.ua),
    lang: toStr(body?.client?.lang),
  };
  const ids = {
    cookie: toStr(body?.ids?.cookie),
    ga: toStr(body?.ids?.ga),
    uid: isString(body?.ids?.uid) ? body.ids.uid : null,
    email_sha256: isString(body?.ids?.email_sha256) ? body.ids.email_sha256 : null,
  };
  const device = {
    os: toStr(body?.device?.os),
    browser: toStr(body?.device?.browser),
    device: toStr(body?.device?.device),
  };
  const properties = (body && typeof body.properties === "object" && body.properties !== null)
    ? body.properties : {};

  return {
    event_id, ts: tsIso, type, url,
    referrer: toStr(body?.referrer),
    utm, client, ids, device, properties
  };
}

function buildS3Key(tsISO: string, eventId: string): string {
  return "web2/date=" + tsISO.slice(0,10) + "/event_" + eventId + ".jsonl";
}

// --- App ---
function buildApp() {
  const app = Fastify({ logger: false });
  app.register(cors, { origin: true });

  app.get("/health", async () => ({ ok: true, time: isoNow() }));

  app.post("/v1/event", async (req, reply) => {
    try {
      const parsed = normalizeEvent((req as any).body ?? {});
      const bodyLine = JSON.stringify(parsed) + "\n";
      const key = buildS3Key(parsed.ts, parsed.event_id);

      await s3.send(new PutObjectCommand({
        Bucket: BUCKET, Key: key, Body: bodyLine, ContentType: "application/x-ndjson"
      }));

      return reply.code(200).send({ ok: true, bucket: BUCKET, key });
    } catch (err: any) {
      const msg = (err && err.message) ? err.message : String(err);
      return reply.code(400).send({ ok: false, error: msg });
    }
  });

  return app;
}

const app = buildApp();
app.listen({ port: PORT, host: "0.0.0.0" }, (err) => {
  if (err) { console.error(err); process.exit(1); }
  console.log("[ingest-api] listening on http://localhost:" + PORT);
});