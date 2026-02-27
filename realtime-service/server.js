'use strict';

const express = require("express");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const createSubscriber = require("pg-listen");
const Redis = require("ioredis");
const { readEnv } = require("./env");

const app = express();
app.use(express.json({ limit: "1mb" }));

let ACCESS_SECRET, DATABASE_URL, REDIS_URL, PORT, LOG_LEVEL;
try {
  ACCESS_SECRET = readEnv("ACCESS_SECRET");
  DATABASE_URL = readEnv("DATABASE_URL");
  REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
  PORT = Number(process.env.PORT || 3003);
  LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
} catch (e) {
  console.error("❌", e.message);
  process.exit(1);
}

const INSTANCE = process.env.INSTANCE_ID || require("os").hostname();

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
function shouldLog(level) {
  const a = LEVELS[level] ?? 20;
  const b = LEVELS[LOG_LEVEL] ?? 20;
  return a >= b;
}
function log(level, msg, meta) {
  if (!shouldLog(level)) return;
  const out = { t: new Date().toISOString(), level, msg, meta: meta || undefined };
  console.log(JSON.stringify(out));
}

app.use((req, res, next) => {
  const t0 = Date.now();
  res.on("finish", () => {
    log("debug", "http", {
      method: req.method,
      path: req.originalUrl || req.url,
      code: res.statusCode,
      ms: Date.now() - t0,
    });
  });
  next();
});

const pool = new Pool({ connectionString: DATABASE_URL });
pool.on("error", (err) => log("error", "pg.pool.error", { err: err?.message || String(err) }));

const redis = new Redis(REDIS_URL, { enableAutoPipelining: false, lazyConnect: true });
redis.on("error", (e) => log("error", "redis.error", { err: e?.message || String(e) }));

let redisConnectPromise = null;
async function ensureRedisConnected() {
  try {
    if (redis.status === "ready") return true;
    if (redisConnectPromise) return redisConnectPromise;

    redisConnectPromise = redis
      .connect()
      .then(() => {
        log("info", "redis.connected", { status: redis.status });
        return true;
      })
      .catch((e) => {
        log("error", "redis.connect.failed", { err: e?.message || String(e), status: redis.status });
        return false;
      })
      .finally(() => {
        redisConnectPromise = null;
      });

    return redisConnectPromise;
  } catch (e) {
    log("error", "redis.connect.failed", { err: e?.message || String(e), status: redis.status });
    return false;
  }
}

const PRESENCE_TTL_SEC = Number(process.env.PRESENCE_TTL_SEC || 60);
function presenceKey(uid) {
  return `online:${uid}`;
}
async function touchPresence(uid) {
  const ok = await ensureRedisConnected();
  if (!ok || redis.status !== "ready") return;
  await redis.set(presenceKey(uid), "1", "EX", PRESENCE_TTL_SEC);
}
async function isOnline(uid) {
  const ok = await ensureRedisConnected();
  if (!ok || redis.status !== "ready") return false;
  const v = await redis.get(presenceKey(uid));
  return v !== null;
}
async function onlineBatch(ids) {
  const ok = await ensureRedisConnected();
  if (!ok || redis.status !== "ready") return [];

  const unique = Array.from(new Set(ids.map(Number).filter((x) => Number.isInteger(x) && x > 0))).slice(0, 200);
  if (!unique.length) return [];

  const pipe = redis.pipeline();
  for (const id of unique) pipe.get(presenceKey(id));
  const results = await pipe.exec();
  return unique.map((id, idx) => ({ user_id: id, online: results[idx]?.[1] !== null }));
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const authenticateToken = (req, res, next) => {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1];
  if (!token) return res.status(401).json({ error: "Token missing" });

  jwt.verify(token, ACCESS_SECRET, (err, payload) => {
    if (err) return res.status(401).json({ error: "Access token expired or invalid" });
    req.user = payload;
    next();
  });
};

const sseClients = new Map();
let sseConnSeq = 0;

function addSseClient(userId, res, scope) {
  const uid = Number(userId);
  if (!sseClients.has(uid)) sseClients.set(uid, new Set());
  const entry = { res, scope, connId: ++sseConnSeq, createdAt: Date.now() };
  sseClients.get(uid).add(entry);
  log("info", "sse.client.add", { uid, scope, connId: entry.connId, totalForUser: sseClients.get(uid).size });
  return entry;
}

function removeSseClient(userId, entry, reason) {
  const uid = Number(userId);
  const set = sseClients.get(uid);
  if (!set) return;
  set.delete(entry);
  if (set.size === 0) sseClients.delete(uid);
  log("info", "sse.client.remove", { uid, scope: entry.scope, connId: entry.connId, reason: reason || "unknown" });
}

function sseSend(res, obj, id = null) {
  const data = JSON.stringify(obj);
  if (id !== null && id !== undefined) res.write(`id: ${String(id)}\n`);
  res.write(`data: ${data}\n\n`);
}

function sseComment(res, text) {
  res.write(`: ${text}\n\n`);
}

function safeJson(s) {
  try {
    return JSON.parse(s);
  } catch (_) {
    return { raw: s };
  }
}

function broadcastToUser(uid, payload, scopeFilterFn = null, id = null) {
  const set = sseClients.get(Number(uid));
  if (!set) return 0;

  let sent = 0;
  for (const entry of set) {
    if (scopeFilterFn && !scopeFilterFn(entry.scope)) continue;
    try {
      sseSend(entry.res, payload, id);
      sent++;
    } catch (_) {
      try {
        entry.res.end();
      } catch (_) {}
      removeSseClient(uid, entry, "write_failed");
    }
  }

  return sent;
}

app.get("/ping", (req, res) => {
  res.json({ status: "ok", message: "pong", service: "realtime-service", server_time: new Date().toISOString() });
});

app.get("/events", authenticateToken, async (req, res) => {
  const scope = (req.query.scope || "global").toString();

  res.status(200);
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders?.();

  await touchPresence(req.user.uid);

  try {
    res.write("retry: 3000\n\n");
  } catch (_) {}

  const entry = addSseClient(req.user.uid, res, scope);

  sseSend(res, {
    type: "hello",
    ok: true,
    scope,
    server_time: new Date().toISOString(),
    instance: INSTANCE,
  });

  const ka = setInterval(async () => {
    try {
      sseComment(res, `keepalive ${Date.now()}`);
      await touchPresence(req.user.uid);
    } catch (_) {}
  }, 10000);

  req.on("close", () => {
    clearInterval(ka);
    removeSseClient(req.user.uid, entry, "client_close");
  });
});

app.post("/presence/offline", authenticateToken, async (req, res) => {
  try {
    const ok = await ensureRedisConnected();
    if (ok && redis.status === "ready") await redis.del(presenceKey(req.user.uid));
  } catch (_) {}
  res.json({ ok: true });
});

app.get("/presence/online/:userId", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ error: "bad userId" });

  const online = await isOnline(userId);
  res.json({ user_id: userId, online });
});

app.get("/presence/online-batch", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);
  const ids = (req.query.ids || "").toString().split(",").map((x) => Number(x.trim()));
  const online = await onlineBatch(ids);
  res.json({ online });
});

const subscriber = createSubscriber({ connectionString: DATABASE_URL });
let subscriberHandlersReady = false;

function setupSubscriberHandlersOnce() {
  if (subscriberHandlersReady) return;
  subscriberHandlersReady = true;

  subscriber.notifications.on("global_messages", (payload) => {
    const obj = safeJson(payload);
    const msgId = obj?.id;

    const out = {
      type: "global_message",
      id: obj.id,
      user_id: obj.user_id,
      nickname: obj.nickname,
      body: obj.body,
      created_at: obj.created_at,
      reply_to_id: obj.reply_to_id ?? null,
      mention_user_ids: obj.mention_user_ids || [],
      instance: INSTANCE,
      source: "notify",
    };

    let sent = 0;
    for (const [, set] of sseClients.entries()) {
      for (const entry of set) {
        if (entry.scope !== "global") continue;
        try {
          sseSend(entry.res, out, msgId || null);
          sent++;
        } catch (_) {}
      }
    }

    log("info", "global.event.sse.sent", { msgId, source: "notify", sent });
  });

  subscriber.notifications.on("dm_messages", (payload) => {
    (async () => {
      try {
        const obj = safeJson(payload);
        const msgId = obj?.id;
        const convId = obj?.conversation_id;
        if (!convId) return;

        const r = await pool.query(
          "SELECT user_low, user_high FROM dm_pairs WHERE conversation_id = $1 LIMIT 1",
          [convId]
        );
        if (!r.rows.length) return;

        const { user_low, user_high } = r.rows[0];

        const out = {
          type: "dm_message",
          id: obj.id,
          conversation_id: convId,
          sender_user_id: obj.sender_user_id,
          sender_nickname: obj.sender_nickname,
          body: obj.body,
          created_at: obj.created_at,
          reply_to_id: obj.reply_to_id ?? null,
          mention_user_ids: obj.mention_user_ids || [],
          instance: INSTANCE,
          source: "notify",
        };

        const toLow = broadcastToUser(user_low, out, (scope) => scope === "dm" || scope === `dm:${convId}`, msgId || null);
        const toHigh = broadcastToUser(user_high, out, (scope) => scope === "dm" || scope === `dm:${convId}`, msgId || null);

        log("info", "dm.event.sse.sent", { msgId, convId, source: "notify", toLow, toHigh });
      } catch (e) {
        log("error", "dm.notify.handler.failed", { err: e?.message || String(e) });
      }
    })();
  });

  subscriber.events.on("error", (err) => log("error", "pg-listen.error", { err: err?.message || String(err) }));
}

async function startSubscriberForever() {
  while (true) {
    try {
      setupSubscriberHandlersOnce();
      log("info", "subscriber.connecting", { instance: INSTANCE });
      await subscriber.connect();
      await subscriber.listenTo("global_messages");
      await subscriber.listenTo("dm_messages");
      log("info", "subscriber.ready");
      return;
    } catch (e) {
      log("error", "subscriber.failed", { err: e?.message || String(e) });
      await sleep(5000);
    }
  }
}

async function waitDbForever() {
  while (true) {
    try {
      await pool.query("SELECT 1");
      log("info", "db.ready");
      return;
    } catch (e) {
      log("error", "db.wait.failed", { err: e?.message || String(e) });
      await sleep(5000);
    }
  }
}

async function main() {
  log("info", "boot", { instance: INSTANCE, port: PORT, log_level: LOG_LEVEL });
  await waitDbForever();
  startSubscriberForever().catch((e) => log("error", "subscriber.fatal", { err: e?.message || String(e) }));
  app.listen(PORT, () => log("info", "http.listening", { port: PORT, instance: INSTANCE }));
}

main().catch((e) => {
  log("error", "fatal", { err: e?.message || String(e) });
  process.exit(1);
});

process.on("SIGINT", async () => {
  log("warn", "sigint");
  try {
    await subscriber.close();
  } catch (_) {}
  try {
    await redis.quit();
  } catch (_) {}
  process.exit(0);
});
