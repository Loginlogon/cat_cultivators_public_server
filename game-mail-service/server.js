'use strict';

const express = require("express");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const { v4: uuidv4 } = require("uuid");
const createSubscriber = require("pg-listen");
const Redis = require("ioredis");
const { readEnv } = require("./env");

// -------------------- CONFIG --------------------
const app = express();
app.use(express.json({ limit: "1mb" }));

let ACCESS_SECRET, ADMIN_SECRET_KEY, DATABASE_URL, REDIS_URL, PORT, LOG_LEVEL;
try {
  ACCESS_SECRET = readEnv("ACCESS_SECRET");
  ADMIN_SECRET_KEY = readEnv("ADMIN_SECRET_KEY");
  DATABASE_URL = readEnv("DATABASE_URL");
  REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";
  PORT = Number(process.env.PORT || 3001);
  LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
} catch (e) {
  console.error("❌", e.message);
  process.exit(1);
}

const INSTANCE = process.env.INSTANCE_ID || require("os").hostname();

// -------------------- LOGGER --------------------
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
function shouldLog(level) {
  const a = LEVELS[level] ?? 20;
  const b = LEVELS[LOG_LEVEL] ?? 20;
  return a >= b;
}
function log(level, msg, meta) {
  if (!shouldLog(level)) return;
  const out = {
    t: new Date().toISOString(),
    level,
    msg,
    meta: meta || undefined,
  };
  // eslint-disable-next-line no-console
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

// -------------------- DB --------------------
const pool = new Pool({ connectionString: DATABASE_URL });
pool.on("error", (err) => log("error", "pg.pool.error", { err: err?.message || String(err) }));

// -------------------- REDIS (presence) --------------------
const redis = new Redis(REDIS_URL, {
  enableAutoPipelining: false,
  lazyConnect: true,
});
redis.on("error", (e) => log("error", "redis.error", { err: e?.message || String(e) }));

let redisConnectPromise = null;
async function ensureRedisConnected() {
  try {
    if (redis.status === "ready") return true;
    if (redisConnectPromise) return redisConnectPromise;

    redisConnectPromise = redis
      .connect()
      .then(() => {
        log("info", "redis.connected", { url: "REDIS_URL", status: redis.status });
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

const PRESENCE_TTL_SEC = Number(process.env.PRESENCE_TTL_SEC || 15);
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

// -------------------- HELPERS --------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const clampInt = (v, def, min, max) => {
  const n = Number.isFinite(Number(v)) ? Number(v) : def;
  return Math.max(min, Math.min(max, Math.trunc(n)));
};

const requireAdmin = (req, res, next) => {
  const key = req.headers["x-admin-key"];
  if (!ADMIN_SECRET_KEY) return res.status(500).json({ error: "ADMIN_SECRET_KEY not configured" });
  if (!key || key !== ADMIN_SECRET_KEY) return res.status(403).json({ error: "Forbidden" });
  next();
};

const authenticateToken = (req, res, next) => {
  const authHeader = req.headers["authorization"];
  const token = authHeader && authHeader.split(" ")[1];
  if (!token) return res.status(401).json({ error: "Token missing" });

  jwt.verify(token, ACCESS_SECRET, (err, payload) => {
    if (err) return res.status(401).json({ error: "Access token expired or invalid" });
    req.user = payload; // { uid, login }
    next();
  });
};

async function getUserBasic(userId) {
  const r = await pool.query("SELECT id, login, nickname FROM users WHERE id = $1 LIMIT 1", [userId]);
  return r.rows[0] || null;
}

// -------------------- FIREBASE (FCM) --------------------
let firebaseReady = false;
let _admin = null;

function getFirebaseAdmin() {
  if (_admin) return _admin;
  try {
    _admin = require("firebase-admin");
    return _admin;
  } catch (e) {
    log("warn", "firebase.not_installed", { note: "firebase-admin not installed, push disabled" });
    return null;
  }
}

function initFirebaseOnce() {
  if (firebaseReady) return true;

  const admin = getFirebaseAdmin();
  if (!admin) return false;

  const json = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!json) {
    log("warn", "firebase.no_service_account_json", { note: "FIREBASE_SERVICE_ACCOUNT_JSON not set, push disabled" });
    return false;
  }

  try {
    const serviceAccount = JSON.parse(json);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
    firebaseReady = true;
    log("info", "firebase.ready");
    return true;
  } catch (e) {
    log("error", "firebase.init_failed", { err: e?.message || String(e) });
    return false;
  }
}

async function sendDmPush(toUserId, { title, body, data }) {
  if (!initFirebaseOnce()) return;

  const admin = getFirebaseAdmin();
  if (!admin) return;

  const t = await pool.query(`SELECT token FROM push_tokens WHERE user_id = $1`, [toUserId]);
  const tokens = t.rows.map((r) => r.token).filter(Boolean);
  if (!tokens.length) {
    log("debug", "push.skip.no_tokens", { toUserId });
    return;
  }

  const payloadData = {
    ...(data || {}),
    type: "dm",
    title: String(title || ""),
    body: String(body || ""),
  };

  log("debug", "push.send.start", { toUserId, tokens: tokens.length });

  const resp = await admin.messaging().sendEachForMulticast({
    tokens,
    data: Object.fromEntries(Object.entries(payloadData).map(([k, v]) => [k, String(v)])),
    android: { priority: "high" },
  });

  const dead = [];
  resp.responses.forEach((r, idx) => {
    if (r.success) return;
    const code = r.error?.code || "";
    if (
      code === "messaging/registration-token-not-registered" ||
      code === "messaging/invalid-registration-token"
    ) {
      dead.push(tokens[idx]);
    }
  });

  if (dead.length) {
    await pool.query(
      `DELETE FROM push_tokens WHERE user_id = $1 AND token = ANY($2::text[])`,
      [toUserId, dead]
    );
    log("warn", "push.tokens.cleaned", { toUserId, dead: dead.length });
  }

  log("info", "push.send.done", { toUserId, ok: resp.successCount, total: tokens.length });
}

// -------------------- SSE --------------------
// ВАЖНО: отправляем без `event:` чтобы клиент, который слушает только default "message", тоже работал.
// Тип события кладём в JSON: { type: "global_message" | "dm_message" | ... }

const sseClients = new Map(); // userId -> Set<entry>
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

// SSE frame (без event:)
function sseSend(res, obj, id = null) {
  const data = JSON.stringify(obj);
  if (id !== null && id !== undefined) res.write(`id: ${String(id)}\n`);
  res.write(`data: ${data}\n\n`);
}

function sseComment(res, text) {
  // Комментарии начинаются с ":" — удобно как keep-alive. :contentReference[oaicite:1]{index=1}
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
    } catch (e) {
      // если клиент отвалился — убираем
      try { entry.res.end(); } catch (_) {}
      removeSseClient(uid, entry, "write_failed");
    }
  }
  return sent;
}

// -------------------- DEDUPE (direct + NOTIFY) --------------------
const RECENT_TTL_MS = Number(process.env.RECENT_TTL_MS || 10000);
const recentGlobal = new Map(); // id -> ts
const recentDm = new Map();     // id -> ts

function markRecent(map, key) {
  map.set(String(key), Date.now());
}
function wasRecent(map, key) {
  const k = String(key);
  const ts = map.get(k);
  if (!ts) return false;
  if (Date.now() - ts < RECENT_TTL_MS) return true;
  map.delete(k);
  return false;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, ts] of recentGlobal.entries()) if (now - ts > RECENT_TTL_MS) recentGlobal.delete(k);
  for (const [k, ts] of recentDm.entries()) if (now - ts > RECENT_TTL_MS) recentDm.delete(k);
}, 5000).unref?.();

// -------------------- DB: triggers for NOTIFY --------------------
async function ensureNotifyTriggers(client) {
  // ✅ ДОБАВИЛИ body в payload (это критично для обновления чата)
  await client.query(`
    CREATE OR REPLACE FUNCTION notify_global_message() RETURNS trigger AS $$
    BEGIN
      PERFORM pg_notify(
        'global_messages',
        json_build_object(
          'id', NEW.id,
          'user_id', NEW.user_id,
          'nickname', NEW.nickname,
          'body', NEW.body,
          'created_at', NEW.created_at
        )::text
      );
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

  // ✅ ДОБАВИЛИ body в payload (это критично для DM)
  await client.query(`
    CREATE OR REPLACE FUNCTION notify_dm_message() RETURNS trigger AS $$
    BEGIN
      PERFORM pg_notify(
        'dm_messages',
        json_build_object(
          'id', NEW.id,
          'conversation_id', NEW.conversation_id,
          'sender_user_id', NEW.sender_user_id,
          'sender_nickname', NEW.sender_nickname,
          'body', NEW.body,
          'created_at', NEW.created_at
        )::text
      );
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

  await client.query(`DROP TRIGGER IF EXISTS trg_notify_global_message ON global_messages;`);
  await client.query(`
    CREATE TRIGGER trg_notify_global_message
    AFTER INSERT ON global_messages
    FOR EACH ROW EXECUTE FUNCTION notify_global_message();
  `);

  await client.query(`DROP TRIGGER IF EXISTS trg_notify_dm_message ON dm_messages;`);
  await client.query(`
    CREATE TRIGGER trg_notify_dm_message
    AFTER INSERT ON dm_messages
    FOR EACH ROW EXECUTE FUNCTION notify_dm_message();
  `);

  log("info", "db.triggers.ready");
}

// -------------------- DB INIT + "soft migrations" --------------------
let subscriberStarted = false;

async function initDbForever() {
  while (true) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      await client.query(`
        CREATE TABLE IF NOT EXISTS global_messages (
          id BIGSERIAL PRIMARY KEY,
          user_id INTEGER NOT NULL,
          nickname TEXT NOT NULL,
          body TEXT NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_global_messages_id_desc ON global_messages(id DESC);`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS conversations (
          id UUID PRIMARY KEY,
          type TEXT NOT NULL CHECK (type IN ('dm')),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dm_pairs (
          user_low INTEGER NOT NULL,
          user_high INTEGER NOT NULL,
          conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (user_low, user_high)
        );
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dm_messages (
          id BIGSERIAL PRIMARY KEY,
          conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
          sender_user_id INTEGER NOT NULL,
          sender_nickname TEXT NOT NULL,
          body TEXT NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_messages_conv_id_desc ON dm_messages(conversation_id, id DESC);`);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_dm_messages_conv_sender_id_desc
        ON dm_messages(conversation_id, sender_user_id, id DESC);
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dm_contacts (
          owner_user_id INTEGER NOT NULL,
          contact_user_id INTEGER NOT NULL,
          conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
          last_message_id BIGINT,
          last_at TIMESTAMP,
          PRIMARY KEY (owner_user_id, contact_user_id)
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_contacts_owner_last_at ON dm_contacts(owner_user_id, last_at DESC);`);

      await client.query(`ALTER TABLE dm_contacts ADD COLUMN IF NOT EXISTS last_read_message_id BIGINT;`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_contacts_owner_unread ON dm_contacts(owner_user_id, last_message_id, last_read_message_id);`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS mails (
          id UUID PRIMARY KEY,
          to_user_id INTEGER NOT NULL,
          from_type TEXT NOT NULL CHECK (from_type IN ('system','admin','player')),
          from_user_id INTEGER,
          subject TEXT NOT NULL,
          body TEXT NOT NULL,
          reward_json JSONB,
          status TEXT NOT NULL DEFAULT 'unread' CHECK (status IN ('unread','read','claimed')),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_mails_to_created_desc ON mails(to_user_id, created_at DESC);`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_mails_to_status_created_desc ON mails(to_user_id, status, created_at DESC);`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS global_reads (
          user_id INTEGER PRIMARY KEY,
          last_read_id BIGINT NOT NULL DEFAULT 0,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS push_tokens (
          user_id INTEGER NOT NULL,
          token TEXT NOT NULL,
          platform TEXT NOT NULL DEFAULT 'android' CHECK (platform IN ('android')),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (user_id, token)
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_push_tokens_user ON push_tokens(user_id);`);

      await ensureNotifyTriggers(client);

      await client.query("COMMIT");
      log("info", "db.ready");

      if (!subscriberStarted) {
        subscriberStarted = true;
        startSubscriber().catch((e) => log("error", "subscriber.start.failed", { err: e?.message || String(e) }));
      }

      return;
    } catch (err) {
      try { await client.query("ROLLBACK"); } catch (_) {}
      log("error", "db.init.failed", { err: err?.message || String(err) });
      await sleep(5000);
    } finally {
      client.release();
    }
  }
}

// -------------------- SSE endpoint --------------------
app.get("/events", authenticateToken, async (req, res) => {
  const scope = (req.query.scope || "global").toString();

  res.status(200);
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders?.();

  await touchPresence(req.user.uid);

  // retry — если клиент отвалился, пусть пытается переподключаться
  try { res.write(`retry: 3000\n\n`); } catch (_) {}

  const entry = addSseClient(req.user.uid, res, scope);

  // hello (без event:, type внутри JSON)
  sseSend(res, {
    type: "hello",
    ok: true,
    scope,
    server_time: new Date().toISOString(),
    instance: INSTANCE,
  });

  // keep-alive
  const ka = setInterval(async () => {
    try {
      sseComment(res, `keepalive ${Date.now()}`);
      await touchPresence(req.user.uid);
    } catch (_) {}
  }, 15000);

  req.on("close", () => {
    clearInterval(ka);
    removeSseClient(req.user.uid, entry, "client_close");
  });
});

// -------------------- BASIC ROUTES --------------------
app.get("/ping", (req, res) => {
  res.json({ status: "ok", message: "pong", service: "game-mail-service", server_time: new Date().toISOString() });
});

// -------------------- PUSH: register token --------------------
app.post("/push/register", authenticateToken, async (req, res) => {
  const token = (req.body?.token || "").toString().trim();
  if (!token) return res.status(400).json({ error: "token required" });
  if (token.length > 4096) return res.status(400).json({ error: "token too long" });

  try {
    await pool.query(
      `INSERT INTO push_tokens (user_id, token, platform, updated_at)
       VALUES ($1, $2, 'android', CURRENT_TIMESTAMP)
       ON CONFLICT (user_id, token)
       DO UPDATE SET updated_at = CURRENT_TIMESTAMP`,
      [req.user.uid, token]
    );
    log("info", "push.register.ok", { uid: req.user.uid, tokenLen: token.length });
    res.json({ ok: true });
  } catch (e) {
    log("error", "push.register.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- Presence --------------------
app.post("/presence/offline", authenticateToken, async (req, res) => {
  try {
    const ok = await ensureRedisConnected();
    if (ok && redis.status === "ready") {
      await redis.del(presenceKey(req.user.uid));
    }
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

// -------------------- USERS SEARCH --------------------
app.get("/users/search", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const nickname = (req.query.nickname || "").toString().trim();
  const limit = clampInt(req.query.limit, 20, 1, 50);
  if (!nickname) return res.status(400).json({ error: "nickname query required" });

  try {
    const r = await pool.query(
      `SELECT id, nickname
       FROM users
       WHERE nickname ILIKE $1
       ORDER BY id ASC
       LIMIT $2`,
      [nickname + "%", limit]
    );

    res.json({
      query: nickname,
      limit,
      users: r.rows.map((u) => ({ id: u.id, nickname: u.nickname, header: `${u.nickname}#${u.id}` })),
    });
  } catch (e) {
    log("error", "users.search.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- GLOBAL CHAT --------------------
app.get("/chat/global/history", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const limit = clampInt(req.query.limit, 100, 1, 100);
  const beforeId = req.query.before_id ? Number(req.query.before_id) : null;

  try {
    const q = beforeId
      ? `SELECT id, user_id, nickname, body, created_at
         FROM global_messages
         WHERE id < $1
         ORDER BY id DESC
         LIMIT $2`
      : `SELECT id, user_id, nickname, body, created_at
         FROM global_messages
         ORDER BY id DESC
         LIMIT $1`;

    const params = beforeId ? [beforeId, limit] : [limit];
    const r = await pool.query(q, params);

    const items = r.rows.reverse().map((m) => ({
      id: m.id,
      body: m.body,
      created_at: m.created_at,
      sender: { user_id: m.user_id, nickname: m.nickname, header: `${m.nickname}#${m.user_id}` },
    }));

    const next_before_id = items.length ? items[0].id : null;
    res.json({ limit, next_before_id, messages: items });
  } catch (e) {
    log("error", "global.history.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/chat/global/send", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const body = (req.body?.body || "").toString();
  if (!body.trim()) return res.status(400).json({ error: "Message body required" });
  if (body.length > 1024) return res.status(400).json({ error: "Message too long (max 1024)" });

  try {
    const me = await getUserBasic(req.user.uid);
    if (!me) return res.status(401).json({ error: "User not found" });

    const insert = await pool.query(
      `INSERT INTO global_messages (user_id, nickname, body)
       VALUES ($1, $2, $3)
       RETURNING id, created_at`,
      [me.id, me.nickname, body]
    );

    // чистка до 100
    await pool.query(`
      DELETE FROM global_messages
      WHERE id NOT IN (
        SELECT id FROM global_messages ORDER BY id DESC LIMIT 100
      )
    `);

    const msgId = insert.rows[0].id;
    const createdAt = insert.rows[0].created_at;

    // ✅ direct broadcast (и дедуп на случай, если прилетит NOTIFY)
    const payload = {
      type: "global_message",
      id: msgId,
      user_id: me.id,
      nickname: me.nickname,
      body,
      created_at: createdAt,
      instance: INSTANCE,
      source: "direct",
    };

    markRecent(recentGlobal, msgId);
    let sent = 0;
    for (const [, set] of sseClients.entries()) {
      for (const entry of set) {
        if (entry.scope === "global") {
          try {
            sseSend(entry.res, payload, msgId);
            sent++;
          } catch (_) {}
        }
      }
    }
    log("info", "global.event.sse.sent", { msgId, source: "direct", sent });

    res.status(201).json({
      message: "sent",
      data: {
        id: msgId,
        created_at: createdAt,
        sender: { user_id: me.id, nickname: me.nickname, header: `${me.nickname}#${me.id}` },
      },
    });
  } catch (e) {
    log("error", "global.send.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/chat/global/read", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const last_read_id = Number(req.body?.last_read_id);
  if (!Number.isInteger(last_read_id) || last_read_id < 0) {
    return res.status(400).json({ error: "last_read_id must be integer >= 0" });
  }

  try {
    await pool.query(
      `INSERT INTO global_reads (user_id, last_read_id, updated_at)
       VALUES ($1, $2, CURRENT_TIMESTAMP)
       ON CONFLICT (user_id)
       DO UPDATE SET last_read_id = GREATEST(global_reads.last_read_id, EXCLUDED.last_read_id),
                     updated_at = CURRENT_TIMESTAMP`,
      [req.user.uid, last_read_id]
    );

    res.json({ ok: true, last_read_id });
  } catch (e) {
    log("error", "global.read.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- DM CHAT --------------------
async function getOrCreateDmConversation(userA, userB) {
  const low = Math.min(userA, userB);
  const high = Math.max(userA, userB);

  const existing = await pool.query(
    "SELECT conversation_id FROM dm_pairs WHERE user_low = $1 AND user_high = $2 LIMIT 1",
    [low, high]
  );
  if (existing.rows.length) return existing.rows[0].conversation_id;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const again = await client.query(
      "SELECT conversation_id FROM dm_pairs WHERE user_low = $1 AND user_high = $2 LIMIT 1",
      [low, high]
    );
    if (again.rows.length) {
      await client.query("COMMIT");
      return again.rows[0].conversation_id;
    }

    const convId = uuidv4();
    await client.query("INSERT INTO conversations (id, type) VALUES ($1, 'dm')", [convId]);

    try {
      await client.query(
        "INSERT INTO dm_pairs (user_low, user_high, conversation_id) VALUES ($1, $2, $3)",
        [low, high, convId]
      );
    } catch (e) {
      const reread = await client.query(
        "SELECT conversation_id FROM dm_pairs WHERE user_low = $1 AND user_high = $2 LIMIT 1",
        [low, high]
      );
      if (reread.rows.length) {
        await client.query("DELETE FROM conversations WHERE id = $1", [convId]);
        await client.query("COMMIT");
        return reread.rows[0].conversation_id;
      }
      throw e;
    }

    await client.query("COMMIT");
    return convId;
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    throw e;
  } finally {
    client.release();
  }
}

app.post("/chat/dm/start", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const contactId = Number(req.body?.contact_user_id);
  if (!Number.isInteger(contactId) || contactId <= 0) return res.status(400).json({ error: "contact_user_id must be integer" });
  if (contactId === req.user.uid) return res.status(400).json({ error: "Cannot chat with yourself" });

  try {
    const other = await getUserBasic(contactId);
    if (!other) return res.status(404).json({ error: "Contact user not found" });

    const cid = await getOrCreateDmConversation(req.user.uid, contactId);
    res.json({ conversation_id: cid, contact: { user_id: other.id, nickname: other.nickname, header: `${other.nickname}#${other.id}` } });
  } catch (e) {
    log("error", "dm.start.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.get("/chat/dm/:conversation_id/history", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const limit = clampInt(req.query.limit, 50, 1, 50);
  const beforeId = req.query.before_id ? Number(req.query.before_id) : null;
  const conversationId = req.params.conversation_id;

  try {
    const member = await pool.query(
      `SELECT 1
       FROM dm_pairs p
       WHERE p.conversation_id = $1
         AND ($2 IN (p.user_low, p.user_high))
       LIMIT 1`,
      [conversationId, req.user.uid]
    );
    if (member.rows.length === 0) return res.status(403).json({ error: "Not a member of this conversation" });

    const q = beforeId
      ? `SELECT id, sender_user_id, sender_nickname, body, created_at
         FROM dm_messages
         WHERE conversation_id = $1 AND id < $2
         ORDER BY id DESC
         LIMIT $3`
      : `SELECT id, sender_user_id, sender_nickname, body, created_at
         FROM dm_messages
         WHERE conversation_id = $1
         ORDER BY id DESC
         LIMIT $2`;

    const params = beforeId ? [conversationId, beforeId, limit] : [conversationId, limit];
    const r = await pool.query(q, params);

    const items = r.rows.reverse().map((m) => ({
      id: m.id,
      body: m.body,
      created_at: m.created_at,
      sender: { user_id: m.sender_user_id, nickname: m.sender_nickname, header: `${m.sender_nickname}#${m.sender_user_id}` },
    }));

    const next_before_id = items.length ? items[0].id : null;
    res.json({ conversation_id: conversationId, limit, next_before_id, messages: items });
  } catch (e) {
    log("error", "dm.history.failed", { err: e?.message || String(e), conversationId });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/chat/dm/:conversation_id/send", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const conversationId = req.params.conversation_id;
  const body = (req.body?.body || "").toString();

  if (!body.trim()) return res.status(400).json({ error: "Message body required" });
  if (body.length > 1024) return res.status(400).json({ error: "Message too long (max 1024)" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const member = await client.query(
      `SELECT user_low, user_high
       FROM dm_pairs
       WHERE conversation_id = $1
       LIMIT 1`,
      [conversationId]
    );
    if (member.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Conversation not found" });
    }

    const { user_low, user_high } = member.rows[0];
    if (req.user.uid !== user_low && req.user.uid !== user_high) {
      await client.query("ROLLBACK");
      return res.status(403).json({ error: "Not a member of this conversation" });
    }

    const me = await client.query("SELECT id, nickname FROM users WHERE id = $1 LIMIT 1", [req.user.uid]);
    if (me.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(401).json({ error: "User not found" });
    }

    const myNick = me.rows[0].nickname;

    const ins = await client.query(
      `INSERT INTO dm_messages (conversation_id, sender_user_id, sender_nickname, body)
       VALUES ($1, $2, $3, $4)
       RETURNING id, created_at`,
      [conversationId, req.user.uid, myNick, body]
    );

    const msgId = ins.rows[0].id;
    const createdAt = ins.rows[0].created_at;

    const otherId = req.user.uid === user_low ? user_high : user_low;

    // dm_contacts upsert for both sides
    await client.query(
      `INSERT INTO dm_contacts (owner_user_id, contact_user_id, conversation_id, last_message_id, last_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (owner_user_id, contact_user_id)
       DO UPDATE SET last_message_id = EXCLUDED.last_message_id,
                     last_at = EXCLUDED.last_at,
                     conversation_id = EXCLUDED.conversation_id`,
      [req.user.uid, otherId, conversationId, msgId, createdAt]
    );

    await client.query(
      `INSERT INTO dm_contacts (owner_user_id, contact_user_id, conversation_id, last_message_id, last_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (owner_user_id, contact_user_id)
       DO UPDATE SET last_message_id = EXCLUDED.last_message_id,
                     last_at = EXCLUDED.last_at,
                     conversation_id = EXCLUDED.conversation_id`,
      [otherId, req.user.uid, conversationId, msgId, createdAt]
    );

    await client.query("COMMIT");

    // ✅ direct SSE (и дедуп на NOTIFY)
    const ssePayload = {
      type: "dm_message",
      id: msgId,
      conversation_id: conversationId,
      sender_user_id: req.user.uid,
      sender_nickname: myNick,
      body,                // ✅ ВАЖНО
      created_at: createdAt,
      instance: INSTANCE,
      source: "direct",
    };

    markRecent(recentDm, msgId);

    const toLow = broadcastToUser(
      user_low,
      ssePayload,
      (scope) => scope === "dm" || scope === `dm:${conversationId}`,
      msgId
    );
    const toHigh = broadcastToUser(
      user_high,
      ssePayload,
      (scope) => scope === "dm" || scope === `dm:${conversationId}`,
      msgId
    );

    log("info", "dm.event.sse.sent", { msgId, convId: conversationId, source: "direct", toLow, toHigh });

    // ✅ PUSH always to receiver
    try {
      await sendDmPush(otherId, {
        title: `Сообщение от ${myNick}`,
        body: body.length > 120 ? body.slice(0, 120) + "…" : body,
        data: {
          type: "dm",
          conversation_id: conversationId,
          sender_user_id: req.user.uid,
          sender_nickname: myNick,
        },
      });
    } catch (e) {
      log("warn", "push.send.failed", { err: e?.message || String(e) });
    }

    res.status(201).json({
      message: "sent",
      data: {
        id: msgId,
        created_at: createdAt,
        sender: {
          user_id: req.user.uid,
          nickname: myNick,
          header: `${myNick}#${req.user.uid}`,
        },
      },
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    log("error", "dm.send.failed", { err: e?.message || String(e), conversationId });
    res.status(500).json({ error: "Database error" });
  } finally {
    client.release();
  }
});

// DM contacts list (unread_count)
app.get("/chat/contacts", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  try {
    const r = await pool.query(
      `SELECT
         c.contact_user_id,
         c.conversation_id,
         c.last_message_id,
         c.last_read_message_id,
         c.last_at,
         u.nickname,
         (
           SELECT COUNT(*)::int
           FROM dm_messages m
           WHERE m.conversation_id = c.conversation_id
             AND m.sender_user_id = c.contact_user_id
             AND m.id > COALESCE(c.last_read_message_id, 0)
         ) AS unread_count
       FROM dm_contacts c
       JOIN users u ON u.id = c.contact_user_id
       WHERE c.owner_user_id = $1
       ORDER BY c.last_at DESC NULLS LAST
       LIMIT 200`,
      [req.user.uid]
    );

    res.json({
      contacts: r.rows.map((x) => ({
        contact_user_id: x.contact_user_id,
        nickname: x.nickname,
        header: `${x.nickname}#${x.contact_user_id}`,
        conversation_id: x.conversation_id,
        last_message_id: x.last_message_id,
        last_read_message_id: x.last_read_message_id,
        last_at: x.last_at,
        unread_count: x.unread_count || 0,
      })),
    });
  } catch (e) {
    log("error", "contacts.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// отметить DM как прочитанный (до message_id)
app.post("/chat/dm/:conversation_id/read", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const conversationId = req.params.conversation_id;
  const last_read_message_id = Number(req.body?.last_read_message_id);

  if (!Number.isInteger(last_read_message_id) || last_read_message_id < 0) {
    return res.status(400).json({ error: "last_read_message_id must be integer >= 0" });
  }

  try {
    const pair = await pool.query(
      `SELECT user_low, user_high
       FROM dm_pairs
       WHERE conversation_id = $1
       LIMIT 1`,
      [conversationId]
    );
    if (!pair.rows.length) return res.status(404).json({ error: "Conversation not found" });

    const { user_low, user_high } = pair.rows[0];
    if (req.user.uid !== user_low && req.user.uid !== user_high) return res.status(403).json({ error: "Not a member of this conversation" });

    const otherId = req.user.uid === user_low ? user_high : user_low;

    await pool.query(
      `UPDATE dm_contacts
       SET last_read_message_id = GREATEST(COALESCE(last_read_message_id, 0), $1)
       WHERE owner_user_id = $2 AND contact_user_id = $3`,
      [last_read_message_id, req.user.uid, otherId]
    );

    res.json({ ok: true, conversation_id: conversationId, last_read_message_id });
  } catch (e) {
    log("error", "dm.read.failed", { err: e?.message || String(e), conversationId });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- NOTIFICATIONS SUMMARY --------------------
app.get("/notifications/summary", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  try {
    const dm = await pool.query(
      `SELECT
         COUNT(*) FILTER (WHERE u.unread_count > 0)::int AS dm_unread_threads,
         COALESCE(SUM(u.unread_count), 0)::int AS dm_unread_messages
       FROM dm_contacts c
       LEFT JOIN LATERAL (
         SELECT COUNT(*)::int AS unread_count
         FROM dm_messages m
         WHERE m.conversation_id = c.conversation_id
           AND m.sender_user_id = c.contact_user_id
           AND m.id > COALESCE(c.last_read_message_id, 0)
       ) u ON true
       WHERE c.owner_user_id = $1`,
      [req.user.uid]
    );

    const gr = await pool.query(`SELECT last_read_id FROM global_reads WHERE user_id = $1 LIMIT 1`, [req.user.uid]);
    const lastRead = gr.rows.length ? Number(gr.rows[0].last_read_id) : 0;

    const g = await pool.query(`SELECT COUNT(*)::int AS cnt FROM global_messages WHERE id > $1`, [lastRead]);
    const m = await pool.query(`SELECT COUNT(*)::int AS cnt FROM mails WHERE to_user_id = $1 AND status = 'unread'`, [req.user.uid]);

    res.json({
      dm_unread_threads: dm.rows[0]?.dm_unread_threads ?? 0,
      dm_unread_messages: dm.rows[0]?.dm_unread_messages ?? 0,
      global_unread: g.rows[0].cnt,
      mail_unread: m.rows[0].cnt,
    });
  } catch (e) {
    log("error", "notifications.summary.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- MAIL --------------------
app.get("/mail/inbox", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const limit = clampInt(req.query.limit, 50, 1, 50);
  const before = (req.query.before_at || "").toString().trim();

  try {
    const q = before
      ? `SELECT id, from_type, from_user_id, subject, status, created_at
         FROM mails
         WHERE to_user_id = $1 AND created_at < $2
         ORDER BY created_at DESC
         LIMIT $3`
      : `SELECT id, from_type, from_user_id, subject, status, created_at
         FROM mails
         WHERE to_user_id = $1
         ORDER BY created_at DESC
         LIMIT $2`;

    const params = before ? [req.user.uid, before, limit] : [req.user.uid, limit];
    const r = await pool.query(q, params);

    const next_before_at = r.rows.length ? r.rows[r.rows.length - 1].created_at : null;
    res.json({ limit, next_before_at, mails: r.rows });
  } catch (e) {
    log("error", "mail.inbox.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.get("/mail/:id", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const id = req.params.id;
  try {
    const r = await pool.query(
      `SELECT id, to_user_id, from_type, from_user_id, subject, body, reward_json, status, created_at
       FROM mails
       WHERE id = $1 AND to_user_id = $2
       LIMIT 1`,
      [id, req.user.uid]
    );
    if (r.rows.length === 0) return res.status(404).json({ error: "Mail not found" });

    if (r.rows[0].status === "unread") {
      await pool.query(
        `UPDATE mails SET status = 'read' WHERE id = $1 AND to_user_id = $2 AND status = 'unread'`,
        [id, req.user.uid]
      );
      r.rows[0].status = "read";
    }

    res.json({ mail: r.rows[0] });
  } catch (e) {
    log("error", "mail.get.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/mail/:id/read", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const id = req.params.id;

  try {
    const upd = await pool.query(
      `UPDATE mails
       SET status = 'read'
       WHERE id = $1 AND to_user_id = $2 AND status = 'unread'
       RETURNING id, status`,
      [id, req.user.uid]
    );

    if (upd.rows.length === 0) {
      const chk = await pool.query(`SELECT id, status FROM mails WHERE id = $1 AND to_user_id = $2 LIMIT 1`, [id, req.user.uid]);
      if (chk.rows.length === 0) return res.status(404).json({ error: "Mail not found" });
      return res.json({ mail: chk.rows[0] });
    }

    res.json({ mail: upd.rows[0] });
  } catch (e) {
    log("error", "mail.read.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/mail/:id/claim", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const id = req.params.id;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const mailR = await client.query(
      `SELECT id, reward_json, status
       FROM mails
       WHERE id = $1 AND to_user_id = $2
       FOR UPDATE`,
      [id, req.user.uid]
    );

    if (mailR.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "Mail not found" });
    }

    const mail = mailR.rows[0];
    if (mail.status === "claimed") {
      await client.query("COMMIT");
      return res.json({ status: "already_claimed" });
    }

    const reward = mail.reward_json || {};
    const addMortals = Number.isFinite(Number(reward.money_mortals)) ? Number(reward.money_mortals) : 0;
    const addCult = Number.isFinite(Number(reward.money_cultivators)) ? Number(reward.money_cultivators) : 0;

    if (addMortals !== 0 || addCult !== 0) {
      await client.query(
        `UPDATE profiles
         SET money_mortals = money_mortals + $1,
             money_cultivators = money_cultivators + $2
         WHERE user_id = $3`,
        [addMortals, addCult, req.user.uid]
      );
    }

    await client.query(`UPDATE mails SET status = 'claimed' WHERE id = $1 AND to_user_id = $2`, [id, req.user.uid]);

    await client.query("COMMIT");
    res.json({ status: "claimed", applied: { money_mortals: addMortals, money_cultivators: addCult } });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    log("error", "mail.claim.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  } finally {
    client.release();
  }
});

app.post("/admin/mail/send", requireAdmin, async (req, res) => {
  const to_user_id = Number(req.body?.to_user_id);
  const subject = (req.body?.subject || "").toString();
  const body = (req.body?.body || "").toString();
  const reward_json = req.body?.reward_json ?? null;

  if (!Number.isInteger(to_user_id) || to_user_id <= 0) return res.status(400).json({ error: "to_user_id must be integer" });
  if (!subject.trim()) return res.status(400).json({ error: "subject required" });
  if (!body.trim()) return res.status(400).json({ error: "body required" });

  try {
    const u = await getUserBasic(to_user_id);
    if (!u) return res.status(404).json({ error: "User not found" });

    const id = uuidv4();
    await pool.query(
      `INSERT INTO mails (id, to_user_id, from_type, from_user_id, subject, body, reward_json, status)
       VALUES ($1, $2, 'admin', NULL, $3, $4, $5, 'unread')`,
      [id, to_user_id, subject, body, reward_json]
    );

    res.status(201).json({ message: "sent", mail_id: id });
  } catch (e) {
    log("error", "admin.mail.send.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- SSE + Postgres LISTEN/NOTIFY --------------------
const subscriber = createSubscriber({ connectionString: DATABASE_URL });

async function startSubscriber() {
  try {
    log("info", "subscriber.connecting", { url: "DATABASE_URL", instance: INSTANCE });

    subscriber.notifications.on("global_messages", (payload) => {
      log("debug", "notify.global_messages");
      const obj = safeJson(payload);
      const msgId = obj?.id;

      // дедуп: если это уже ушло direct — не повторяем
      if (msgId && wasRecent(recentGlobal, msgId)) {
        log("debug", "notify.global_messages.skipped_recent", { msgId });
        return;
      }

      const out = {
        type: "global_message",
        id: obj.id,
        user_id: obj.user_id,
        nickname: obj.nickname,
        body: obj.body, // ✅ ВАЖНО
        created_at: obj.created_at,
        instance: INSTANCE,
        source: "notify",
      };

      let sent = 0;
      for (const [, set] of sseClients.entries()) {
        for (const entry of set) {
          if (entry.scope === "global") {
            try { sseSend(entry.res, out, msgId || null); sent++; } catch (_) {}
          }
        }
      }
      log("info", "global.event.sse.sent", { msgId, source: "notify", sent });
    });

    subscriber.notifications.on("dm_messages", (payload) => {
      log("debug", "notify.dm_messages");
      (async () => {
        try {
          const obj = safeJson(payload);
          const msgId = obj?.id;

          // дедуп: если уже ушло direct — не повторяем
          if (msgId && wasRecent(recentDm, msgId)) {
            log("debug", "notify.dm_messages.skipped_recent", { msgId });
            return;
          }

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
            body: obj.body, // ✅ ВАЖНО
            created_at: obj.created_at,
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

    await subscriber.connect();
    await subscriber.listenTo("global_messages");
    await subscriber.listenTo("dm_messages");

    log("info", "subscriber.ready");
  } catch (e) {
    log("error", "subscriber.failed", { err: e?.message || String(e) });
    await sleep(5000);
    return startSubscriber();
  }
}

// -------------------- START --------------------
async function main() {
  log("info", "boot", { instance: INSTANCE, port: PORT, log_level: LOG_LEVEL, has_firebase_json: !!process.env.FIREBASE_SERVICE_ACCOUNT_JSON });
  await initDbForever();

  app.listen(PORT, () => log("info", "http.listening", { port: PORT, instance: INSTANCE }));
}

main().catch((e) => {
  log("error", "fatal", { err: e?.message || String(e) });
  process.exit(1);
});

process.on("SIGINT", async () => {
  log("warn", "sigint");
  try { await subscriber.close(); } catch (_) {}
  try { await redis.quit(); } catch (_) {}
  process.exit(0);
});
