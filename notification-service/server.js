'use strict';

const express = require("express");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const { readEnv } = require("./env");

// -------------------- CONFIG --------------------
const app = express();
app.use(express.json({ limit: "1mb" }));

let ACCESS_SECRET, ADMIN_SECRET_KEY, DATABASE_URL, PORT, LOG_LEVEL;
try {
  ACCESS_SECRET = readEnv("ACCESS_SECRET");
  ADMIN_SECRET_KEY = readEnv("ADMIN_SECRET_KEY", { required: false, allowEmpty: true }) || "";
  DATABASE_URL = readEnv("DATABASE_URL");
  PORT = Number(process.env.PORT || 3002);
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

// -------------------- DB --------------------
const pool = new Pool({ connectionString: DATABASE_URL });
pool.on("error", (err) => log("error", "pg.pool.error", { err: err?.message || String(err) }));

// -------------------- HELPERS --------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const parseBool = (v, def) => {
  if (v === undefined || v === null) return def;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "no", "n", "off"].includes(s)) return false;
  return def;
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

// -------------------- NOTIFICATION SETTINGS + DM MUTES --------------------
async function ensureNotifSettingsRow(userId) {
  await pool.query(
    `INSERT INTO notification_settings (user_id)
     VALUES ($1)
     ON CONFLICT (user_id) DO NOTHING`,
    [userId]
  );
}

async function getNotifSettings(userId) {
  await ensureNotifSettingsRow(userId);
  const r = await pool.query(
    `SELECT allow_dm, allow_mentions, allow_reactions
     FROM notification_settings
     WHERE user_id = $1
     LIMIT 1`,
    [userId]
  );
  const row = r.rows[0] || {};
  return {
    allow_dm: row.allow_dm !== false,
    allow_mentions: row.allow_mentions !== false,
    allow_reactions: row.allow_reactions !== false,
  };
}

async function isDmMutedForRecipient(recipientId, senderId) {
  const r = await pool.query(
    `SELECT muted
     FROM dm_mutes
     WHERE owner_user_id = $1 AND muted_user_id = $2
     LIMIT 1`,
    [recipientId, senderId]
  );
  return r.rows[0]?.muted === true;
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
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    firebaseReady = true;
    log("info", "firebase.ready");
    return true;
  } catch (e) {
    log("error", "firebase.init_failed", { err: e?.message || String(e) });
    return false;
  }
}

async function sendPush(toUserId, { type, title, body, data }) {
  if (!initFirebaseOnce()) return;
  const admin = getFirebaseAdmin();
  if (!admin) return;

  const t = await pool.query(`SELECT token FROM push_tokens WHERE user_id = $1`, [toUserId]);
  const tokens = t.rows.map((r) => r.token).filter(Boolean);

  if (!tokens.length) {
    log("debug", "push.skip.no_tokens", { toUserId, type });
    return;
  }

  const payloadData = {
    ...(data || {}),
    type: String(type || ""),
    title: String(title || ""),
    body: String(body || ""),
  };

  log("debug", "push.send.start", { toUserId, type, tokens: tokens.length });

  const resp = await admin.messaging().sendEachForMulticast({
    tokens,
    data: Object.fromEntries(Object.entries(payloadData).map(([k, v]) => [k, String(v)])),
    android: { priority: "high" },
  });

  const dead = [];
  resp.responses.forEach((r, idx) => {
    if (r.success) return;
    const code = r.error?.code || "";
    if (code === "messaging/registration-token-not-registered" || code === "messaging/invalid-registration-token") {
      dead.push(tokens[idx]);
    }
  });

  if (dead.length) {
    await pool.query(`DELETE FROM push_tokens WHERE user_id = $1 AND token = ANY($2::text[])`, [toUserId, dead]);
    log("warn", "push.tokens.cleaned", { toUserId, dead: dead.length });
  }

  log("info", "push.send.done", { toUserId, type, ok: resp.successCount, total: tokens.length });
}

async function maybeSendDmPush(toUserId, fromUserId, payload) {
  const s = await getNotifSettings(toUserId);
  if (!s.allow_dm) {
    log("debug", "push.dm.skip.settings", { toUserId });
    return;
  }
  if (await isDmMutedForRecipient(toUserId, fromUserId)) {
    log("debug", "push.dm.skip.muted", { toUserId, fromUserId });
    return;
  }
  await sendPush(toUserId, payload);
}

async function maybeSendMentionPush(toUserId, payload) {
  const s = await getNotifSettings(toUserId);
  if (!s.allow_mentions) {
    log("debug", "push.mention.skip.settings", { toUserId });
    return;
  }
  await sendPush(toUserId, payload);
}

async function maybeSendReactionPush(toUserId, payload) {
  const s = await getNotifSettings(toUserId);
  if (!s.allow_reactions) {
    log("debug", "push.reaction.skip.settings", { toUserId });
    return;
  }
  await sendPush(toUserId, payload);
}

// -------------------- DB INIT + "soft migrations" --------------------
async function initDbForever() {
  while (true) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

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

      await client.query(`
        CREATE TABLE IF NOT EXISTS notification_settings (
          user_id INTEGER PRIMARY KEY,
          allow_dm BOOLEAN NOT NULL DEFAULT TRUE,
          allow_mentions BOOLEAN NOT NULL DEFAULT TRUE,
          allow_reactions BOOLEAN NOT NULL DEFAULT TRUE,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dm_mutes (
          owner_user_id INTEGER NOT NULL,
          muted_user_id INTEGER NOT NULL,
          muted BOOLEAN NOT NULL DEFAULT TRUE,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (owner_user_id, muted_user_id)
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_mutes_owner ON dm_mutes(owner_user_id);`);

      await client.query("COMMIT");
      log("info", "db.ready");
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

// -------------------- ROUTES --------------------
app.get("/ping", (req, res) => {
  res.json({ status: "ok", message: "pong", service: "notification-service", server_time: new Date().toISOString() });
});

// -------------------- SETTINGS: notifications --------------------
app.get("/settings/notifications", authenticateToken, async (req, res) => {
  try {
    const s = await getNotifSettings(req.user.uid);
    res.json({ user_id: req.user.uid, ...s });
  } catch (e) {
    log("error", "settings.notifications.get.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/settings/notifications", authenticateToken, async (req, res) => {
  try {
    await ensureNotifSettingsRow(req.user.uid);

    const cur = await getNotifSettings(req.user.uid);
    const allow_dm = parseBool((req.body?.allow_dm ?? req.body?.dm_enabled), cur.allow_dm);
    const allow_mentions = parseBool((req.body?.allow_mentions ?? req.body?.mentions_enabled), cur.allow_mentions);
    const allow_reactions = parseBool((req.body?.allow_reactions ?? req.body?.global_reactions_enabled ?? req.body?.reactions_enabled), cur.allow_reactions);

    await pool.query(
      `UPDATE notification_settings
       SET allow_dm = $2,
           allow_mentions = $3,
           allow_reactions = $4,
           updated_at = CURRENT_TIMESTAMP
       WHERE user_id = $1`,
      [req.user.uid, allow_dm, allow_mentions, allow_reactions]
    );

    res.json({ ok: true, user_id: req.user.uid, allow_dm, allow_mentions, allow_reactions });
  } catch (e) {
    log("error", "settings.notifications.set.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.get("/prefs/notifications", authenticateToken, async (req, res) => {
  try {
    await ensureNotifSettingsRow(req.user.uid);

    const r = await pool.query(
      `SELECT allow_dm, allow_mentions, allow_reactions, updated_at
       FROM notification_settings
       WHERE user_id = $1
       LIMIT 1`,
      [req.user.uid]
    );

    const row = r.rows[0] || {};
    res.json({
      dm_enabled: row.allow_dm !== false,
      mentions_enabled: row.allow_mentions !== false,
      global_reactions_enabled: row.allow_reactions !== false,
      updated_at: row.updated_at || new Date().toISOString(),
    });
  } catch (e) {
    log("error", "prefs.notifications.get.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/prefs/notifications", authenticateToken, async (req, res) => {
  try {
    await ensureNotifSettingsRow(req.user.uid);

    const cur = await getNotifSettings(req.user.uid);

    const allow_dm = parseBool((req.body?.dm_enabled ?? req.body?.allow_dm), cur.allow_dm);
    const allow_mentions = parseBool((req.body?.mentions_enabled ?? req.body?.allow_mentions), cur.allow_mentions);
    const allow_reactions = parseBool((req.body?.global_reactions_enabled ?? req.body?.allow_reactions ?? req.body?.reactions_enabled), cur.allow_reactions);

    const upd = await pool.query(
      `UPDATE notification_settings
       SET allow_dm = $2,
           allow_mentions = $3,
           allow_reactions = $4,
           updated_at = CURRENT_TIMESTAMP
       WHERE user_id = $1
       RETURNING updated_at`,
      [req.user.uid, allow_dm, allow_mentions, allow_reactions]
    );

    res.json({
      dm_enabled: allow_dm,
      mentions_enabled: allow_mentions,
      global_reactions_enabled: allow_reactions,
      updated_at: upd.rows[0]?.updated_at || new Date().toISOString(),
    });
  } catch (e) {
    log("error", "prefs.notifications.set.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
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

// -------------------- DM MUTE (toggle push from this contact) --------------------
async function getOtherIdFromConversation(conversationId, myId) {
  const pair = await pool.query(
    `SELECT user_low, user_high FROM dm_pairs WHERE conversation_id = $1 LIMIT 1`,
    [conversationId]
  );
  if (!pair.rows.length) return { err: "Conversation not found", code: 404 };
  const { user_low, user_high } = pair.rows[0];
  if (myId !== user_low && myId !== user_high) return { err: "Not a member of this conversation", code: 403 };
  const otherId = myId === user_low ? user_high : user_low;
  return { otherId, user_low, user_high };
}

app.get("/chat/dm/:conversation_id/mute", authenticateToken, async (req, res) => {
  const conversationId = req.params.conversation_id;
  try {
    const r0 = await getOtherIdFromConversation(conversationId, req.user.uid);
    if (r0.err) return res.status(r0.code).json({ error: r0.err });

    const otherId = r0.otherId;

    const r = await pool.query(
      `SELECT muted
       FROM dm_mutes
       WHERE owner_user_id = $1 AND muted_user_id = $2
       LIMIT 1`,
      [req.user.uid, otherId]
    );

    const muted = r.rows[0]?.muted === true;

    res.json({
      ok: true,
      conversation_id: conversationId,
      contact_user_id: otherId,
      muted,
      muted_user_id: otherId,
      mute: muted,
    });
  } catch (e) {
    // если dm_pairs ещё не создан (например сервис чата не поднялся) — аккуратно
    const msg = String(e?.message || "");
    if (msg.includes("dm_pairs") && msg.includes("does not exist")) {
      return res.status(503).json({ error: "chat tables not ready yet" });
    }
    log("error", "dm.mute.get.failed", { err: e?.message || String(e), conversationId });
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/chat/dm/:conversation_id/mute", authenticateToken, async (req, res) => {
  const conversationId = req.params.conversation_id;
  const muted = parseBool((req.body?.muted ?? req.body?.mute), true);

  try {
    const r0 = await getOtherIdFromConversation(conversationId, req.user.uid);
    if (r0.err) return res.status(r0.code).json({ error: r0.err });

    const otherId = r0.otherId;

    await pool.query(
      `INSERT INTO dm_mutes (owner_user_id, muted_user_id, muted, updated_at)
       VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
       ON CONFLICT (owner_user_id, muted_user_id)
       DO UPDATE SET muted = EXCLUDED.muted, updated_at = CURRENT_TIMESTAMP`,
      [req.user.uid, otherId, muted]
    );

    res.json({
      ok: true,
      conversation_id: conversationId,
      contact_user_id: otherId,
      muted,
      muted_user_id: otherId,
      mute: muted,
    });
  } catch (e) {
    const msg = String(e?.message || "");
    if (msg.includes("dm_pairs") && msg.includes("does not exist")) {
      return res.status(503).json({ error: "chat tables not ready yet" });
    }
    log("error", "dm.mute.set.failed", { err: e?.message || String(e), conversationId });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- NOTIFICATIONS SUMMARY --------------------
app.get("/notifications/summary", authenticateToken, async (req, res) => {
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
      global_unread: g.rows[0]?.cnt ?? 0,
      mail_unread: m.rows[0]?.cnt ?? 0,
    });
  } catch (e) {
    const msg = String(e?.message || "");
    // если таблицы чата/почты ещё не созданы — просто отдаём нули
    if (msg.includes("does not exist")) {
      return res.json({
        dm_unread_threads: 0,
        dm_unread_messages: 0,
        global_unread: 0,
        mail_unread: 0,
      });
    }
    log("error", "notifications.summary.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- INTERNAL: push dispatcher (called by chat-service) --------------------
app.post("/internal/push/send", requireAdmin, async (req, res) => {
  const kind = (req.body?.kind || "").toString(); // dm | mention | reaction (или любое, но фильтруем)
  const to_user_id = Number(req.body?.to_user_id);
  const from_user_id = req.body?.from_user_id === null || req.body?.from_user_id === undefined ? null : Number(req.body?.from_user_id);

  const type = (req.body?.type || "").toString();
  const title = (req.body?.title || "").toString();
  const body = (req.body?.body || "").toString();
  const data = req.body?.data && typeof req.body.data === "object" ? req.body.data : {};

  if (!Number.isInteger(to_user_id) || to_user_id <= 0) return res.status(400).json({ error: "to_user_id must be int > 0" });
  if (!type) return res.status(400).json({ error: "type required" });

  try {
    if (kind === "dm") {
      if (!Number.isInteger(from_user_id) || from_user_id <= 0) return res.status(400).json({ error: "from_user_id required for dm" });
      await maybeSendDmPush(to_user_id, from_user_id, { type, title, body, data });
      return res.json({ ok: true, dispatched: true });
    }
    if (kind === "mention") {
      await maybeSendMentionPush(to_user_id, { type, title, body, data });
      return res.json({ ok: true, dispatched: true });
    }
    if (kind === "reaction") {
      await maybeSendReactionPush(to_user_id, { type, title, body, data });
      return res.json({ ok: true, dispatched: true });
    }

    // если kind неизвестен — шлём как есть (без фильтров)
    await sendPush(to_user_id, { type, title, body, data });
    return res.json({ ok: true, dispatched: true, note: "unknown kind, sent raw" });
  } catch (e) {
    log("error", "internal.push.send.failed", { err: e?.message || String(e), kind, to_user_id, from_user_id });
    res.status(500).json({ error: "push failed" });
  }
});

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
