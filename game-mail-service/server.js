'use strict';

const express = require("express");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const { v4: uuidv4 } = require("uuid");
const { readEnv } = require("./env");

// -------------------- CONFIG --------------------
const app = express();
app.use(express.json({ limit: "1mb" }));

let ACCESS_SECRET, ADMIN_SECRET_KEY, DATABASE_URL, PORT, LOG_LEVEL;
try {
  ACCESS_SECRET = readEnv("ACCESS_SECRET");
  ADMIN_SECRET_KEY = readEnv("ADMIN_SECRET_KEY", { required: false, allowEmpty: true }) || "";
  DATABASE_URL = readEnv("DATABASE_URL");
  PORT = Number(process.env.PORT || 3001);
  LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
} catch (e) {
  console.error("❌", e.message);
  process.exit(1);
}

const INSTANCE = process.env.INSTANCE_ID || require("os").hostname();

// куда проксировать уведомления
const NOTIFICATION_URL = (process.env.NOTIFICATION_URL || "http://notification-service:3002").replace(/\/+$/, "");
const REALTIME_URL = (process.env.REALTIME_URL || "http://realtime-service:3003").replace(/\/+$/, "");

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

async function touchPresence(uid) {
  void uid;
}

// -------------------- HELPERS --------------------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clampInt = (v, def, min, max) => {
  const n = Number.isFinite(Number(v)) ? Number(v) : def;
  return Math.max(min, Math.min(max, Math.trunc(n)));
};
const parseBool = (v, def) => {
  if (v === undefined || v === null) return def;
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "no", "n", "off"].includes(s)) return false;
  return def;
};

// stickers helpers
function normalizeStickerPackCode(v) {
  return String(v || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");
}

function validateStickerPackCode(code) {
  return /^[a-z0-9][a-z0-9._-]{0,63}$/.test(code);
}

function normalizeStickerString(v) {
  return String(v ?? "").trim();
}

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

// -------------------- PROXY TO NOTIFICATION-SERVICE --------------------
async function proxyToNotification(req, res) {
  try {
    const target = new URL(NOTIFICATION_URL + req.originalUrl);

    const headers = {};
    if (req.headers.authorization) headers["authorization"] = req.headers.authorization;
    headers["content-type"] = "application/json; charset=utf-8";

    const method = req.method.toUpperCase();
    const hasBody = !["GET", "HEAD"].includes(method);

    const r = await fetch(target.toString(), {
      method,
      headers,
      body: hasBody ? JSON.stringify(req.body ?? {}) : undefined,
    });

    const text = await r.text();
    const ct = r.headers.get("content-type");
    if (ct) res.setHeader("content-type", ct);

    res.status(r.status).send(text);
  } catch (e) {
    log("error", "proxy.notification.failed", { err: e?.message || String(e), path: req.originalUrl });
    res.status(502).json({ error: "notification-service unavailable" });
  }
}

async function proxyToRealtime(req, res) {
  try {
    const target = new URL(REALTIME_URL + req.originalUrl);

    const headers = {};
    if (req.headers.authorization) headers["authorization"] = req.headers.authorization;
    headers["content-type"] = "application/json; charset=utf-8";

    const method = req.method.toUpperCase();
    const hasBody = !["GET", "HEAD"].includes(method);

    const r = await fetch(target.toString(), {
      method,
      headers,
      body: hasBody ? JSON.stringify(req.body ?? {}) : undefined,
    });

    const text = await r.text();
    const ct = r.headers.get("content-type");
    if (ct) res.setHeader("content-type", ct);

    res.status(r.status).send(text);
  } catch (e) {
    log("error", "proxy.realtime.failed", { err: e?.message || String(e), path: req.originalUrl });
    res.status(502).json({ error: "realtime-service unavailable" });
  }
}

async function sendInternalPush({ kind, to_user_id, from_user_id, type, title, body, data }) {
  if (!ADMIN_SECRET_KEY) {
    log("warn", "internal.push.skip.no_admin_key");
    return;
  }
  try {
    const url = `${NOTIFICATION_URL}/internal/push/send`;
    const payload = {
      kind,
      to_user_id,
      from_user_id: from_user_id ?? null,
      type,
      title,
      body,
      data: data || {},
    };

    const r = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json; charset=utf-8",
        "x-admin-key": ADMIN_SECRET_KEY,
      },
      body: JSON.stringify(payload),
    });

    if (!r.ok) {
      const txt = await r.text().catch(() => "");
      log("warn", "internal.push.failed", { code: r.status, kind, to_user_id, txt: txt.slice(0, 300) });
    }
  } catch (e) {
    log("warn", "internal.push.failed", { err: e?.message || String(e), kind, to_user_id });
  }
}

// -------------------- MENTIONS --------------------
// формат: @nickname#123
const MENTION_RE = /@([\p{L}\p{N}_\-\.]{1,64})#(\d{1,10})/gu;

function extractMentionTokens(text) {
  const out = [];
  if (!text) return out;
  let m;
  while ((m = MENTION_RE.exec(text)) !== null) {
    const nick = (m[1] || "").toString();
    const id = Number(m[2]);
    if (Number.isInteger(id) && id > 0) out.push({ id, nick });
    if (out.length >= 50) break; // защита
  }
  return out;
}

async function resolveMentions(text) {
  const tokens = extractMentionTokens(text).slice(0, 20);
  if (!tokens.length) return [];

  const ids = Array.from(new Set(tokens.map((t) => t.id))).slice(0, 20);
  const r = await pool.query(
    `SELECT id, nickname
     FROM users
     WHERE id = ANY($1::int[])`,
    [ids]
  );

  const nickById = new Map(r.rows.map((x) => [Number(x.id), String(x.nickname || "")]));
  const ok = [];

  for (const t of tokens) {
    const realNick = nickById.get(t.id);
    if (!realNick) continue;
    if (realNick.toLowerCase() !== String(t.nick || "").toLowerCase()) continue;
    ok.push(t.id);
  }

  return Array.from(new Set(ok)).slice(0, 20);
}

// -------------------- DB: triggers for NOTIFY (for realtime-service) --------------------
async function ensureNotifyTriggers(client) {
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
          'created_at', NEW.created_at,
          'reply_to_id', NEW.reply_to_id,
          'mention_user_ids', NEW.mention_user_ids
        )::text
      );
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
  `);

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
          'created_at', NEW.created_at,
          'reply_to_id', NEW.reply_to_id,
          'mention_user_ids', NEW.mention_user_ids
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
      await client.query(`ALTER TABLE global_messages ADD COLUMN IF NOT EXISTS reply_to_id BIGINT;`);
      await client.query(`ALTER TABLE global_messages ADD COLUMN IF NOT EXISTS mention_user_ids INTEGER[];`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_global_messages_id_desc ON global_messages(id DESC);`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_global_messages_reply_to ON global_messages(reply_to_id);`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_global_messages_mentions_gin ON global_messages USING GIN (mention_user_ids);`);

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
      await client.query(`ALTER TABLE dm_messages ADD COLUMN IF NOT EXISTS reply_to_id BIGINT;`);
      await client.query(`ALTER TABLE dm_messages ADD COLUMN IF NOT EXISTS mention_user_ids INTEGER[];`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_messages_conv_id_desc ON dm_messages(conversation_id, id DESC);`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_messages_reply_to ON dm_messages(conversation_id, reply_to_id);`);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_dm_messages_conv_sender_id_desc
        ON dm_messages(conversation_id, sender_user_id, id DESC);
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_messages_mentions_gin ON dm_messages USING GIN (mention_user_ids);`);

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

      // reactions
      await client.query(`
        CREATE TABLE IF NOT EXISTS global_reactions (
          message_id BIGINT NOT NULL,
          user_id INTEGER NOT NULL,
          reaction SMALLINT NOT NULL CHECK (reaction IN (-1, 1)),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (message_id, user_id)
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_global_reactions_msg ON global_reactions(message_id);`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS dm_reactions (
          message_id BIGINT NOT NULL,
          user_id INTEGER NOT NULL,
          reaction SMALLINT NOT NULL CHECK (reaction IN (-1, 1)),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (message_id, user_id)
        );
      `);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_dm_reactions_msg ON dm_reactions(message_id);`);

      // -------------------- STICKERS (packs + items + grants) --------------------
      await client.query(`
        CREATE TABLE IF NOT EXISTS sticker_packs (
          id SERIAL PRIMARY KEY,
          code TEXT NOT NULL UNIQUE,
          title TEXT NOT NULL,
          is_default BOOLEAN NOT NULL DEFAULT false,
          is_active BOOLEAN NOT NULL DEFAULT true,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`ALTER TABLE sticker_packs ADD COLUMN IF NOT EXISTS code TEXT;`);
      await client.query(`ALTER TABLE sticker_packs ADD COLUMN IF NOT EXISTS title TEXT;`);
      await client.query(`ALTER TABLE sticker_packs ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT false;`);
      await client.query(`ALTER TABLE sticker_packs ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT true;`);
      await client.query(`ALTER TABLE sticker_packs ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`);

      await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_sticker_packs_code ON sticker_packs(code);`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_sticker_packs_active ON sticker_packs(is_active, id);`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_sticker_packs_default ON sticker_packs(is_default, id);`);

      await client.query(`
        CREATE TABLE IF NOT EXISTS sticker_pack_items (
          id BIGSERIAL PRIMARY KEY,
          pack_id INTEGER NOT NULL REFERENCES sticker_packs(id) ON DELETE CASCADE,
          value TEXT NOT NULL,
          sort_order INTEGER NOT NULL DEFAULT 0,
          is_active BOOLEAN NOT NULL DEFAULT true,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);

      await client.query(`ALTER TABLE sticker_pack_items ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0;`);
      await client.query(`ALTER TABLE sticker_pack_items ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT true;`);
      await client.query(`ALTER TABLE sticker_pack_items ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;`);

      await client.query(`
        CREATE UNIQUE INDEX IF NOT EXISTS uq_sticker_pack_items_pack_value
        ON sticker_pack_items(pack_id, value);
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_sticker_pack_items_pack_sort
        ON sticker_pack_items(pack_id, is_active, sort_order, id);
      `);

      await client.query(`
        CREATE TABLE IF NOT EXISTS user_sticker_packs (
          user_id INTEGER NOT NULL,
          pack_id INTEGER NOT NULL REFERENCES sticker_packs(id) ON DELETE CASCADE,
          granted_by_user_id INTEGER,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (user_id, pack_id)
        );
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_user_sticker_packs_user
        ON user_sticker_packs(user_id, pack_id);
      `);

      // Гарантируем наличие дефолтного пака (доступен всем автоматически)
      await client.query(`
        INSERT INTO sticker_packs (code, title, is_default, is_active)
        VALUES ('default', 'Default', true, true)
        ON CONFLICT (code)
        DO UPDATE SET
          title = EXCLUDED.title,
          is_default = true,
          is_active = true
      `);

      await ensureNotifyTriggers(client);

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

// -------------------- REALTIME ROUTES (PROXY) --------------------
app.get("/events", authenticateToken, proxyToRealtime);

// -------------------- BASIC ROUTES --------------------
app.get("/ping", (req, res) => {
  res.json({ status: "ok", message: "pong", service: "game-mail-service", server_time: new Date().toISOString() });
});

// -------------------- NOTIFICATIONS ROUTES (PROXY) --------------------
app.get("/settings/notifications", authenticateToken, proxyToNotification);
app.post("/settings/notifications", authenticateToken, proxyToNotification);

app.get("/prefs/notifications", authenticateToken, proxyToNotification);
app.post("/prefs/notifications", authenticateToken, proxyToNotification);

app.post("/push/register", authenticateToken, proxyToNotification);

app.get("/chat/dm/:conversation_id/mute", authenticateToken, proxyToNotification);
app.post("/chat/dm/:conversation_id/mute", authenticateToken, proxyToNotification);

app.get("/notifications/summary", authenticateToken, proxyToNotification);

app.post("/presence/offline", authenticateToken, proxyToRealtime);
app.get("/presence/online/:userId", authenticateToken, proxyToRealtime);
app.get("/presence/online-batch", authenticateToken, proxyToRealtime);

// -------------------- USERS SEARCH --------------------
app.get("/users/search", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const nickname = (req.query.nickname || "").toString().trim();
  const limit = clampInt(req.query.limit, 20, 1, 50);
  const contains = parseBool(req.query.contains, false);
  const excludeSelf = parseBool(req.query.exclude_self, false);

  if (!nickname) return res.status(400).json({ error: "nickname query required" });

  try {
    const pattern = contains ? `%${nickname}%` : `${nickname}%`;

    const r = await pool.query(
      `SELECT id, nickname
       FROM users
       WHERE nickname ILIKE $1
         AND ($2::boolean = false OR id <> $3)
       ORDER BY id ASC
       LIMIT $4`,
      [pattern, excludeSelf, req.user.uid, limit]
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

// -------------------- STICKERS --------------------

// Admin: список паков (чтобы удобно получать pack_id, включая default)
app.get("/admin/stickers/packs", requireAdmin, async (req, res) => {
  try {
    const rows = await pool.query(
      `
      SELECT
        p.id, p.code, p.title, p.is_default, p.is_active, p.created_at,
        COALESCE(cnt.items_count, 0)::int AS items_count
      FROM sticker_packs p
      LEFT JOIN (
        SELECT pack_id, COUNT(*)::int AS items_count
        FROM sticker_pack_items
        WHERE is_active = true
        GROUP BY pack_id
      ) cnt ON cnt.pack_id = p.id
      ORDER BY p.is_default DESC, p.id ASC
      `
    );

    res.json({
      packs: rows.rows.map((r) => ({
        id: r.id,
        code: r.code,
        title: r.title,
        is_default: r.is_default === true,
        is_active: r.is_active === true,
        items_count: r.items_count || 0,
        created_at: r.created_at,
      })),
    });
  } catch (e) {
    log("error", "stickers.admin.list_packs.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// Admin: создать новый пак (не default)
app.post("/admin/stickers/packs", requireAdmin, async (req, res) => {
  const code = normalizeStickerPackCode(req.body?.code);
  const title = String(req.body?.title || "").trim();
  const is_active = parseBool(req.body?.is_active, true);

  if (!code) return res.status(400).json({ error: "code required" });
  if (!validateStickerPackCode(code)) {
    return res.status(400).json({ error: "code format invalid (a-z0-9._-, max 64, starts with alnum)" });
  }
  if (!title) return res.status(400).json({ error: "title required" });
  if (title.length > 128) return res.status(400).json({ error: "title too long (max 128)" });
  if (code === "default") {
    return res.status(400).json({ error: "default pack is created automatically" });
  }

  try {
    const ins = await pool.query(
      `
      INSERT INTO sticker_packs (code, title, is_default, is_active)
      VALUES ($1, $2, false, $3)
      RETURNING id, code, title, is_default, is_active, created_at
      `,
      [code, title, is_active]
    );

    res.status(201).json({ pack: ins.rows[0] });
  } catch (e) {
    if (e?.code === "23505") {
      return res.status(409).json({ error: "pack code already exists" });
    }
    log("error", "stickers.admin.create_pack.failed", { err: e?.message || String(e), code });
    res.status(500).json({ error: "Database error" });
  }
});

// Admin: добавить стикер (string) в пак
app.post("/admin/stickers/packs/:pack_id/stickers", requireAdmin, async (req, res) => {
  const packId = Number(req.params.pack_id);
  const sticker = normalizeStickerString(req.body?.sticker); // string
  const sort_order = clampInt(req.body?.sort_order, 0, -1000000, 1000000);
  const is_active = parseBool(req.body?.is_active, true);

  if (!Number.isInteger(packId) || packId <= 0) return res.status(400).json({ error: "bad pack_id" });
  if (!sticker) return res.status(400).json({ error: "sticker required" });
  if (sticker.length > 256) return res.status(400).json({ error: "sticker too long (max 256)" });

  try {
    const pack = await pool.query(
      `SELECT id, code, title, is_default, is_active FROM sticker_packs WHERE id = $1 LIMIT 1`,
      [packId]
    );
    if (!pack.rows.length) return res.status(404).json({ error: "pack not found" });

    const ins = await pool.query(
      `
      INSERT INTO sticker_pack_items (pack_id, value, sort_order, is_active)
      VALUES ($1, $2, $3, $4)
      RETURNING id, pack_id, value, sort_order, is_active, created_at
      `,
      [packId, sticker, sort_order, is_active]
    );

    res.status(201).json({
      pack: {
        id: pack.rows[0].id,
        code: pack.rows[0].code,
        title: pack.rows[0].title,
        is_default: pack.rows[0].is_default === true,
        is_active: pack.rows[0].is_active === true,
      },
      sticker: {
        id: ins.rows[0].id,
        value: ins.rows[0].value,
        sort_order: ins.rows[0].sort_order,
        is_active: ins.rows[0].is_active === true,
        created_at: ins.rows[0].created_at,
      },
    });
  } catch (e) {
    if (e?.code === "23505") {
      return res.status(409).json({ error: "sticker already exists in this pack" });
    }
    log("error", "stickers.admin.add_item.failed", { err: e?.message || String(e), packId });
    res.status(500).json({ error: "Database error" });
  }
});

// Admin: выдать пак пользователю по user_id
app.post("/admin/stickers/packs/:pack_id/grant-user", requireAdmin, async (req, res) => {
  const packId = Number(req.params.pack_id);
  const userId = Number(req.body?.user_id);

  if (!Number.isInteger(packId) || packId <= 0) return res.status(400).json({ error: "bad pack_id" });
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ error: "user_id must be integer" });

  try {
    const u = await getUserBasic(userId);
    if (!u) return res.status(404).json({ error: "User not found" });

    const p = await pool.query(
      `SELECT id, code, title, is_default, is_active FROM sticker_packs WHERE id = $1 LIMIT 1`,
      [packId]
    );
    if (!p.rows.length) return res.status(404).json({ error: "pack not found" });

    const pack = p.rows[0];
    if (pack.is_default === true) {
      return res.json({
        ok: true,
        granted: false,
        reason: "pack_is_default_and_already_available_to_all",
        user: { id: u.id, login: u.login, nickname: u.nickname },
        pack: {
          id: pack.id,
          code: pack.code,
          title: pack.title,
          is_default: true,
          is_active: pack.is_active === true,
        },
      });
    }

    const ins = await pool.query(
      `
      INSERT INTO user_sticker_packs (user_id, pack_id, granted_by_user_id)
      VALUES ($1, $2, NULL)
      ON CONFLICT (user_id, pack_id) DO NOTHING
      RETURNING user_id, pack_id, created_at
      `,
      [userId, packId]
    );

    res.json({
      ok: true,
      granted: ins.rows.length > 0,
      user: { id: u.id, login: u.login, nickname: u.nickname },
      pack: {
        id: pack.id,
        code: pack.code,
        title: pack.title,
        is_default: pack.is_default === true,
        is_active: pack.is_active === true,
      },
      created_at: ins.rows[0]?.created_at || null,
    });
  } catch (e) {
    log("error", "stickers.admin.grant_pack.failed", { err: e?.message || String(e), packId, userId });
    res.status(500).json({ error: "Database error" });
  }
});

// User: получить доступные стикеры (дефолт + выданные)
async function handleGetAvailableStickers(req, res) {
  await touchPresence(req.user.uid);

  try {
    const r = await pool.query(
      `
      SELECT
        p.id AS pack_id,
        p.code AS pack_code,
        p.title AS pack_title,
        p.is_default,
        p.is_active,
        p.created_at AS pack_created_at,

        s.id AS sticker_id,
        s.value AS sticker_value,
        s.sort_order,
        s.is_active AS sticker_is_active,
        s.created_at AS sticker_created_at

      FROM sticker_packs p
      LEFT JOIN sticker_pack_items s
        ON s.pack_id = p.id
       AND s.is_active = true

      WHERE p.is_active = true
        AND (
          p.is_default = true
          OR EXISTS (
            SELECT 1
            FROM user_sticker_packs usp
            WHERE usp.user_id = $1
              AND usp.pack_id = p.id
          )
        )

      ORDER BY p.is_default DESC, p.id ASC, s.sort_order ASC, s.id ASC
      `,
      [req.user.uid]
    );

    const packsMap = new Map();

    for (const row of r.rows) {
      let pack = packsMap.get(row.pack_id);
      if (!pack) {
        pack = {
          id: row.pack_id,
          code: row.pack_code,
          title: row.pack_title,
          is_default: row.is_default === true,
          is_active: row.is_active === true,
          created_at: row.pack_created_at,
          stickers: [],
        };
        packsMap.set(row.pack_id, pack);
      }

      if (row.sticker_id !== null && row.sticker_id !== undefined) {
        pack.stickers.push({
          id: row.sticker_id,
          value: row.sticker_value,
          sort_order: row.sort_order ?? 0,
          created_at: row.sticker_created_at,
        });
      }
    }

    const packs = Array.from(packsMap.values());
    const all_stickers = [];
    for (const p of packs) {
      for (const s of p.stickers) all_stickers.push(s.value);
    }

    res.json({
      user_id: req.user.uid,
      total_packs: packs.length,
      total_stickers: all_stickers.length,
      packs,
      all_stickers,
    });
  } catch (e) {
    log("error", "stickers.user.available.failed", { err: e?.message || String(e), uid: req.user.uid });
    res.status(500).json({ error: "Database error" });
  }
}

app.get("/stickers/available", authenticateToken, handleGetAvailableStickers);
app.get("/stickers/my", authenticateToken, handleGetAvailableStickers);

// -------------------- GLOBAL CHAT HISTORY (reply + reactions + my_reaction) --------------------
app.get("/chat/global/history", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const limit = clampInt(req.query.limit, 100, 1, 100);
  const beforeId = req.query.before_id ? Number(req.query.before_id) : null;

  try {
    const params = [];
    let where = "";
    if (beforeId) { where = "WHERE m.id < $1"; params.push(beforeId); }
    params.push(limit);
    const uidParamIndex = params.length + 1;
    params.push(req.user.uid);

    const q = `
      WITH base AS (
        SELECT m.*
        FROM global_messages m
        ${where}
        ORDER BY m.id DESC
        LIMIT $${beforeId ? 2 : 1}
      ),
      react AS (
        SELECT
          r.message_id,
          COUNT(*) FILTER (WHERE r.reaction = 1)::int  AS like_count,
          COUNT(*) FILTER (WHERE r.reaction = -1)::int AS dislike_count
        FROM global_reactions r
        WHERE r.message_id IN (SELECT id FROM base)
        GROUP BY r.message_id
      ),
      my AS (
        SELECT r.message_id, r.reaction
        FROM global_reactions r
        WHERE r.user_id = $${uidParamIndex}
          AND r.message_id IN (SELECT id FROM base)
      )
      SELECT
        m.id, m.user_id, m.nickname, m.body, m.created_at, m.reply_to_id, m.mention_user_ids,
        COALESCE(react.like_count, 0)    AS like_count,
        COALESCE(react.dislike_count, 0) AS dislike_count,
        COALESCE(my.reaction, 0)         AS my_reaction,

        rt.id         AS reply_id,
        rt.user_id    AS reply_user_id,
        rt.nickname   AS reply_nickname,
        rt.body       AS reply_body,
        rt.created_at AS reply_created_at

      FROM base m
      LEFT JOIN global_messages rt ON rt.id = m.reply_to_id
      LEFT JOIN react ON react.message_id = m.id
      LEFT JOIN my    ON my.message_id    = m.id
      ORDER BY m.id ASC
    `;

    const r = await pool.query(q, params);

    const items = r.rows.map((m) => ({
      id: m.id,
      body: m.body,
      created_at: m.created_at,
      like_count: m.like_count || 0,
      dislike_count: m.dislike_count || 0,
      my_reaction: m.my_reaction || 0,
      mention_user_ids: m.mention_user_ids || [],
      reply_to: m.reply_id ? {
        id: m.reply_id,
        body: m.reply_body,
        created_at: m.reply_created_at,
        sender: { user_id: m.reply_user_id, nickname: m.reply_nickname, header: `${m.reply_nickname}#${m.reply_user_id}` },
      } : null,
      sender: { user_id: m.user_id, nickname: m.nickname, header: `${m.nickname}#${m.user_id}` },
    }));

    const next_before_id = items.length ? items[0].id : null;
    res.json({ limit, next_before_id, messages: items });
  } catch (e) {
    log("error", "global.history.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- GLOBAL SEND (reply_to_id + mentions + mention push via notification-service) --------------------
app.post("/chat/global/send", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const body = (req.body?.body || "").toString();
  const replyToRaw = req.body?.reply_to_id;
  const reply_to_id = replyToRaw === null || replyToRaw === undefined || replyToRaw === "" ? null : Number(replyToRaw);

  if (!body.trim()) return res.status(400).json({ error: "Message body required" });
  if (body.length > 1024) return res.status(400).json({ error: "Message too long (max 1024)" });
  if (reply_to_id !== null && (!Number.isInteger(reply_to_id) || reply_to_id <= 0)) {
    return res.status(400).json({ error: "reply_to_id must be positive integer or null" });
  }

  try {
    const me = await getUserBasic(req.user.uid);
    if (!me) return res.status(401).json({ error: "User not found" });

    if (reply_to_id !== null) {
      const chk = await pool.query(`SELECT 1 FROM global_messages WHERE id = $1 LIMIT 1`, [reply_to_id]);
      if (!chk.rows.length) return res.status(400).json({ error: "reply_to_id not found" });
    }

    const mentionIds = await resolveMentions(body);

    const insert = await pool.query(
      `INSERT INTO global_messages (user_id, nickname, body, reply_to_id, mention_user_ids)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id, created_at`,
      [me.id, me.nickname, body, reply_to_id, mentionIds.length ? mentionIds : null]
    );

    // чистка до 100
    await pool.query(`
      DELETE FROM global_messages
      WHERE id NOT IN (SELECT id FROM global_messages ORDER BY id DESC LIMIT 100)
    `);
    await pool.query(`
      DELETE FROM global_reactions r
      WHERE NOT EXISTS (SELECT 1 FROM global_messages m WHERE m.id = r.message_id)
    `);

    const msgId = insert.rows[0].id;
    const createdAt = insert.rows[0].created_at;

    // mention push (в notification-service)
    if (mentionIds.length) {
      const snippet = body.length > 160 ? body.slice(0, 160) + "…" : body;
      for (const uid of mentionIds) {
        if (uid === me.id) continue;
        await sendInternalPush({
          kind: "mention",
          to_user_id: uid,
          from_user_id: me.id,
          type: "mention",
          title: `Упоминание от ${me.nickname}`,
          body: snippet,
          data: {
            context: "global",
            kind: "global",
            message_id: String(msgId),
            from_user_id: String(me.id),
            from_nickname: String(me.nickname),
          },
        });
      }
    }

    res.status(201).json({
      message: "sent",
      data: { id: msgId, created_at: createdAt, sender: { user_id: me.id, nickname: me.nickname, header: `${me.nickname}#${me.id}` } },
    });
  } catch (e) {
    log("error", "global.send.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- GLOBAL READ --------------------
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

// -------------------- GLOBAL REACT (+ reaction push via notification-service) --------------------
app.post("/chat/global/:message_id/react", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const messageId = Number(req.params.message_id);
  const reaction = Number(req.body?.reaction);

  if (!Number.isInteger(messageId) || messageId <= 0) return res.status(400).json({ error: "bad message_id" });
  if (![-1, 0, 1].includes(reaction)) return res.status(400).json({ error: "reaction must be -1, 0 or 1" });

  try {
    const msg = await pool.query(
      `SELECT id, user_id, nickname, body
       FROM global_messages
       WHERE id = $1
       LIMIT 1`,
      [messageId]
    );
    if (!msg.rows.length) return res.status(404).json({ error: "message not found" });

    const ownerId = Number(msg.rows[0].user_id);

    const prev = await pool.query(
      `SELECT reaction FROM global_reactions WHERE message_id = $1 AND user_id = $2 LIMIT 1`,
      [messageId, req.user.uid]
    );
    const prevReaction = Number(prev.rows[0]?.reaction || 0);

    if (reaction === 0) {
      await pool.query(`DELETE FROM global_reactions WHERE message_id = $1 AND user_id = $2`, [messageId, req.user.uid]);
    } else {
      await pool.query(
        `INSERT INTO global_reactions (message_id, user_id, reaction, updated_at)
         VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
         ON CONFLICT (message_id, user_id)
         DO UPDATE SET reaction = EXCLUDED.reaction, updated_at = CURRENT_TIMESTAMP`,
        [messageId, req.user.uid, reaction]
      );
    }

    // push на новую реакцию, если не себе (в notification-service)
    if (reaction !== 0 && reaction !== prevReaction && ownerId !== req.user.uid) {
      const me = await getUserBasic(req.user.uid);
      if (me) {
        const snippet = String(msg.rows[0].body || "");
        const cut = snippet.length > 140 ? snippet.slice(0, 140) + "…" : snippet;
        const emoji = reaction === 1 ? "👍" : "👎";

        await sendInternalPush({
          kind: "reaction",
          to_user_id: ownerId,
          from_user_id: me.id,
          type: "reaction",
          title: `${me.nickname} поставил ${emoji}`,
          body: cut,
          data: {
            context: "global",
            kind: "global",
            reaction: String(reaction),
            message_id: String(messageId),
            from_user_id: String(me.id),
            from_nickname: String(me.nickname),
          },
        });
      }
    }

    res.json({ ok: true });
  } catch (e) {
    log("error", "global.react.failed", { err: e?.message || String(e), messageId });
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

// -------------------- DM HISTORY (reply + reactions + my_reaction) --------------------
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

    const params = [conversationId];
    let beforeWhere = "";
    if (beforeId) { beforeWhere = "AND m.id < $2"; params.push(beforeId); params.push(limit); }
    else { params.push(limit); }
    const uidParamIndex = params.length + 1;
    params.push(req.user.uid);

    const q = `
      WITH base AS (
        SELECT m.*
        FROM dm_messages m
        WHERE m.conversation_id = $1
        ${beforeWhere}
        ORDER BY m.id DESC
        LIMIT $${beforeId ? 3 : 2}
      ),
      react AS (
        SELECT
          r.message_id,
          COUNT(*) FILTER (WHERE r.reaction = 1)::int  AS like_count,
          COUNT(*) FILTER (WHERE r.reaction = -1)::int AS dislike_count
        FROM dm_reactions r
        WHERE r.message_id IN (SELECT id FROM base)
        GROUP BY r.message_id
      ),
      my AS (
        SELECT r.message_id, r.reaction
        FROM dm_reactions r
        WHERE r.user_id = $${uidParamIndex}
          AND r.message_id IN (SELECT id FROM base)
      )
      SELECT
        m.id, m.sender_user_id, m.sender_nickname, m.body, m.created_at, m.reply_to_id, m.mention_user_ids,
        COALESCE(react.like_count, 0)    AS like_count,
        COALESCE(react.dislike_count, 0) AS dislike_count,
        COALESCE(my.reaction, 0)         AS my_reaction,

        rt.id               AS reply_id,
        rt.sender_user_id   AS reply_user_id,
        rt.sender_nickname  AS reply_nickname,
        rt.body             AS reply_body,
        rt.created_at       AS reply_created_at

      FROM base m
      LEFT JOIN dm_messages rt ON rt.id = m.reply_to_id
      LEFT JOIN react ON react.message_id = m.id
      LEFT JOIN my    ON my.message_id    = m.id
      ORDER BY m.id ASC
    `;

    const r = await pool.query(q, params);

    const items = r.rows.map((m) => ({
      id: m.id,
      body: m.body,
      created_at: m.created_at,
      like_count: m.like_count || 0,
      dislike_count: m.dislike_count || 0,
      my_reaction: m.my_reaction || 0,
      mention_user_ids: m.mention_user_ids || [],
      reply_to: m.reply_id ? {
        id: m.reply_id,
        body: m.reply_body,
        created_at: m.reply_created_at,
        sender: { user_id: m.reply_user_id, nickname: m.reply_nickname, header: `${m.reply_nickname}#${m.reply_user_id}` },
      } : null,
      sender: { user_id: m.sender_user_id, nickname: m.sender_nickname, header: `${m.sender_nickname}#${m.sender_user_id}` },
    }));

    const next_before_id = items.length ? items[0].id : null;
    res.json({ conversation_id: conversationId, limit, next_before_id, messages: items });
  } catch (e) {
    log("error", "dm.history.failed", { err: e?.message || String(e), conversationId });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- DM SEND (reply_to_id + mentions + push via notification-service) --------------------
app.post("/chat/dm/:conversation_id/send", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const conversationId = req.params.conversation_id;
  const body = (req.body?.body || "").toString();

  const replyToRaw = req.body?.reply_to_id;
  const reply_to_id = replyToRaw === null || replyToRaw === undefined || replyToRaw === "" ? null : Number(replyToRaw);

  if (!body.trim()) return res.status(400).json({ error: "Message body required" });
  if (body.length > 1024) return res.status(400).json({ error: "Message too long (max 1024)" });
  if (reply_to_id !== null && (!Number.isInteger(reply_to_id) || reply_to_id <= 0)) {
    return res.status(400).json({ error: "reply_to_id must be positive integer or null" });
  }

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

    if (reply_to_id !== null) {
      const chk = await client.query(
        `SELECT 1 FROM dm_messages WHERE id = $1 AND conversation_id = $2 LIMIT 1`,
        [reply_to_id, conversationId]
      );
      if (!chk.rows.length) {
        await client.query("ROLLBACK");
        return res.status(400).json({ error: "reply_to_id not found in this conversation" });
      }
    }

    const me = await client.query("SELECT id, nickname FROM users WHERE id = $1 LIMIT 1", [req.user.uid]);
    if (me.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(401).json({ error: "User not found" });
    }
    const myNick = me.rows[0].nickname;

    const mentionIds = await resolveMentions(body);

    const ins = await client.query(
      `INSERT INTO dm_messages (conversation_id, sender_user_id, sender_nickname, body, reply_to_id, mention_user_ids)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, created_at`,
      [conversationId, req.user.uid, myNick, body, reply_to_id, mentionIds.length ? mentionIds : null]
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

    // push receiver (mute/settings — в notification-service)
    await sendInternalPush({
      kind: "dm",
      to_user_id: otherId,
      from_user_id: req.user.uid,
      type: "dm",
      title: `Сообщение от ${myNick}`,
      body: body.length > 120 ? body.slice(0, 120) + "…" : body,
      data: {
        context: "dm",
        kind: "dm",
        conversation_id: conversationId,
        sender_user_id: String(req.user.uid),
        sender_nickname: String(myNick),
        from_user_id: String(req.user.uid),
        from_nickname: String(myNick),
      },
    });

    res.status(201).json({
      message: "sent",
      data: { id: msgId, created_at: createdAt, sender: { user_id: req.user.uid, nickname: myNick, header: `${myNick}#${req.user.uid}` } },
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    log("error", "dm.send.failed", { err: e?.message || String(e), conversationId });
    res.status(500).json({ error: "Database error" });
  } finally {
    client.release();
  }
});

// -------------------- DM REACT (+ reaction push via notification-service) --------------------
app.post("/chat/dm/:conversation_id/messages/:message_id/react", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const conversationId = req.params.conversation_id;
  const messageId = Number(req.params.message_id);
  const reaction = Number(req.body?.reaction);

  if (!Number.isInteger(messageId) || messageId <= 0) return res.status(400).json({ error: "bad message_id" });
  if (![-1, 0, 1].includes(reaction)) return res.status(400).json({ error: "reaction must be -1, 0 or 1" });

  try {
    const pair = await pool.query(
      `SELECT user_low, user_high FROM dm_pairs WHERE conversation_id = $1 LIMIT 1`,
      [conversationId]
    );
    if (!pair.rows.length) return res.status(404).json({ error: "conversation not found" });

    const { user_low, user_high } = pair.rows[0];
    if (req.user.uid !== user_low && req.user.uid !== user_high) return res.status(403).json({ error: "Not a member of this conversation" });

    const msg = await pool.query(
      `SELECT id, sender_user_id, body
       FROM dm_messages
       WHERE id = $1 AND conversation_id = $2
       LIMIT 1`,
      [messageId, conversationId]
    );
    if (!msg.rows.length) return res.status(404).json({ error: "message not found in this conversation" });

    const ownerId = Number(msg.rows[0].sender_user_id);

    const prev = await pool.query(
      `SELECT reaction FROM dm_reactions WHERE message_id = $1 AND user_id = $2 LIMIT 1`,
      [messageId, req.user.uid]
    );
    const prevReaction = Number(prev.rows[0]?.reaction || 0);

    if (reaction === 0) {
      await pool.query(`DELETE FROM dm_reactions WHERE message_id = $1 AND user_id = $2`, [messageId, req.user.uid]);
    } else {
      await pool.query(
        `INSERT INTO dm_reactions (message_id, user_id, reaction, updated_at)
         VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
         ON CONFLICT (message_id, user_id)
         DO UPDATE SET reaction = EXCLUDED.reaction, updated_at = EXCLUDED.updated_at`,
        [messageId, req.user.uid, reaction]
      );
    }

    if (reaction !== 0 && reaction !== prevReaction && ownerId !== req.user.uid) {
      const me = await getUserBasic(req.user.uid);
      if (me) {
        const snippet = String(msg.rows[0].body || "");
        const cut = snippet.length > 140 ? snippet.slice(0, 140) + "…" : snippet;
        const emoji = reaction === 1 ? "👍" : "👎";

        await sendInternalPush({
          kind: "reaction",
          to_user_id: ownerId,
          from_user_id: me.id,
          type: "reaction",
          title: `${me.nickname} поставил ${emoji}`,
          body: cut,
          data: {
            context: "dm",
            kind: "dm",
            reaction: String(reaction),
            conversation_id: String(conversationId),
            message_id: String(messageId),
            from_user_id: String(me.id),
            from_nickname: String(me.nickname),
          },
        });
      }
    }

    res.json({ ok: true });
  } catch (e) {
    log("error", "dm.react.failed", { err: e?.message || String(e), conversationId, messageId });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- CONTACTS (unread_count + muted if table exists) --------------------
app.get("/chat/contacts", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const qWithMute = `
    SELECT
      c.contact_user_id,
      c.conversation_id,
      c.last_message_id,
      c.last_read_message_id,
      c.last_at,
      u.nickname,
      COALESCE(m.muted, false) AS muted,
      (
        SELECT COUNT(*)::int
        FROM dm_messages mm
        WHERE mm.conversation_id = c.conversation_id
          AND mm.sender_user_id = c.contact_user_id
          AND mm.id > COALESCE(c.last_read_message_id, 0)
      ) AS unread_count
    FROM dm_contacts c
    JOIN users u ON u.id = c.contact_user_id
    LEFT JOIN dm_mutes m
      ON m.owner_user_id = c.owner_user_id
     AND m.muted_user_id = c.contact_user_id
    WHERE c.owner_user_id = $1
    ORDER BY c.last_at DESC NULLS LAST
    LIMIT 200
  `;

  const qNoMute = `
    SELECT
      c.contact_user_id,
      c.conversation_id,
      c.last_message_id,
      c.last_read_message_id,
      c.last_at,
      u.nickname,
      false AS muted,
      (
        SELECT COUNT(*)::int
        FROM dm_messages mm
        WHERE mm.conversation_id = c.conversation_id
          AND mm.sender_user_id = c.contact_user_id
          AND mm.id > COALESCE(c.last_read_message_id, 0)
      ) AS unread_count
    FROM dm_contacts c
    JOIN users u ON u.id = c.contact_user_id
    WHERE c.owner_user_id = $1
    ORDER BY c.last_at DESC NULLS LAST
    LIMIT 200
  `;

  try {
    let rows;
    try {
      const r = await pool.query(qWithMute, [req.user.uid]);
      rows = r.rows;
    } catch (e) {
      const msg = String(e?.message || "");
      if (msg.includes("dm_mutes") && msg.includes("does not exist")) {
        const r2 = await pool.query(qNoMute, [req.user.uid]);
        rows = r2.rows;
      } else {
        throw e;
      }
    }

    res.json({
      contacts: rows.map((x) => ({
        contact_user_id: x.contact_user_id,
        nickname: x.nickname,
        header: `${x.nickname}#${x.contact_user_id}`,
        conversation_id: x.conversation_id,
        last_message_id: x.last_message_id,
        last_read_message_id: x.last_read_message_id,
        last_at: x.last_at,
        unread_count: x.unread_count || 0,
        muted: x.muted === true,
      })),
    });
  } catch (e) {
    log("error", "contacts.failed", { err: e?.message || String(e) });
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- DM READ --------------------
app.post("/chat/dm/:conversation_id/read", authenticateToken, async (req, res) => {
  await touchPresence(req.user.uid);

  const conversationId = req.params.conversation_id;
  const last_read_message_id = Number(req.body?.last_read_message_id);

  if (!Number.isInteger(last_read_message_id) || last_read_message_id < 0) {
    return res.status(400).json({ error: "last_read_message_id must be integer >= 0" });
  }

  try {
    const pair = await pool.query(
      `SELECT user_low, user_high FROM dm_pairs WHERE conversation_id = $1 LIMIT 1`,
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

// -------------------- START --------------------
async function main() {
  log("info", "boot", { instance: INSTANCE, port: PORT, log_level: LOG_LEVEL, notif_url: NOTIFICATION_URL, realtime_url: REALTIME_URL });
  await initDbForever();
  app.listen(PORT, () => log("info", "http.listening", { port: PORT, instance: INSTANCE }));
}

main().catch((e) => {
  log("error", "fatal", { err: e?.message || String(e) });
  process.exit(1);
});
