const express = require("express");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const { v4: uuidv4 } = require("uuid");
const { readEnv } = require("./env");

const app = express();
app.use(express.json({ limit: "1mb" }));

let ACCESS_SECRET, ADMIN_SECRET_KEY, DATABASE_URL, PORT;
try {
  ACCESS_SECRET = readEnv("ACCESS_SECRET");
  ADMIN_SECRET_KEY = readEnv("ADMIN_SECRET_KEY");
  DATABASE_URL = readEnv("DATABASE_URL");
  PORT = process.env.PORT || 3001;
} catch (e) {
  console.error("❌", e.message);
  process.exit(1);
}

const pool = new Pool({ connectionString: DATABASE_URL });

// дальше твой код без изменений...


// --- HELPERS ---
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
    if (err) return res.status(403).json({ error: "Access token expired or invalid" });
    req.user = payload; // { uid, login }
    next();
  });
};

async function getUserBasic(userId) {
  const r = await pool.query(
    "SELECT id, login, nickname FROM users WHERE id = $1 LIMIT 1",
    [userId]
  );
  return r.rows[0] || null;
}

// --- DB INIT ---
async function initDb() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS global_messages (
        id BIGSERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL,
        nickname TEXT NOT NULL,
        body TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_global_messages_id_desc ON global_messages(id DESC);
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS conversations (
        id UUID PRIMARY KEY,
        type TEXT NOT NULL CHECK (type IN ('dm')),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS dm_pairs (
        user_low INTEGER NOT NULL,
        user_high INTEGER NOT NULL,
        conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_low, user_high)
      );

      CREATE TABLE IF NOT EXISTS dm_messages (
        id BIGSERIAL PRIMARY KEY,
        conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
        sender_user_id INTEGER NOT NULL,
        sender_nickname TEXT NOT NULL,
        body TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_dm_messages_conv_id_desc ON dm_messages(conversation_id, id DESC);

      CREATE TABLE IF NOT EXISTS dm_contacts (
        owner_user_id INTEGER NOT NULL,
        contact_user_id INTEGER NOT NULL,
        conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
        last_message_id BIGINT,
        last_at TIMESTAMP,
        PRIMARY KEY (owner_user_id, contact_user_id)
      );
      CREATE INDEX IF NOT EXISTS idx_dm_contacts_owner_last_at ON dm_contacts(owner_user_id, last_at DESC);
    `);

    await pool.query(`
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
      CREATE INDEX IF NOT EXISTS idx_mails_to_created_desc ON mails(to_user_id, created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_mails_to_status_created_desc ON mails(to_user_id, status, created_at DESC);
    `);

    // если таблица mail_claims осталась от старой версии — можно удалить вручную (см. ниже)
    console.log("✅ game-mail-service: DB initialized");
  } catch (err) {
    console.error("❌ DB init error:", err?.message || err, "Retrying in 5s...");
    setTimeout(initDb, 5000);
  }
}

initDb();

// --- ROUTES ---
app.get("/ping", (req, res) => {
  res.json({ status: "ok", message: "pong", service: "game-mail-service", server_time: new Date().toISOString() });
});

/**
 * Поиск друзей по никнейму (публичный поиск внутри игры)
 * GET /users/search?nickname=кот&limit=20
 *
 * Возвращает список {id, nickname}
 * Важно: nickname у тебя НЕ уникальный → это норм, просто выдаём несколько.
 */
app.get("/users/search", authenticateToken, async (req, res) => {
  const nickname = (req.query.nickname || "").toString().trim();
  const limit = clampInt(req.query.limit, 20, 1, 50);

  if (!nickname) return res.status(400).json({ error: "nickname query required" });

  try {
    // ILIKE для регистронезависимого поиска
    // Ограничиваем по префиксу: "кот%" — обычно лучше для UX и индексов, чем "%кот%"
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
      users: r.rows.map(u => ({ id: u.id, nickname: u.nickname, header: `${u.nickname}#${u.id}` }))
    });
  } catch (e) {
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- GLOBAL CHAT --------------------
app.get("/chat/global/history", authenticateToken, async (req, res) => {
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

    const items = r.rows.reverse().map(m => ({
      id: m.id,
      body: m.body,
      created_at: m.created_at,
      sender: {
        user_id: m.user_id,
        nickname: m.nickname,
        header: `${m.nickname}#${m.user_id}`
      }
    }));

    const next_before_id = items.length ? items[0].id : null;
    res.json({ limit, next_before_id, messages: items });
  } catch (e) {
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/chat/global/send", authenticateToken, async (req, res) => {
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

    await pool.query(`
      DELETE FROM global_messages
      WHERE id NOT IN (
        SELECT id FROM global_messages ORDER BY id DESC LIMIT 100
      )
    `);

    res.status(201).json({
      message: "sent",
      data: {
        id: insert.rows[0].id,
        created_at: insert.rows[0].created_at,
        sender: { user_id: me.id, nickname: me.nickname, header: `${me.nickname}#${me.id}` }
      }
    });
  } catch (e) {
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
  const contactId = Number(req.body?.contact_user_id);
  if (!Number.isInteger(contactId) || contactId <= 0) return res.status(400).json({ error: "contact_user_id must be integer" });
  if (contactId === req.user.uid) return res.status(400).json({ error: "Cannot chat with yourself" });

  try {
    const other = await getUserBasic(contactId);
    if (!other) return res.status(404).json({ error: "Contact user not found" });

    const cid = await getOrCreateDmConversation(req.user.uid, contactId);
    res.json({ conversation_id: cid, contact: { user_id: other.id, nickname: other.nickname, header: `${other.nickname}#${other.id}` } });
  } catch (e) {
    res.status(500).json({ error: "Database error" });
  }
});

app.get("/chat/dm/:conversation_id/history", authenticateToken, async (req, res) => {
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

    const items = r.rows.reverse().map(m => ({
      id: m.id,
      body: m.body,
      created_at: m.created_at,
      sender: {
        user_id: m.sender_user_id,
        nickname: m.sender_nickname,
        header: `${m.sender_nickname}#${m.sender_user_id}`
      }
    }));

    const next_before_id = items.length ? items[0].id : null;
    res.json({ conversation_id: conversationId, limit, next_before_id, messages: items });
  } catch (e) {
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/chat/dm/:conversation_id/send", authenticateToken, async (req, res) => {
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

    const ins = await client.query(
      `INSERT INTO dm_messages (conversation_id, sender_user_id, sender_nickname, body)
       VALUES ($1, $2, $3, $4)
       RETURNING id, created_at`,
      [conversationId, req.user.uid, me.rows[0].nickname, body]
    );

    const msgId = ins.rows[0].id;
    const createdAt = ins.rows[0].created_at;

    const otherId = req.user.uid === user_low ? user_high : user_low;

    await client.query(
      `INSERT INTO dm_contacts (owner_user_id, contact_user_id, conversation_id, last_message_id, last_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (owner_user_id, contact_user_id)
       DO UPDATE SET last_message_id = EXCLUDED.last_message_id, last_at = EXCLUDED.last_at, conversation_id = EXCLUDED.conversation_id`,
      [req.user.uid, otherId, conversationId, msgId, createdAt]
    );

    await client.query(
      `INSERT INTO dm_contacts (owner_user_id, contact_user_id, conversation_id, last_message_id, last_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (owner_user_id, contact_user_id)
       DO UPDATE SET last_message_id = EXCLUDED.last_message_id, last_at = EXCLUDED.last_at, conversation_id = EXCLUDED.conversation_id`,
      [otherId, req.user.uid, conversationId, msgId, createdAt]
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "sent",
      data: {
        id: msgId,
        created_at: createdAt,
        sender: { user_id: req.user.uid, nickname: me.rows[0].nickname, header: `${me.rows[0].nickname}#${req.user.uid}` }
      }
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    res.status(500).json({ error: "Database error" });
  } finally {
    client.release();
  }
});

app.get("/chat/contacts", authenticateToken, async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT c.contact_user_id, c.conversation_id, c.last_message_id, c.last_at, u.nickname
       FROM dm_contacts c
       JOIN users u ON u.id = c.contact_user_id
       WHERE c.owner_user_id = $1
       ORDER BY c.last_at DESC NULLS LAST
       LIMIT 200`,
      [req.user.uid]
    );

    res.json({
      contacts: r.rows.map(x => ({
        contact_user_id: x.contact_user_id,
        nickname: x.nickname,
        header: `${x.nickname}#${x.contact_user_id}`,
        conversation_id: x.conversation_id,
        last_message_id: x.last_message_id,
        last_at: x.last_at
      }))
    });
  } catch (e) {
    res.status(500).json({ error: "Database error" });
  }
});

// -------------------- MAIL --------------------
app.get("/mail/inbox", authenticateToken, async (req, res) => {
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
    res.status(500).json({ error: "Database error" });
  }
});

app.get("/mail/:id", authenticateToken, async (req, res) => {
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
    res.status(500).json({ error: "Database error" });
  }
});

app.post("/mail/:id/read", authenticateToken, async (req, res) => {
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
      const chk = await pool.query(
        `SELECT id, status FROM mails WHERE id = $1 AND to_user_id = $2 LIMIT 1`,
        [id, req.user.uid]
      );
      if (chk.rows.length === 0) return res.status(404).json({ error: "Mail not found" });
      return res.json({ mail: chk.rows[0] });
    }

    res.json({ mail: upd.rows[0] });
  } catch (e) {
    res.status(500).json({ error: "Database error" });
  }
});

/**
 * Claim без idempotency-key:
 * одно письмо = один клейм
 * Повторный вызов просто возвращает already_claimed.
 */
app.post("/mail/:id/claim", authenticateToken, async (req, res) => {
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

    await client.query(
      `UPDATE mails
       SET status = 'claimed'
       WHERE id = $1 AND to_user_id = $2`,
      [id, req.user.uid]
    );

    await client.query("COMMIT");
    res.json({ status: "claimed", applied: { money_mortals: addMortals, money_cultivators: addCult } });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
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
    res.status(500).json({ error: "Database error" });
  }
});

app.listen(PORT, () => console.log(`🚀 game-mail-service on port ${PORT}`));
