'use strict';

const express = require('express');
const jwt = require('jsonwebtoken');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const { readEnv } = require('./env');

const app = express();
app.use(express.json({ limit: '1mb' }));

let ACCESS_SECRET, ADMIN_SECRET_KEY, DATABASE_URL, PORT, LOG_LEVEL;
try {
  ACCESS_SECRET = readEnv('ACCESS_SECRET');
  ADMIN_SECRET_KEY = readEnv('ADMIN_SECRET_KEY', { required: false, allowEmpty: true }) || '';
  DATABASE_URL = readEnv('DATABASE_URL');
  PORT = Number(process.env.PORT || 3004);
  LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
} catch (e) {
  console.error('ERROR', e.message);
  process.exit(1);
}

const GAME_SERVICE_URL = (process.env.GAME_SERVICE_URL || 'http://game-service:3005').replace(/\/+$/, '');
const INSTANCE = process.env.INSTANCE_ID || require('os').hostname();

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
  res.on('finish', () => {
    log('debug', 'http', {
      method: req.method,
      path: req.originalUrl || req.url,
      code: res.statusCode,
      ms: Date.now() - t0,
    });
  });
  next();
});

const pool = new Pool({ connectionString: DATABASE_URL });
pool.on('error', (err) => log('error', 'pg.pool.error', { err: err?.message || String(err) }));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clampInt = (v, def, min, max) => {
  const n = Number.isFinite(Number(v)) ? Number(v) : def;
  return Math.max(min, Math.min(max, Math.trunc(n)));
};

const requireAdmin = (req, res, next) => {
  const key = req.headers['x-admin-key'];
  if (!ADMIN_SECRET_KEY) return res.status(500).json({ error: 'ADMIN_SECRET_KEY not configured' });
  if (!key || key !== ADMIN_SECRET_KEY) return res.status(403).json({ error: 'Forbidden' });
  next();
};

const authenticateToken = (req, res, next) => {
  const authHeader = req.headers.authorization;
  const token = authHeader && authHeader.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Token missing' });

  jwt.verify(token, ACCESS_SECRET, (err, payload) => {
    if (err) return res.status(401).json({ error: 'Access token expired or invalid' });
    req.user = payload;
    next();
  });
};

async function getUserBasic(userId) {
  const r = await pool.query('SELECT id, login, nickname FROM users WHERE id = $1 LIMIT 1', [userId]);
  return r.rows[0] || null;
}

function normalizeReward(reward) {
  if (reward === null || reward === undefined) reward = {};
  if (!reward || typeof reward !== 'object' || Array.isArray(reward)) {
    throw new Error('reward must be an object');
  }

  const money_mortals = Number(reward.money_mortals ?? 0);
  const money_cultivators = Number(reward.money_cultivators ?? 0);

  if (!Number.isFinite(money_mortals) || !Number.isInteger(money_mortals) || money_mortals < 0) {
    throw new Error('money_mortals must be integer >= 0');
  }
  if (!Number.isFinite(money_cultivators) || !Number.isInteger(money_cultivators) || money_cultivators < 0) {
    throw new Error('money_cultivators must be integer >= 0');
  }

  return { money_mortals, money_cultivators };
}

async function applyRewardViaGameService({ user_id, source_type, source_id, reward }) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 5000);

  let r;
  try {
    r = await fetch(`${GAME_SERVICE_URL}/internal/rewards/apply`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json; charset=utf-8',
        'x-admin-key': ADMIN_SECRET_KEY,
      },
      body: JSON.stringify({ user_id, source_type, source_id, reward }),
      signal: controller.signal,
    });
  } catch (err) {
    clearTimeout(timer);
    if (err?.name === 'AbortError') {
      const timeoutError = new Error('game-service request timed out');
      timeoutError.statusCode = 504;
      throw timeoutError;
    }
    throw err;
  }
  clearTimeout(timer);

  const text = await r.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch (_) {
    payload = null;
  }

  if (!r.ok) {
    const error = new Error(payload?.error || text || `game-service returned ${r.status}`);
    error.statusCode = r.status;
    throw error;
  }

  return payload || { ok: true };
}

async function createMail({ to_user_id, from_type, from_user_id, subject, body, reward_json }) {
  const id = uuidv4();
  await pool.query(
    `INSERT INTO mails (id, to_user_id, from_type, from_user_id, subject, body, reward_json, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, 'unread')`,
    [id, to_user_id, from_type, from_user_id ?? null, subject, body, JSON.stringify(reward_json)]
  );
  return id;
}

async function initDbForever() {
  while (true) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

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

      await client.query('COMMIT');
      log('info', 'db.ready');
      return;
    } catch (err) {
      try { await client.query('ROLLBACK'); } catch (_) {}
      log('error', 'db.init.failed', { err: err?.message || String(err) });
      await sleep(5000);
    } finally {
      client.release();
    }
  }
}

app.get('/ping', (req, res) => {
  res.json({ status: 'ok', message: 'pong', service: 'mail-service', server_time: new Date().toISOString() });
});

app.get('/mail/inbox', authenticateToken, async (req, res) => {
  const limit = clampInt(req.query.limit, 50, 1, 50);
  const before = (req.query.before_at || '').toString().trim();

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
    log('error', 'mail.inbox.failed', { err: e?.message || String(e) });
    res.status(500).json({ error: 'Database error' });
  }
});

app.get('/mail/:id', authenticateToken, async (req, res) => {
  const id = req.params.id;
  try {
    const r = await pool.query(
      `SELECT id, to_user_id, from_type, from_user_id, subject, body, reward_json, status, created_at
       FROM mails
       WHERE id = $1 AND to_user_id = $2
       LIMIT 1`,
      [id, req.user.uid]
    );
    if (r.rows.length === 0) return res.status(404).json({ error: 'Mail not found' });

    if (r.rows[0].status === 'unread') {
      await pool.query(
        `UPDATE mails SET status = 'read' WHERE id = $1 AND to_user_id = $2 AND status = 'unread'`,
        [id, req.user.uid]
      );
      r.rows[0].status = 'read';
    }

    res.json({ mail: r.rows[0] });
  } catch (e) {
    log('error', 'mail.get.failed', { err: e?.message || String(e) });
    res.status(500).json({ error: 'Database error' });
  }
});

app.post('/mail/:id/read', authenticateToken, async (req, res) => {
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
      if (chk.rows.length === 0) return res.status(404).json({ error: 'Mail not found' });
      return res.json({ mail: chk.rows[0] });
    }

    res.json({ mail: upd.rows[0] });
  } catch (e) {
    log('error', 'mail.read.failed', { err: e?.message || String(e) });
    res.status(500).json({ error: 'Database error' });
  }
});

app.post('/mail/:id/claim', authenticateToken, async (req, res) => {
  const id = req.params.id;
  try {
    const mailR = await pool.query(
      `SELECT id, reward_json, status
       FROM mails
       WHERE id = $1 AND to_user_id = $2
       LIMIT 1`,
      [id, req.user.uid]
    );

    if (mailR.rows.length === 0) {
      return res.status(404).json({ error: 'Mail not found' });
    }

    const mail = mailR.rows[0];
    if (mail.status === 'claimed') {
      return res.json({ status: 'already_claimed' });
    }

    let reward;
    try {
      reward = normalizeReward(mail.reward_json);
    } catch (e) {
      return res.status(400).json({ error: e.message });
    }

    const applied = await applyRewardViaGameService({
      user_id: req.user.uid,
      source_type: 'mail',
      source_id: id,
      reward,
    });

    await pool.query(
      `UPDATE mails
       SET status = 'claimed'
       WHERE id = $1 AND to_user_id = $2 AND status <> 'claimed'`,
      [id, req.user.uid]
    );

    res.json({
      status: 'claimed',
      applied: applied.applied || reward,
      idempotent: applied.already_applied === true,
    });
  } catch (e) {
    log('error', 'mail.claim.failed', { err: e?.message || String(e), statusCode: e?.statusCode || null });
    res.status(e?.statusCode === 400 ? 400 : 502).json({
      error: e?.statusCode === 400 ? e.message : 'game-service unavailable',
    });
  }
});

app.post('/admin/mail/send', requireAdmin, async (req, res) => {
  const to_user_id = Number(req.body?.to_user_id);
  const subject = (req.body?.subject || '').toString();
  const body = (req.body?.body || '').toString();

  if (!Number.isInteger(to_user_id) || to_user_id <= 0) return res.status(400).json({ error: 'to_user_id must be integer' });
  if (!subject.trim()) return res.status(400).json({ error: 'subject required' });
  if (!body.trim()) return res.status(400).json({ error: 'body required' });

  let reward_json;
  try {
    reward_json = normalizeReward(req.body?.reward_json);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  try {
    const u = await getUserBasic(to_user_id);
    if (!u) return res.status(404).json({ error: 'User not found' });

    const id = await createMail({
      to_user_id,
      from_type: 'admin',
      from_user_id: null,
      subject,
      body,
      reward_json,
    });

    res.status(201).json({ message: 'sent', mail_id: id });
  } catch (e) {
    log('error', 'admin.mail.send.failed', { err: e?.message || String(e) });
    res.status(500).json({ error: 'Database error' });
  }
});

app.post('/internal/mail/send', requireAdmin, async (req, res) => {
  const to_user_id = Number(req.body?.to_user_id);
  const from_type = (req.body?.from_type || 'system').toString();
  const from_user_id = req.body?.from_user_id === null || req.body?.from_user_id === undefined
    ? null
    : Number(req.body?.from_user_id);
  const subject = (req.body?.subject || '').toString();
  const body = (req.body?.body || '').toString();

  if (!Number.isInteger(to_user_id) || to_user_id <= 0) return res.status(400).json({ error: 'to_user_id must be integer' });
  if (!['system', 'admin', 'player'].includes(from_type)) return res.status(400).json({ error: 'bad from_type' });
  if (from_user_id !== null && (!Number.isInteger(from_user_id) || from_user_id <= 0)) {
    return res.status(400).json({ error: 'from_user_id must be positive integer or null' });
  }
  if (!subject.trim()) return res.status(400).json({ error: 'subject required' });
  if (!body.trim()) return res.status(400).json({ error: 'body required' });

  let reward_json;
  try {
    reward_json = normalizeReward(req.body?.reward_json);
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  try {
    const u = await getUserBasic(to_user_id);
    if (!u) return res.status(404).json({ error: 'User not found' });

    const id = await createMail({
      to_user_id,
      from_type,
      from_user_id,
      subject,
      body,
      reward_json,
    });

    res.status(201).json({ ok: true, mail_id: id });
  } catch (e) {
    log('error', 'internal.mail.send.failed', { err: e?.message || String(e) });
    res.status(500).json({ error: 'Database error' });
  }
});

async function main() {
  log('info', 'boot', { instance: INSTANCE, port: PORT, log_level: LOG_LEVEL, game_service_url: GAME_SERVICE_URL });
  await initDbForever();
  app.listen(PORT, () => log('info', 'http.listening', { port: PORT, instance: INSTANCE }));
}

main().catch((e) => {
  log('error', 'fatal', { err: e?.message || String(e) });
  process.exit(1);
});
