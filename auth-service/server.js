'use strict';

const express = require('express');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json({ limit: '1mb' }));

function normalizeSecret(v) {
  const s = String(v || '');
  return s.charCodeAt(0) === 0xfeff ? s.slice(1) : s;
}

// --- CONFIGURATION FROM ENV ---
const ACCESS_SECRET = normalizeSecret(process.env.ACCESS_SECRET);
const REFRESH_SECRET = normalizeSecret(process.env.REFRESH_SECRET);
const PORT = process.env.PORT || 3000;
const MAIL_SERVICE_URL = String(process.env.MAIL_SERVICE_URL || 'http://mail-service:3004').replace(/\/+$/, '');

// optional admin key (для сидов/добавления новых названий аватарок, если захочешь)
const ADMIN_SECRET_KEY = normalizeSecret(process.env.ADMIN_SECRET_KEY);

if (!ACCESS_SECRET || !REFRESH_SECRET || !process.env.DATABASE_URL) {
  console.error('❌ Missing required env vars: ACCESS_SECRET, REFRESH_SECRET, DATABASE_URL');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// --- ACTUAL GAME FILES ---
const ACTUAL_GAME_DIR = path.join(__dirname, 'actual-game');
const VERSION_FILE = path.join(ACTUAL_GAME_DIR, 'version.json');
const APK_FILE = path.join(ACTUAL_GAME_DIR, 'game.apk');

// --- DATABASE INITIALIZATION (FRESH) ---
const initDb = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        login TEXT UNIQUE NOT NULL,
        nickname TEXT NOT NULL,
        password TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // ✅ деньги по умолчанию теперь 0/0
    await pool.query(`
      CREATE TABLE IF NOT EXISTS profiles (
        user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        nickname TEXT NOT NULL,
        level_cultivation INTEGER DEFAULT 0,
        level_body INTEGER DEFAULT 0,
        money_mortals BIGINT DEFAULT 0,
        money_cultivators BIGINT DEFAULT 0
      );
    `);

    // ✅ если таблица уже была создана раньше с дефолтом 20/1 — исправляем дефолты “мягкой миграцией”
    await pool.query(`ALTER TABLE profiles ALTER COLUMN money_mortals SET DEFAULT 0;`);
    await pool.query(`ALTER TABLE profiles ALTER COLUMN money_cultivators SET DEFAULT 0;`);

    // --- AVATAR NAMES CATALOG (server stores only names/codes) ---
    await pool.query(`
      CREATE TABLE IF NOT EXISTS avatar_names (
        code TEXT PRIMARY KEY,
        title TEXT NOT NULL
      );
    `);

    // --- USER UNLOCKED AVATARS ---
    await pool.query(`
      CREATE TABLE IF NOT EXISTS user_avatars (
        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        avatar_code TEXT NOT NULL REFERENCES avatar_names(code) ON DELETE CASCADE,
        unlocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, avatar_code)
      );
    `);

    // --- current avatar in profile ---
    await pool.query(`ALTER TABLE profiles ADD COLUMN IF NOT EXISTS current_avatar TEXT;`);

    // seed default avatar name if missing
    await pool.query(`
      INSERT INTO avatar_names (code, title)
      VALUES ('default', 'Стандарт')
      ON CONFLICT (code) DO NOTHING;
    `);

    // ✅ ВАЖНО: таблица mails

    await pool.query(`
      CREATE TABLE IF NOT EXISTS pending_registration_mails (
        user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        nickname TEXT NOT NULL,
        subject TEXT NOT NULL,
        body TEXT NOT NULL,
        reward_json JSONB NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        next_attempt_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_pending_registration_mails_retry
      ON pending_registration_mails(next_attempt_at, created_at);
    `);

    console.log('✅ Database tables initialized');
  } catch (err) {
    console.log('❌ DB Error:', err?.message || err, 'Retrying in 5s...');
    setTimeout(initDb, 5000);
  }
};
initDb();

// --- MIDDLEWARE ---
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) return res.status(401).json({ error: 'Token missing' });

  jwt.verify(token, ACCESS_SECRET, (err, payload) => {
    if (err) return res.status(403).json({ error: 'Access token expired or invalid' });
    req.user = payload; // { uid, login }
    next();
  });
};

const requireAdmin = (req, res, next) => {
  const key = req.headers['x-admin-key'];
  if (!ADMIN_SECRET_KEY) return res.status(500).json({ error: 'ADMIN_SECRET_KEY not configured' });
  if (!key || key !== ADMIN_SECRET_KEY) return res.status(403).json({ error: 'Forbidden' });
  next();
};

// --- HELPERS ---
function safeReadVersion() {
  try {
    const raw = fs.readFileSync(VERSION_FILE, 'utf8');
    const obj = JSON.parse(raw);
    const version_code = Number(obj?.version_code);
    const version_name = String(obj?.version_name ?? '');

    return {
      version_code: Number.isInteger(version_code) && version_code > 0 ? version_code : 1,
      version_name: version_name.trim() || '1.0.0'
    };
  } catch (_) {
    return { version_code: 1, version_name: '1.0.0' };
  }
}

function buildBaseUrl(req) {
  const proto = (req.headers['x-forwarded-proto'] || req.protocol || 'http').toString();
  const host = (req.headers['x-forwarded-host'] || req.headers.host || '').toString();
  return `${proto}://${host}`;
}

async function ensureAvatarExists(code) {
  const r = await pool.query(`SELECT code FROM avatar_names WHERE code = $1 LIMIT 1`, [code]);
  return r.rows.length > 0;
}

async function isAvatarUnlocked(uid, code) {
  if (code === 'default') return true;

  const r = await pool.query(
    `SELECT 1 FROM user_avatars WHERE user_id = $1 AND avatar_code = $2 LIMIT 1`,
    [uid, code]
  );
  return r.rows.length > 0;
}

function titleFromCode(code) {
  // default_avatar_12 -> "Аватар №12"
  const m = String(code || '').match(/(\d+)$/);
  if (m && m[1]) return `Аватар №${m[1]}`;
  return String(code || 'Аватар');
}

async function upsertAvatarNameIfMissing(clientOrPool, code) {
  // создаём запись в каталоге, если её нет (чтобы /avatars/add работал "из приложения")
  const title = titleFromCode(code);
  await clientOrPool.query(
    `INSERT INTO avatar_names (code, title)
     VALUES ($1, $2)
     ON CONFLICT (code) DO NOTHING`,
    [code, title]
  );
}

function makeInheritanceMail(nickname) {
  const subject = 'Наследство';
  const body =
`Здравствуйте, ${nickname}.

С прискорбием сообщаем: после долгой болезни ваши родители скончались.
Перед смертью они оставили вам наследство.

Наследство:
• 20 монет смертных
• 1 монета культиваторов

Откройте письмо и нажмите «Получить», чтобы забрать наследство.

— Канцелярия`;

  const reward = { money_mortals: 20, money_cultivators: 1 };
  return { subject, body, reward };
}

async function markRegistrationMailRetry(client, userId, errorText, delayMinutes = 5) {
  await client.query(
    `UPDATE pending_registration_mails
     SET attempts = attempts + 1,
         last_error = $2,
         next_attempt_at = CURRENT_TIMESTAMP + ($3::text || ' minutes')::interval,
         updated_at = CURRENT_TIMESTAMP
     WHERE user_id = $1`,
    [userId, String(errorText || 'unknown error').slice(0, 2000), String(delayMinutes)]
  );
}

async function flushPendingRegistrationMail(userId) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const pending = await client.query(
      `SELECT user_id, subject, body, reward_json
       FROM pending_registration_mails
       WHERE user_id = $1
       FOR UPDATE`,
      [userId]
    );

    if (!pending.rows.length) {
      await client.query('COMMIT');
      return { ok: true, pending: false, delivered: false };
    }

    const row = pending.rows[0];

    try {
      const response = await fetch(`${MAIL_SERVICE_URL}/internal/mail/send`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json; charset=utf-8',
          'x-admin-key': ADMIN_SECRET_KEY,
        },
        body: JSON.stringify({
          to_user_id: row.user_id,
          from_type: 'system',
          subject: row.subject,
          body: row.body,
          reward_json: row.reward_json,
        }),
      });

      if (!response.ok) {
        const text = await response.text().catch(() => '');
        throw new Error(`mail-service returned ${response.status}: ${text}`.trim());
      }

      await client.query(`DELETE FROM pending_registration_mails WHERE user_id = $1`, [userId]);
      await client.query('COMMIT');
      return { ok: true, pending: false, delivered: true };
    } catch (err) {
      await markRegistrationMailRetry(client, userId, err?.message || err);
      await client.query('COMMIT');
      return { ok: false, pending: true, delivered: false, error: err?.message || String(err) };
    }
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
}

async function flushPendingRegistrationMailsBatch(limit = 100) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const due = await client.query(
      `SELECT user_id
       FROM pending_registration_mails
       WHERE next_attempt_at <= CURRENT_TIMESTAMP
       ORDER BY created_at ASC
       LIMIT $1
       FOR UPDATE SKIP LOCKED`,
      [limit]
    );
    await client.query('COMMIT');

    let delivered = 0;
    let failed = 0;

    for (const row of due.rows) {
      const result = await flushPendingRegistrationMail(row.user_id);
      if (result.delivered) delivered++;
      else failed++;
    }

    return { scanned: due.rows.length, delivered, failed };
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    throw err;
  } finally {
    client.release();
  }
}

// --- ROUTES ---
// 0. Ping
app.get('/ping', (req, res) => {
  res.json({
    status: 'ok',
    message: 'pong',
    server_time: new Date().toISOString()
  });
});

// -------------------- VERSION / UPDATE --------------------
app.post('/app/check-version', (req, res) => {
  const client_code = Number(req.body?.version_code);
  const client_name = (req.body?.version_name || '').toString();

  if (!Number.isInteger(client_code) || client_code <= 0) {
    return res.status(400).json({ error: 'version_code required (int > 0)' });
  }

  const v = safeReadVersion();
  const update_available = client_code < v.version_code;

  const base = buildBaseUrl(req);
  const download_url = `${base}/app/download-apk`;

  return res.json({
    your_version_code: client_code,
    your_version_name: client_name || null,
    latest_version_code: v.version_code,
    latest_version_name: v.version_name,
    update_available,
    force: false,
    download_url
  });
});

app.get('/app/latest', (req, res) => {
  const v = safeReadVersion();
  const base = buildBaseUrl(req);
  res.json({
    latest_version_code: v.version_code,
    latest_version_name: v.version_name,
    download_url: `${base}/app/download-apk`
  });
});

app.get('/app/download-apk', (req, res) => {
  if (!fs.existsSync(APK_FILE)) {
    return res.status(404).json({ error: 'APK not found on server' });
  }
  const v = safeReadVersion();
  const niceName = `game-${v.version_name}-(${v.version_code}).apk`;
  return res.download(APK_FILE, niceName);
});

// -------------------- AUTH / USERS --------------------
app.post('/user/check-login', async (req, res) => {
  const { login } = req.body;

  if (!login || typeof login !== 'string' || !login.trim()) {
    return res.status(400).json({ error: 'Login required' });
  }

  try {
    const result = await pool.query(
      'SELECT 1 FROM users WHERE login = $1 LIMIT 1',
      [login.trim()]
    );

    const exists = result.rows.length > 0;
    return res.json({
      login: login.trim(),
      exists,
      available: !exists
    });
  } catch (err) {
    return res.status(500).json({ error: 'Database error' });
  }
});

app.post('/register', async (req, res) => {
  const { login, nickname, password } = req.body;

  if (!login || typeof login !== 'string' || !login.trim()) {
    return res.status(400).json({ error: 'Login required' });
  }
  if (!nickname || typeof nickname !== 'string' || !nickname.trim()) {
    return res.status(400).json({ error: 'Nickname required' });
  }
  if (!password || typeof password !== 'string' || password.length < 4) {
    return res.status(400).json({ error: 'Password too short' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const hashedPassword = await bcrypt.hash(password, 10);

    const userResult = await client.query(
      'INSERT INTO users (login, nickname, password) VALUES ($1, $2, $3) RETURNING id, login, nickname',
      [login.trim(), nickname.trim(), hashedPassword]
    );

    const userId = userResult.rows[0].id;
    const nick = userResult.rows[0].nickname;

    await client.query(
      `INSERT INTO profiles (user_id, nickname, current_avatar, money_mortals, money_cultivators)
       VALUES ($1, $2, $3, $4, $5)`,
      [userId, nick, 'default', 0, 0]
    );

    const mail = makeInheritanceMail(nick);
    await client.query(
      `INSERT INTO pending_registration_mails (user_id, nickname, subject, body, reward_json)
       VALUES ($1, $2, $3, $4, $5::jsonb)`,
      [userId, nick, mail.subject, mail.body, JSON.stringify(mail.reward)]
    );

    await client.query('COMMIT');
    const mailDelivery = await flushPendingRegistrationMail(userId);
    return res.status(201).json({
      message: 'Success',
      registration_mail: mailDelivery.delivered ? 'sent' : 'queued'
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}

    if (err && err.code === '23505') {
      return res.status(409).json({ error: 'Login already taken' });
    }

    return res.status(400).json({ error: 'Registration failed' });
  } finally {
    client.release();
  }
});

app.post('/internal/maintenance/registration-mails/retry', requireAdmin, async (req, res) => {
  const rawLimit = Number(req.body?.limit);
  const limit = Number.isInteger(rawLimit) ? Math.max(1, Math.min(500, rawLimit)) : 100;

  try {
    const result = await flushPendingRegistrationMailsBatch(limit);
    return res.json({ ok: true, ...result });
  } catch (_) {
    return res.status(500).json({ error: 'Retry failed' });
  }
});

app.post('/login', async (req, res) => {
  const { login, password } = req.body;

  if (!login || typeof login !== 'string' || !login.trim()) {
    return res.status(400).json({ error: 'Login required' });
  }
  if (!password || typeof password !== 'string') {
    return res.status(400).json({ error: 'Password required' });
  }

  try {
    const result = await pool.query(
      'SELECT id, login, nickname, password FROM users WHERE login = $1',
      [login.trim()]
    );
    if (result.rows.length === 0) return res.status(401).json({ error: 'User not found' });

    const user = result.rows[0];
    const isValid = await bcrypt.compare(password, user.password);
    if (!isValid) return res.status(401).json({ error: 'Wrong password' });

    const payload = { uid: user.id, login: user.login };

    const accessToken = jwt.sign(payload, ACCESS_SECRET, { expiresIn: '1d' });
    const refreshToken = jwt.sign(payload, REFRESH_SECRET, { expiresIn: '14d' });

    return res.json({
      access_token: accessToken,
      refresh_token: refreshToken,
      nickname: user.nickname
    });
  } catch (err) {
    return res.status(500).json({ error: 'Internal error' });
  }
});

app.get('/user/reg-date/:login', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT created_at FROM users WHERE login = $1',
      [req.params.login]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'User not found' });

    return res.json({
      login: req.params.login,
      registration_date: result.rows[0].created_at
    });
  } catch (err) {
    return res.status(500).json({ error: 'Database error' });
  }
});

app.post('/refresh', (req, res) => {
  const { refresh_token } = req.body;
  if (!refresh_token) return res.status(401).json({ error: 'Token required' });

  jwt.verify(refresh_token, REFRESH_SECRET, (err, payload) => {
    if (err) return res.status(403).json({ error: 'Expired refresh token' });

    const newAccessToken = jwt.sign(
      { uid: payload.uid, login: payload.login },
      ACCESS_SECRET,
      { expiresIn: '1d' }
    );

    return res.json({ access_token: newAccessToken });
  });
});

app.get('/cat-stats', authenticateToken, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT nickname, level_cultivation, level_body, money_mortals, money_cultivators, current_avatar
       FROM profiles
       WHERE user_id = $1
       LIMIT 1`,
      [req.user.uid]
    );

    if (result.rows.length === 0) return res.status(404).json({ error: 'Profile not found' });
    return res.json({ data: result.rows[0] });
  } catch (err) {
    return res.status(500).json({ error: 'Stats error' });
  }
});

// -------------------- AVATARS --------------------

// 1) Получить аву конкретного игрока по user_id
app.get('/users/:userId/avatar', authenticateToken, async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ error: 'bad userId' });

  try {
    const r = await pool.query(
      `SELECT user_id, current_avatar
       FROM profiles
       WHERE user_id = $1
       LIMIT 1`,
      [userId]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'Profile not found' });

    res.json({ user_id: r.rows[0].user_id, avatar: r.rows[0].current_avatar || 'default' });
  } catch (_) {
    res.status(500).json({ error: 'Database error' });
  }
});

// 2) Получить список “скинов/аватарок” игрока (его unlocks + default)
app.get('/avatars/my', authenticateToken, async (req, res) => {
  try {
    const prof = await pool.query(
      `SELECT current_avatar FROM profiles WHERE user_id = $1 LIMIT 1`,
      [req.user.uid]
    );
    if (!prof.rows.length) return res.status(404).json({ error: 'Profile not found' });

    const current = prof.rows[0].current_avatar || 'default';

    const r = await pool.query(
      `SELECT a.code, a.title
       FROM avatar_names a
       WHERE a.code = 'default'
          OR EXISTS (
            SELECT 1 FROM user_avatars ua
            WHERE ua.user_id = $1 AND ua.avatar_code = a.code
          )
       ORDER BY a.code ASC`,
      [req.user.uid]
    );

    res.json({
      current_avatar: current,
      avatars: r.rows.map(x => ({ code: x.code, title: x.title }))
    });
  } catch (_) {
    res.status(500).json({ error: 'Database error' });
  }
});

// 3) add avatar (unlock) — игроку добавить аватарку по строке
// body: { "avatar": "default_avatar_1" }
app.post('/avatars/add', requireAdmin, async (req, res) => {
  const targetUserId = Number(req.body?.user_id);
  const code = (req.body?.avatar || '').toString().trim();
  if (!Number.isInteger(targetUserId) || targetUserId <= 0) return res.status(400).json({ error: 'user_id required' });
  if (!code) return res.status(400).json({ error: 'avatar required' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // ✅ ВАЖНО: если кода нет в каталоге — создаём его автоматически
    await upsertAvatarNameIfMissing(client, code);

    if (code === 'default') {
      await client.query('COMMIT');
      return res.json({ ok: true, avatar: 'default', already: true });
    }

    await client.query(
      `INSERT INTO user_avatars (user_id, avatar_code)
       VALUES ($1, $2)
       ON CONFLICT (user_id, avatar_code) DO NOTHING`,
      [targetUserId, code]
    );

    await client.query('COMMIT');
    res.json({ ok: true, user_id: targetUserId, avatar: code });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    res.status(500).json({ error: 'Database error' });
  } finally {
    client.release();
  }
});

// 4) select avatar
app.post('/avatars/select', authenticateToken, async (req, res) => {
  const code = (req.body?.avatar || '').toString().trim();
  if (!code) return res.status(400).json({ error: 'avatar required' });

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const prof = await client.query(
      `SELECT user_id FROM profiles WHERE user_id = $1 FOR UPDATE`,
      [req.user.uid]
    );
    if (!prof.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Profile not found' });
    }

    const exists = await client.query(`SELECT 1 FROM avatar_names WHERE code = $1 LIMIT 1`, [code]);
    if (!exists.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Avatar code not found in catalog' });
    }

    const unlocked = await isAvatarUnlocked(req.user.uid, code);
    if (!unlocked) {
      await client.query('ROLLBACK');
      return res.status(403).json({ error: 'Avatar is locked' });
    }

    await client.query(
      `UPDATE profiles SET current_avatar = $1 WHERE user_id = $2`,
      [code, req.user.uid]
    );

    await client.query('COMMIT');
    res.json({ ok: true, current_avatar: code });
  } catch (_) {
    try { await client.query('ROLLBACK'); } catch (__) {}
    res.status(500).json({ error: 'Database error' });
  } finally {
    client.release();
  }
});

// --- OPTIONAL ADMIN ---
app.post('/admin/avatars/upsert', requireAdmin, async (req, res) => {
  const code = (req.body?.code || '').toString().trim();
  const title = (req.body?.title || '').toString().trim();
  if (!code) return res.status(400).json({ error: 'code required' });
  if (!title) return res.status(400).json({ error: 'title required' });

  try {
    const r = await pool.query(
      `INSERT INTO avatar_names (code, title)
       VALUES ($1, $2)
       ON CONFLICT (code) DO UPDATE SET title = EXCLUDED.title
       RETURNING code, title`,
      [code, title]
    );
    res.json({ ok: true, avatar: r.rows[0] });
  } catch (_) {
    res.status(500).json({ error: 'Database error' });
  }
});

console.log(
  (app._router?.stack || [])
    .filter((l) => l.route)
    .map((l) => `${Object.keys(l.route.methods).join(',').toUpperCase()} ${l.route.path}`)
);

app.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));
