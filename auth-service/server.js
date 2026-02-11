const express = require('express');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

// --- CONFIGURATION FROM ENV ---
const ACCESS_SECRET = process.env.ACCESS_SECRET;
const REFRESH_SECRET = process.env.REFRESH_SECRET;
const PORT = process.env.PORT || 3000;

if (!ACCESS_SECRET || !REFRESH_SECRET || !process.env.DATABASE_URL) {
  console.error("❌ Missing required env vars: ACCESS_SECRET, REFRESH_SECRET, DATABASE_URL");
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// --- DATABASE INITIALIZATION (FRESH) ---
const initDb = async () => {
  try {
    // users: login уникальный, nickname НЕ уникальный
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        login TEXT UNIQUE NOT NULL,
        nickname TEXT NOT NULL,
        password TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // profiles: храним nickname ещё раз + игровые поля
    await pool.query(`
      CREATE TABLE IF NOT EXISTS profiles (
        user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        nickname TEXT NOT NULL,
        level_cultivation INTEGER DEFAULT 0,
        level_body INTEGER DEFAULT 0,
        money_mortals BIGINT DEFAULT 20,
        money_cultivators BIGINT DEFAULT 1
      );
    `);

    console.log("✅ Database tables initialized");
  } catch (err) {
    console.log("❌ DB Error:", err?.message || err, "Retrying in 5s...");
    setTimeout(initDb, 5000);
  }
};
initDb();

// --- MIDDLEWARE ---
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) return res.status(401).json({ error: "Token missing" });

  jwt.verify(token, ACCESS_SECRET, (err, payload) => {
    if (err) return res.status(403).json({ error: "Access token expired or invalid" });
    req.user = payload; // { uid, login }
    next();
  });
};

// --- ROUTES ---

// 0. Ping (Health Check)
app.get('/ping', (req, res) => {
  res.json({
    status: "ok",
    message: "pong",
    server_time: new Date().toISOString()
  });
});

// Проверка уникальности ЛОГИНА
// body: { "login": "cat123" } -> { exists: true/false, available: true/false }
app.post('/user/check-login', async (req, res) => {
  const { login } = req.body;

  if (!login || typeof login !== 'string' || !login.trim()) {
    return res.status(400).json({ error: "Login required" });
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
    return res.status(500).json({ error: "Database error" });
  }
});

// 1. Registration (Atomic): login (unique) + nickname + password
app.post('/register', async (req, res) => {
  const { login, nickname, password } = req.body;

  if (!login || typeof login !== 'string' || !login.trim()) {
    return res.status(400).json({ error: "Login required" });
  }
  if (!nickname || typeof nickname !== 'string' || !nickname.trim()) {
    return res.status(400).json({ error: "Nickname required" });
  }
  if (!password || typeof password !== 'string' || password.length < 4) {
    return res.status(400).json({ error: "Password too short" });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const hashedPassword = await bcrypt.hash(password, 10);

    // пишем nickname в users
    const userResult = await client.query(
      'INSERT INTO users (login, nickname, password) VALUES ($1, $2, $3) RETURNING id, login, nickname',
      [login.trim(), nickname.trim(), hashedPassword]
    );

    const userId = userResult.rows[0].id;

    // и дублируем nickname в profiles
    await client.query(
      'INSERT INTO profiles (user_id, nickname) VALUES ($1, $2)',
      [userId, nickname.trim()]
    );

    await client.query('COMMIT');
    return res.status(201).json({ message: "Success" });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}

    // 23505 = unique_violation (логин занят)
    if (err && err.code === '23505') {
      return res.status(409).json({ error: "Login already taken" });
    }

    return res.status(400).json({ error: "Registration failed" });
  } finally {
    client.release();
  }
});

// 2. Login: login + password
app.post('/login', async (req, res) => {
  const { login, password } = req.body;

  if (!login || typeof login !== 'string' || !login.trim()) {
    return res.status(400).json({ error: "Login required" });
  }
  if (!password || typeof password !== 'string') {
    return res.status(400).json({ error: "Password required" });
  }

  try {
    const result = await pool.query(
      'SELECT id, login, nickname, password FROM users WHERE login = $1',
      [login.trim()]
    );
    if (result.rows.length === 0) return res.status(401).json({ error: "User not found" });

    const user = result.rows[0];
    const isValid = await bcrypt.compare(password, user.password);
    if (!isValid) return res.status(401).json({ error: "Wrong password" });

    const payload = { uid: user.id, login: user.login };

    const accessToken = jwt.sign(payload, ACCESS_SECRET, { expiresIn: '1d' });
    const refreshToken = jwt.sign(payload, REFRESH_SECRET, { expiresIn: '14d' });

    return res.json({
      access_token: accessToken,
      refresh_token: refreshToken,
      // часто удобно отдать ник сразу после логина
      nickname: user.nickname
    });
  } catch (err) {
    return res.status(500).json({ error: "Internal error" });
  }
});

// 3. Get Registration Date by LOGIN
app.get('/user/reg-date/:login', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT created_at FROM users WHERE login = $1',
      [req.params.login]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: "User not found" });

    return res.json({
      login: req.params.login,
      registration_date: result.rows[0].created_at
    });
  } catch (err) {
    return res.status(500).json({ error: "Database error" });
  }
});

// 4. Refresh Token
app.post('/refresh', (req, res) => {
  const { refresh_token } = req.body;
  if (!refresh_token) return res.status(401).json({ error: "Token required" });

  jwt.verify(refresh_token, REFRESH_SECRET, (err, payload) => {
    if (err) return res.status(403).json({ error: "Expired refresh token" });

    const newAccessToken = jwt.sign(
      { uid: payload.uid, login: payload.login },
      ACCESS_SECRET,
      { expiresIn: '1d' }
    );

    return res.json({ access_token: newAccessToken });
  });
});

// 5. Game Stats (возвращаем профиль + nickname из обеих таблиц, чтобы видеть, что совпадает)
app.get('/cat-stats', authenticateToken, async (req, res) => {
  try {
const result = await pool.query(
  `SELECT nickname, level_cultivation, level_body, money_mortals, money_cultivators
   FROM profiles
   WHERE user_id = $1
   LIMIT 1`,
  [req.user.uid]
);


    if (result.rows.length === 0) return res.status(404).json({ error: "Profile not found" });

    return res.json({ data: result.rows[0] });
  } catch (err) {
    return res.status(500).json({ error: "Stats error" });
  }
});

app.listen(PORT, () => console.log(`🚀 Server on port ${PORT}`));
