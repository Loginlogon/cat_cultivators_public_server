'use strict';

const express = require('express');
const { Pool } = require('pg');
const { readEnv } = require('./env');

const app = express();
app.use(express.json({ limit: '1mb' }));

let ADMIN_SECRET_KEY, DATABASE_URL, PORT, LOG_LEVEL;
try {
  ADMIN_SECRET_KEY = readEnv('ADMIN_SECRET_KEY');
  DATABASE_URL = readEnv('DATABASE_URL');
  PORT = Number(process.env.PORT || 3005);
  LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
} catch (e) {
  console.error('ERROR', e.message);
  process.exit(1);
}

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

const requireAdmin = (req, res, next) => {
  const key = req.headers['x-admin-key'];
  if (!key || key !== ADMIN_SECRET_KEY) return res.status(403).json({ error: 'Forbidden' });
  next();
};

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

function hasRewardPayload(reward) {
  return reward.money_mortals > 0 || reward.money_cultivators > 0;
}

async function findExistingGrant(client, source_type, source_id) {
  const existing = await client.query(
    `SELECT id, user_id, reward_json, applied_at
     FROM reward_grants
     WHERE source_type = $1 AND source_id = $2
     LIMIT 1`,
    [source_type, source_id]
  );
  return existing.rows[0] || null;
}

async function recordRewardGrant(client, { user_id, source_type, source_id, reward }) {
  const grant = await client.query(
    `INSERT INTO reward_grants (user_id, source_type, source_id, reward_json)
     VALUES ($1, $2, $3, $4::jsonb)
     ON CONFLICT (source_type, source_id) DO NOTHING
     RETURNING id, applied_at`,
    [user_id, source_type, source_id, JSON.stringify(reward)]
  );
  return grant.rows[0] || null;
}

async function applyProfileReward(client, user_id, reward) {
  if (!hasRewardPayload(reward)) return;

  await client.query(
    `UPDATE profiles
     SET money_mortals = money_mortals + $1,
         money_cultivators = money_cultivators + $2
     WHERE user_id = $3`,
    [reward.money_mortals, reward.money_cultivators, user_id]
  );
}

async function applyRewardByDomain(client, { user_id, reward }) {
  // For now rewards only touch profile economy fields.
  // This dispatcher is the future extension point for inventory, progress and trade.
  await applyProfileReward(client, user_id, reward);
}

async function initDbForever() {
  while (true) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      await client.query(`
        CREATE TABLE IF NOT EXISTS reward_grants (
          id BIGSERIAL PRIMARY KEY,
          user_id INTEGER NOT NULL,
          source_type TEXT NOT NULL,
          source_id TEXT NOT NULL,
          reward_json JSONB NOT NULL,
          applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      `);
      await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_reward_grants_source ON reward_grants(source_type, source_id);`);
      await client.query(`CREATE INDEX IF NOT EXISTS idx_reward_grants_user_applied ON reward_grants(user_id, applied_at DESC);`);

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
  res.json({ status: 'ok', message: 'pong', service: 'game-service', server_time: new Date().toISOString() });
});

app.post('/internal/rewards/apply', requireAdmin, async (req, res) => {
  const user_id = Number(req.body?.user_id);
  const source_type = (req.body?.source_type || '').toString().trim();
  const source_id = (req.body?.source_id || '').toString().trim();

  if (!Number.isInteger(user_id) || user_id <= 0) return res.status(400).json({ error: 'user_id must be positive integer' });
  if (!source_type) return res.status(400).json({ error: 'source_type required' });
  if (!source_id) return res.status(400).json({ error: 'source_id required' });

  let reward;
  try {
    reward = normalizeReward(req.body?.reward ?? {});
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const profile = await client.query(
      'SELECT user_id FROM profiles WHERE user_id = $1 FOR UPDATE',
      [user_id]
    );
    if (!profile.rows.length) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Profile not found' });
    }

    const grant = await recordRewardGrant(client, { user_id, source_type, source_id, reward });

    if (!grant) {
      const existing = await findExistingGrant(client, source_type, source_id);
      await client.query('COMMIT');
      return res.json({
        ok: true,
        already_applied: true,
        applied: existing?.reward_json || reward,
        grant_id: existing?.id || null,
        applied_at: existing?.applied_at || null,
      });
    }

    await applyRewardByDomain(client, { user_id, reward });

    await client.query('COMMIT');
    res.json({
      ok: true,
      already_applied: false,
      applied: reward,
      grant_id: grant.id,
      applied_at: grant.applied_at,
    });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    log('error', 'reward.apply.failed', { err: e?.message || String(e), user_id, source_type, source_id });
    res.status(500).json({ error: 'Database error' });
  } finally {
    client.release();
  }
});

async function main() {
  log('info', 'boot', { instance: INSTANCE, port: PORT, log_level: LOG_LEVEL });
  await initDbForever();
  app.listen(PORT, () => log('info', 'http.listening', { port: PORT, instance: INSTANCE }));
}

main().catch((e) => {
  log('error', 'fatal', { err: e?.message || String(e) });
  process.exit(1);
});
