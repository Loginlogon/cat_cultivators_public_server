'use strict';

const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const path = require('node:path');

const rootDir = path.resolve(__dirname, '..');

function getConfig() {
  const cfg = {
    authUrl: process.env.SMOKE_AUTH_URL || 'http://localhost:3000',
    chatUrl: process.env.SMOKE_CHAT_URL || 'http://localhost:3001',
    mailUrl: process.env.SMOKE_MAIL_URL || 'http://localhost:3004',
    gameUrl: process.env.SMOKE_GAME_URL || 'http://localhost:3005',
    adminKey: process.env.SMOKE_ADMIN_KEY || '',
  };

  assert.ok(cfg.adminKey, 'SMOKE_ADMIN_KEY is required');
  return cfg;
}

function makeSuffix() {
  return randomUUID().slice(0, 8);
}

function makeUser(prefix) {
  const suffix = makeSuffix();
  return {
    login: `${prefix}_${suffix}`,
    nickname: `${prefix}_${suffix}`,
    password: '1234',
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function http(method, url, { token, adminKey, body, expected = [200] } = {}) {
  const headers = { accept: 'application/json' };
  if (token) headers.authorization = `Bearer ${token}`;
  if (adminKey) headers['x-admin-key'] = adminKey;
  if (body !== undefined) headers['content-type'] = 'application/json; charset=utf-8';

  const res = await fetch(url, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {
    json = null;
  }

  assert.ok(
    expected.includes(res.status),
    `${method} ${url} expected ${expected.join('/')} got ${res.status}: ${text}`
  );

  return { status: res.status, json, text };
}

async function poll(fn, { timeoutMs = 10000, intervalMs = 500, label = 'poll' } = {}) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const result = await fn();
    if (result) return result;
    await sleep(intervalMs);
  }
  throw new Error(`Timeout while waiting for ${label}`);
}

async function registerUser(cfg, user) {
  return http('POST', `${cfg.authUrl}/register`, {
    body: user,
    expected: [201],
  });
}

async function loginUser(cfg, user) {
  const res = await http('POST', `${cfg.authUrl}/login`, {
    body: { login: user.login, password: user.password },
    expected: [200],
  });
  assert.ok(res.json?.access_token, 'access_token missing');
  return res.json.access_token;
}

async function getCatStats(cfg, token) {
  const res = await http('GET', `${cfg.authUrl}/cat-stats`, {
    token,
    expected: [200],
  });
  assert.ok(res.json?.data, 'cat stats missing');
  return res.json.data;
}

async function triggerRegistrationMailRetry(cfg, limit = 50) {
  return http('POST', `${cfg.authUrl}/internal/maintenance/registration-mails/retry`, {
    adminKey: cfg.adminKey,
    body: { limit },
    expected: [200],
  });
}

async function getInbox(cfg, token) {
  const res = await http('GET', `${cfg.mailUrl}/mail/inbox`, {
    token,
    expected: [200],
  });
  return res.json?.mails || [];
}

async function ensureRegistrationMailDelivered(cfg, token) {
  let mails = await getInbox(cfg, token);
  if (mails.length > 0) return mails;

  await triggerRegistrationMailRetry(cfg);

  mails = await poll(
    async () => {
      const next = await getInbox(cfg, token);
      return next.length > 0 ? next : null;
    },
    { timeoutMs: 15000, intervalMs: 700, label: 'registration mail delivery' }
  );

  return mails;
}

async function getMail(cfg, token, mailId) {
  const res = await http('GET', `${cfg.mailUrl}/mail/${mailId}`, {
    token,
    expected: [200],
  });
  assert.ok(res.json?.mail, 'mail payload missing');
  return res.json.mail;
}

async function claimMail(cfg, token, mailId, expected = [200]) {
  return http('POST', `${cfg.mailUrl}/mail/${mailId}/claim`, {
    token,
    expected,
  });
}

async function adminSendMail(cfg, body) {
  return http('POST', `${cfg.mailUrl}/admin/mail/send`, {
    adminKey: cfg.adminKey,
    body,
    expected: [201],
  });
}

async function searchChatUsers(cfg, token, nickname, limit = 20) {
  const res = await http(
    'GET',
    `${cfg.chatUrl}/users/search?nickname=${encodeURIComponent(nickname)}&limit=${limit}`,
    { token, expected: [200] }
  );
  return res.json?.users || [];
}

async function findChatUser(cfg, token, nickname) {
  const users = await searchChatUsers(cfg, token, nickname);
  const found = users.find((u) => u.nickname === nickname);
  assert.ok(found?.id, `chat user not found for nickname ${nickname}`);
  return found;
}

async function registerAndLogin(cfg, prefix) {
  const user = makeUser(prefix);
  const reg = await registerUser(cfg, user);
  const token = await loginUser(cfg, user);
  return { user, token, reg: reg.json };
}

async function registerPair(cfg, prefixA, prefixB) {
  const a = await registerAndLogin(cfg, prefixA);
  const b = await registerAndLogin(cfg, prefixB);
  return { a, b };
}

function dockerCompose(args) {
  return execFileSync('docker', ['compose', ...args], {
    cwd: rootDir,
    stdio: 'pipe',
    encoding: 'utf8',
  });
}

module.exports = {
  assert,
  claimMail,
  adminSendMail,
  dockerCompose,
  ensureRegistrationMailDelivered,
  findChatUser,
  getCatStats,
  getConfig,
  getInbox,
  getMail,
  http,
  loginUser,
  makeUser,
  poll,
  registerAndLogin,
  registerPair,
  registerUser,
  searchChatUsers,
  sleep,
  triggerRegistrationMailRetry,
};
