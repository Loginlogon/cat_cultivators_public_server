'use strict';

const test = require('node:test');

const {
  assert,
  getConfig,
  http,
  registerAndLogin,
} = require('./helpers');

test('global chat send works', async () => {
  const cfg = getConfig();
  const account = await registerAndLogin(cfg, 'smoke_chat_global');

  const sent = await http('POST', `${cfg.chatUrl}/chat/global/send`, {
    token: account.token,
    body: { body: `global message ${Date.now()}` },
    expected: [201],
  });

  const messageId = sent.json?.data?.id;
  assert.ok(messageId, 'global send did not return id');

  const history = await http('GET', `${cfg.chatUrl}/chat/global/history?limit=20`, {
    token: account.token,
    expected: [200],
  });

  assert.ok((history.json?.messages || []).some((m) => Number(m.id) === Number(messageId)));
});
