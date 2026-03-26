'use strict';

const test = require('node:test');

const {
  assert,
  findChatUser,
  getConfig,
  http,
  registerPair,
} = require('./helpers');

test('dm start and send work', async () => {
  const cfg = getConfig();
  const pair = await registerPair(cfg, 'smoke_dm_a', 'smoke_dm_b');

  const userB = await findChatUser(cfg, pair.a.token, pair.b.user.nickname);

  const started = await http('POST', `${cfg.chatUrl}/chat/dm/start`, {
    token: pair.a.token,
    body: { contact_user_id: userB.id },
    expected: [200, 201],
  });

  const conversationId = started.json?.conversation_id;
  assert.ok(conversationId, 'conversation_id missing');

  const sent = await http('POST', `${cfg.chatUrl}/chat/dm/${conversationId}/send`, {
    token: pair.a.token,
    body: { body: `dm message ${Date.now()}` },
    expected: [201],
  });

  const messageId = sent.json?.data?.id;
  assert.ok(messageId, 'dm send did not return id');

  const history = await http('GET', `${cfg.chatUrl}/chat/dm/${conversationId}/history?limit=20`, {
    token: pair.b.token,
    expected: [200],
  });

  assert.ok((history.json?.messages || []).some((m) => Number(m.id) === Number(messageId)));
});
