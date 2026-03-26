'use strict';

const test = require('node:test');

const {
  getConfig,
  http,
  registerAndLogin,
} = require('./helpers');

test('avatars/add is forbidden for ordinary player', async () => {
  const cfg = getConfig();
  const account = await registerAndLogin(cfg, 'smoke_avatar_guard');

  await http('POST', `${cfg.authUrl}/avatars/add`, {
    token: account.token,
    body: { user_id: 1, avatar: 'cat_01' },
    expected: [403],
  });
});
