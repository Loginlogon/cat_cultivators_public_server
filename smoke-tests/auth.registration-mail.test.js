'use strict';

const test = require('node:test');

const {
  assert,
  ensureRegistrationMailDelivered,
  getCatStats,
  getConfig,
  registerAndLogin,
} = require('./helpers');

test('registration creates profile and puts start mail into delivery flow', async () => {
  const cfg = getConfig();
  const account = await registerAndLogin(cfg, 'smoke_auth_mail');

  assert.ok(['sent', 'queued'].includes(account.reg.registration_mail), 'registration_mail flag missing');

  const stats = await getCatStats(cfg, account.token);
  assert.equal(typeof stats.nickname, 'string');

  const inbox = await ensureRegistrationMailDelivered(cfg, account.token);
  assert.ok(inbox.length > 0, 'expected registration mail in inbox');
});
