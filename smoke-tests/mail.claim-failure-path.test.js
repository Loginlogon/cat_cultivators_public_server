'use strict';

const test = require('node:test');

const {
  adminSendMail,
  assert,
  claimMail,
  dockerCompose,
  findChatUser,
  getConfig,
  getMail,
  http,
  poll,
  registerAndLogin,
} = require('./helpers');

test('mail stays non-claimed while game-service is unavailable and claims after recovery', async () => {
  const cfg = getConfig();
  const account = await registerAndLogin(cfg, 'smoke_claim_failure');
  const selfUser = await findChatUser(cfg, account.token, account.user.nickname);

  const created = await adminSendMail(cfg, {
    to_user_id: selfUser.id,
    subject: 'Failure path reward',
    body: 'Should remain unclaimed while game-service is down',
    reward_json: { money_mortals: 3, money_cultivators: 0 },
  });

  const mailId = created.json?.mail_id;
  assert.ok(mailId, 'mail_id missing');

  dockerCompose(['stop', 'game-service']);

  try {
    await claimMail(cfg, account.token, mailId, [502]);

    const afterFailure = await getMail(cfg, account.token, mailId);
    assert.notEqual(afterFailure.status, 'claimed');
  } finally {
    dockerCompose(['start', 'game-service']);
  }

  await poll(
    async () => {
      const ping = await http('GET', `${cfg.gameUrl}/ping`, { expected: [200] });
      return ping.json?.status === 'ok' ? true : null;
    },
    { timeoutMs: 20000, intervalMs: 1000, label: 'game-service restart' }
  );

  const success = await claimMail(cfg, account.token, mailId, [200]);
  assert.equal(success.json?.status, 'claimed');
});
