'use strict';

const test = require('node:test');

const {
  assert,
  claimMail,
  ensureRegistrationMailDelivered,
  getCatStats,
  getConfig,
  getMail,
  registerAndLogin,
} = require('./helpers');

test('mail claim applies reward once and stays idempotent', async () => {
  const cfg = getConfig();
  const account = await registerAndLogin(cfg, 'smoke_mail_claim');

  const statsBefore = await getCatStats(cfg, account.token);
  const inbox = await ensureRegistrationMailDelivered(cfg, account.token);
  const mailId = inbox[0]?.id;
  assert.ok(mailId, 'registration mail missing');

  const mail = await getMail(cfg, account.token, mailId);
  const reward = mail.reward_json || {};
  const mortals = Number(reward.money_mortals || 0);
  const cultivators = Number(reward.money_cultivators || 0);

  const firstClaim = await claimMail(cfg, account.token, mailId, [200]);
  assert.equal(firstClaim.json?.status, 'claimed');

  const statsAfterFirstClaim = await getCatStats(cfg, account.token);
  assert.equal(Number(statsAfterFirstClaim.money_mortals), Number(statsBefore.money_mortals) + mortals);
  assert.equal(Number(statsAfterFirstClaim.money_cultivators), Number(statsBefore.money_cultivators) + cultivators);

  const secondClaim = await claimMail(cfg, account.token, mailId, [200]);
  assert.ok(['already_claimed', 'claimed'].includes(secondClaim.json?.status));

  const statsAfterSecondClaim = await getCatStats(cfg, account.token);
  assert.equal(Number(statsAfterSecondClaim.money_mortals), Number(statsAfterFirstClaim.money_mortals));
  assert.equal(Number(statsAfterSecondClaim.money_cultivators), Number(statsAfterFirstClaim.money_cultivators));
});
