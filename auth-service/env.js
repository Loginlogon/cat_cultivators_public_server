'use strict';

function readEnv(name, opts = {}) {
  const required = opts.required !== false;
  const allowEmpty = opts.allowEmpty === true;

  const v = process.env[name];

  if (v === undefined || v === null) {
    if (required) throw new Error(`Missing env var: ${name}`);
    return '';
  }

  const s = String(v);

  if (required && !allowEmpty && s.trim() === '') {
    throw new Error(`Empty env var: ${name}`);
  }

  return s;
}

module.exports = { readEnv };





