'use strict';

function readEnv(name, opts = {}) {
  const required = opts.required !== false;
  const allowEmpty = opts.allowEmpty === true;

  const v = process.env[name];

  if (v === undefined || v === null) {
    if (required) throw new Error(`Missing env var: ${name}`);
    return '';
  }

  let s = String(v);
  if (s.charCodeAt(0) === 0xfeff) s = s.slice(1);

  if (required && !allowEmpty && s.trim() === '') {
    throw new Error(`Empty env var: ${name}`);
  }

  return s;
}

module.exports = { readEnv };
