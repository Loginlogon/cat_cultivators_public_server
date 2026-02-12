// game-mail-service/env.js
const fs = require("fs");

function readEnv(name, required = true) {
  const fileKey = `${name}_FILE`;

  if (process.env[fileKey]) {
    const p = process.env[fileKey];
    const v = fs.readFileSync(p, "utf8").trim();
    if (!v && required) throw new Error(`${fileKey} is empty`);
    return v;
  }

  const v = process.env[name];
  if ((!v || !String(v).trim()) && required) {
    throw new Error(`Missing env var: ${name} (or ${fileKey})`);
  }
  return v;
}

module.exports = { readEnv };
