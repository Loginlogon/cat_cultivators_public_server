#!/bin/sh
set -eu

SECRETS_DIR="/run/secrets"

read_secret_trim() {
  name="$1"
  path="${SECRETS_DIR}/${name}"
  if [ -f "$path" ]; then
    cat "$path" | tr -d '\r' | tr -d '\n'
    return 0
  fi
  return 1
}

export ADMIN_SECRET_KEY="${ADMIN_SECRET_KEY:-$(read_secret_trim ADMIN_SECRET_KEY 2>/dev/null || echo "")}"
export DB_USER="${DB_USER:-$(read_secret_trim DB_USER 2>/dev/null || echo "")}"
export DB_PASSWORD="${DB_PASSWORD:-$(read_secret_trim DB_PASSWORD 2>/dev/null || echo "")}"
export DB_NAME="${DB_NAME:-$(read_secret_trim DB_NAME 2>/dev/null || echo "")}"

: "${DB_HOST:=db}"
: "${DB_PORT:=5432}"

if [ -z "${DATABASE_URL:-}" ]; then
  if [ -n "${DB_USER}" ] && [ -n "${DB_PASSWORD}" ] && [ -n "${DB_NAME}" ]; then
    export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
  fi
fi

echo "[entrypoint] game-service starting; PORT=${PORT:-3005} DB_HOST=${DB_HOST} DB_NAME=${DB_NAME}"

exec node server.js
