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

read_secret_raw() {
  name="$1"
  path="${SECRETS_DIR}/${name}"
  if [ -f "$path" ]; then
    cat "$path" | tr -d '\r'
    return 0
  fi
  return 1
}

export ACCESS_SECRET="${ACCESS_SECRET:-$(read_secret_trim ACCESS_SECRET 2>/dev/null || echo "")}"
export ADMIN_SECRET_KEY="${ADMIN_SECRET_KEY:-$(read_secret_trim ADMIN_SECRET_KEY 2>/dev/null || echo "")}"

export DB_USER="${DB_USER:-$(read_secret_trim DB_USER 2>/dev/null || echo "")}"
export DB_PASSWORD="${DB_PASSWORD:-$(read_secret_trim DB_PASSWORD 2>/dev/null || echo "")}"
export DB_NAME="${DB_NAME:-$(read_secret_trim DB_NAME 2>/dev/null || echo "")}"

# Firebase JSON именно как env (у тебя код читает process.env напрямую)
if [ -z "${FIREBASE_SERVICE_ACCOUNT_JSON:-}" ]; then
  export FIREBASE_SERVICE_ACCOUNT_JSON="$(read_secret_raw FIREBASE_SERVICE_ACCOUNT_JSON 2>/dev/null || echo "")"
fi

: "${DB_HOST:=db}"
: "${DB_PORT:=5432}"

if [ -z "${DATABASE_URL:-}" ]; then
  if [ -n "${DB_USER}" ] && [ -n "${DB_PASSWORD}" ] && [ -n "${DB_NAME}" ]; then
    export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
  fi
fi

echo "[entrypoint] notification-service starting; PORT=${PORT:-3002} DB_HOST=${DB_HOST} DB_NAME=${DB_NAME} has_firebase_json=$([ -n "${FIREBASE_SERVICE_ACCOUNT_JSON:-}" ] && echo yes || echo no)"

exec node server.js