#!/bin/sh
set -eu

SECRETS_DIR="/run/secrets"

# trim для ключей/паролей (убираем \r и переводы строк)
read_secret_trim() {
  name="$1"
  path="${SECRETS_DIR}/${name}"
  if [ -f "$path" ]; then
    cat "$path" | tr -d '\r' | tr -d '\n'
    return 0
  fi
  return 1
}

# raw для JSON (убираем только \r, но оставляем \n как пробельные символы JSON)
read_secret_raw() {
  name="$1"
  path="${SECRETS_DIR}/${name}"
  if [ -f "$path" ]; then
    cat "$path" | tr -d '\r'
    return 0
  fi
  return 1
}

# Прокидываем секреты в env (что есть — то подхватим)
export ACCESS_SECRET="${ACCESS_SECRET:-$(read_secret_trim ACCESS_SECRET 2>/dev/null || echo "")}"
export ADMIN_SECRET_KEY="${ADMIN_SECRET_KEY:-$(read_secret_trim ADMIN_SECRET_KEY 2>/dev/null || echo "")}"

export DB_USER="${DB_USER:-$(read_secret_trim DB_USER 2>/dev/null || echo "")}"
export DB_PASSWORD="${DB_PASSWORD:-$(read_secret_trim DB_PASSWORD 2>/dev/null || echo "")}"
export DB_NAME="${DB_NAME:-$(read_secret_trim DB_NAME 2>/dev/null || echo "")}"

: "${DB_HOST:=db}"
: "${DB_PORT:=5432}"

# Собираем DATABASE_URL, если не задан
if [ -z "${DATABASE_URL:-}" ]; then
  if [ -n "${DB_USER}" ] && [ -n "${DB_PASSWORD}" ] && [ -n "${DB_NAME}" ]; then
    export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
  fi
fi

# Диагностика (одной строкой, без утечки паролей)
echo "[entrypoint] game-mail starting; PORT=${PORT:-3001} DB_HOST=${DB_HOST} DB_NAME=${DB_NAME} REDIS_URL=${REDIS_URL:-} NOTIFICATION_URL=${NOTIFICATION_URL:-}"

exec node server.js