#!/bin/sh
set -eu

read_secret() {
  name="$1"
  path="/run/secrets/$name"
  if [ -f "$path" ]; then
    cat "$path" | tr -d '\r'
  else
    echo ""
  fi
}

# --- DB secrets ---
DB_USER="$(read_secret DB_USER)"
DB_PASSWORD="$(read_secret DB_PASSWORD)"
DB_NAME="$(read_secret DB_NAME)"

if [ -n "$DB_USER" ]; then export DB_USER; fi
if [ -n "$DB_PASSWORD" ]; then export DB_PASSWORD; fi
if [ -n "$DB_NAME" ]; then export DB_NAME; fi

# Build DATABASE_URL for node (pg)
if [ -n "${DATABASE_URL:-}" ]; then
  :
else
  if [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ] || [ -z "$DB_NAME" ]; then
    echo "❌ Missing DB secrets (DB_USER/DB_PASSWORD/DB_NAME) to build DATABASE_URL" >&2
    exit 1
  fi
  export DATABASE_URL="postgres://${DB_USER}:${DB_PASSWORD}@db:5432/${DB_NAME}"
fi

# --- App secrets ---
ACCESS_SECRET="$(read_secret ACCESS_SECRET)"
ADMIN_SECRET_KEY="$(read_secret ADMIN_SECRET_KEY)"
FIREBASE_SERVICE_ACCOUNT_JSON="$(read_secret FIREBASE_SERVICE_ACCOUNT_JSON)"

if [ -n "$ACCESS_SECRET" ]; then export ACCESS_SECRET; fi
if [ -n "$ADMIN_SECRET_KEY" ]; then export ADMIN_SECRET_KEY; fi
if [ -n "$FIREBASE_SERVICE_ACCOUNT_JSON" ]; then export FIREBASE_SERVICE_ACCOUNT_JSON; fi

if [ "$#" -eq 0 ]; then
  set -- node server.js
fi

exec "$@"
