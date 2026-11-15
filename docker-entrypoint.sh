#!/bin/sh
set -e

if [ "$#" -gt 0 ]; then
    exec "$@"
fi

echo "==> Running database & sources initialization (morning-review init)..."
morning-review init

echo "==> Starting Uvicorn server on port ${PORT:-8000}..."
exec uvicorn morning_review.api.app:app \
    --host 0.0.0.0 \
    --port "${PORT:-8000}" \
    --proxy-headers \
    --forwarded-allow-ips="*"
