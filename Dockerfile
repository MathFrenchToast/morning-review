# ==============================================================================
# Stage 1: Builder (uv + Python 3.12)
# ==============================================================================
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
COPY backend/ ./backend/
COPY edge_sync/ ./edge_sync/

RUN uv sync --frozen --no-dev

# ==============================================================================
# Stage 2: Runtime (non-root user 10001:10001)
# ==============================================================================
FROM python:3.12-slim AS runtime

RUN groupadd --gid 10001 morning && \
    useradd --uid 10001 --gid 10001 --create-home --shell /bin/sh morning

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_ROOT="/app" \
    PATH="/app/.venv/bin:$PATH" \
    FASTEMBED_CACHE_PATH="/app/.cache/fastembed" \
    OMP_NUM_THREADS="2" \
    PORT="8000"

WORKDIR /app

COPY --from=builder --chown=morning:morning /app/.venv /app/.venv
COPY --from=builder --chown=morning:morning /app/pyproject.toml /app/README.md ./
COPY --from=builder --chown=morning:morning /app/backend ./backend
COPY --from=builder --chown=morning:morning /app/edge_sync ./edge_sync
COPY --chown=morning:morning config/ ./config/
COPY --chown=morning:morning frontend/ ./frontend/
COPY --chown=morning:morning docker-entrypoint.sh /app/docker-entrypoint.sh

RUN chmod +x /app/docker-entrypoint.sh && \
    mkdir -p /app/.cache/fastembed /app/reports /app/.data && \
    chown -R morning:morning /app/.cache /app/reports /app/.data /app/config

USER morning

EXPOSE 8000

ENTRYPOINT ["/app/docker-entrypoint.sh"]
