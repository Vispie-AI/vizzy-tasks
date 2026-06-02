#!/usr/bin/env bash
#
# deploy.sh — pull this fork and (re)deploy the self-hosted Multica stack on
# a self-host box, preserving the existing Postgres DB, uploads, and .env.
#
# Designed for the "rebuild + deploy on push to main" flow. Safe to re-run.
#
# What it does:
#   1. git pull --ff-only the current branch.
#   2. Build the backend image from this checkout (multica-backend:<TAG>).
#      The frontend is left on its configured image unless --with-frontend
#      is passed (we rarely change the frontend; rebuilding it is slow).
#   3. Run the task-usage backfill, which is a required pre-migrate step on
#      version jumps (migration 103 refuses to run with a stale watermark).
#   4. Bring the stack up with the same compose project name so the existing
#      named volumes (DB + uploads) are reused. Never runs `down -v`.
#   5. Health-check the backend.
#
# Usage:
#   ./deploy.sh                 # backend-only, tag "dev"
#   ./deploy.sh --with-frontend # also rebuild the frontend
#   ./deploy.sh --no-pull       # skip git pull (deploy current checkout)
#   ./deploy.sh --tag v1        # build/tag multica-backend:v1
#
# Env (optional overrides):
#   ENV_FILE      path to the .env used by the stack (default: ./.env)
#   COMPOSE_PROJECT   compose project name (default: multica)
#   PORT          backend port for the health check (default: 8080)
set -euo pipefail

cd "$(dirname "$0")"

TAG="dev"
WITH_FRONTEND=0
DO_PULL=1
ENV_FILE="${ENV_FILE:-.env}"
PROJECT="${COMPOSE_PROJECT:-multica}"
HEALTH_PORT="${PORT:-8080}"

while [ $# -gt 0 ]; do
  case "$1" in
    --with-frontend) WITH_FRONTEND=1 ;;
    --no-pull)       DO_PULL=0 ;;
    --tag)           TAG="$2"; shift ;;
    -h|--help)       sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

BACKEND_IMAGE="multica-backend:${TAG}"
COMPOSE_FILE="docker-compose.selfhost.yml"

log() { echo "==> $*"; }

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: env file not found: $ENV_FILE" >&2
  echo "Copy your existing self-host .env here, or set ENV_FILE." >&2
  exit 1
fi

# 1. Pull latest.
if [ "$DO_PULL" = "1" ]; then
  log "git pull --ff-only"
  git pull --ff-only
fi
log "deploying $(git rev-parse --short HEAD) on branch $(git rev-parse --abbrev-ref HEAD)"

# 2. Build backend image from this checkout.
log "building ${BACKEND_IMAGE} from source"
docker build -t "${BACKEND_IMAGE}" -f Dockerfile .

if [ "$WITH_FRONTEND" = "1" ]; then
  log "building multica-web:${TAG} from source"
  docker build -t "multica-web:${TAG}" -f Dockerfile.web \
    --build-arg REMOTE_API_URL="http://backend:8080" .
fi

# 3. Pin the backend image in .env so a plain `docker compose up` (e.g. after a
#    reboot) also uses our fork build, not the upstream GHCR image. Idempotent.
log "pinning MULTICA_BACKEND_IMAGE / MULTICA_IMAGE_TAG in ${ENV_FILE}"
set_env() {
  local key="$1" val="$2"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${val}|" "$ENV_FILE"
  else
    echo "${key}=${val}" >> "$ENV_FILE"
  fi
}
set_env MULTICA_BACKEND_IMAGE "multica-backend"
set_env MULTICA_IMAGE_TAG "${TAG}"
if [ "$WITH_FRONTEND" = "1" ]; then
  set_env MULTICA_WEB_IMAGE "multica-web"
fi

# 4. Required pre-migrate step on version jumps: the task-usage hourly backfill
#    must run before migration 103 (which otherwise refuses to drop the legacy
#    daily rollups with a stale watermark). Harmless to run when already
#    up-to-date — it just stamps the watermark to now()-5m.
log "running task-usage backfill (pre-migrate upgrade step)"
docker compose -p "$PROJECT" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" \
  run --rm --no-deps --entrypoint /app/backfill_task_usage_hourly backend || {
    echo "WARNING: backfill returned non-zero; continuing (it is a no-op when current)." >&2
  }

# 5. Bring the stack up. Same project name => existing pgdata/uploads volumes
#    are reused. We force-recreate the backend so the new image is picked up;
#    postgres/frontend are only recreated if their config changed.
log "starting stack (project=${PROJECT})"
docker compose -p "$PROJECT" --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up -d

# 6. Health check.
log "waiting for backend health on :${HEALTH_PORT}"
ok=0
for _ in $(seq 1 30); do
  if curl -sf "http://localhost:${HEALTH_PORT}/health" >/dev/null 2>&1; then ok=1; break; fi
  sleep 2
done
if [ "$ok" = "1" ]; then
  log "✓ backend healthy — running image: $(docker inspect ${PROJECT}-backend-1 --format '{{.Config.Image}}')"
else
  echo "✗ backend did not become healthy in time. Recent logs:" >&2
  docker logs "${PROJECT}-backend-1" --tail 40 2>&1 || true
  exit 1
fi
