#!/usr/bin/env bash
# Deploy data-gateway as a baked image built from the current source trees.
#
# Model (since 2026-07-23): gateway code ships ONLY via the built image — the
# compose file has no ./gateway source mount, so the working tree is not
# production and editing files never changes the running service. Every code
# deploy is: build image → tag → recreate the gateway container (never redis).
#
#   scripts/deploy.sh            # build + deploy current source as a new tag
#   scripts/deploy.sh --restart  # config-only reload: restart without rebuild
#
# Roll back to any previous image:
#   docker image ls data-gateway                      # list available tags
#   GATEWAY_IMAGE_TAG=<YYYYMMDD-sha> docker compose up -d --no-deps gateway
#
# The image build COPYs ../empire-core, ../empire-schemas, and the vendored UW
# SDK from the monorepo working trees AS-IS — dirty trees get baked. The
# warnings below make that loud instead of silent.
set -euo pipefail
cd "$(dirname "$0")/.."

wait_healthy() {
  printf "Waiting for healthcheck"
  local healthy=false status
  for _ in $(seq 1 45); do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' data-gateway 2>/dev/null || echo unknown)"
    if [[ "${status}" == "healthy" ]]; then
      healthy=true
      break
    fi
    printf "."
    sleep 2
  done
  echo
  [[ "${healthy}" == "true" ]]
}

sha="$(git rev-parse --short HEAD 2>/dev/null || echo '?')"
subject="$(git log -1 --format='%s' 2>/dev/null || echo '?')"

if [[ "${1:-}" == "--restart" ]]; then
  echo "Restarting data-gateway (config-only reload — image unchanged)…"
  docker compose restart gateway
  if wait_healthy; then
    echo "✅ Restarted on existing image ($(docker inspect -f '{{.Config.Image}}' data-gateway))"
  else
    echo "⚠️  Container not reporting healthy — check: docker logs --tail 50 data-gateway"
    exit 1
  fi
  exit 0
fi

# Dirty-tree warnings: the image bakes these trees exactly as they sit on disk.
for repo in . ../empire-core ../empire-schemas; do
  if [[ -n "$(git -C "${repo}" status --porcelain --untracked-files=no 2>/dev/null)" ]]; then
    echo "⚠️  WARNING: $(cd "${repo}" && pwd) has uncommitted changes — they WILL be baked into this image."
  fi
done

TAG="$(date -u +%Y%m%d)-${sha}"
echo "Building data-gateway:${TAG} (${sha}: ${subject})…"
GATEWAY_IMAGE_TAG="${TAG}" docker compose build gateway
docker tag "data-gateway:${TAG}" data-gateway:latest

echo "Recreating gateway container (redis untouched)…"
GATEWAY_IMAGE_TAG="${TAG}" docker compose up -d --no-deps gateway

if wait_healthy; then
  echo "✅ Deployed data-gateway:${TAG} (${sha}: ${subject})"
else
  echo "⚠️  Container not reporting healthy — check: docker logs --tail 50 data-gateway"
  echo "    Roll back: GATEWAY_IMAGE_TAG=<previous> docker compose up -d --no-deps gateway"
  exit 1
fi
