#!/usr/bin/env bash
# Deploy the running data-gateway to the current source tree.
#
# Why this exists: gateway source is bind-mounted (./gateway:/app/gateway) and
# loaded by uvicorn at process start, so a committed code fix does NOT take
# effect until the container is restarted. A fix can sit undeployed for hours
# (that is exactly how the darkpool feed stayed broken after its fix landed).
# This makes deploying one reliable, verifiable step.
#
#   scripts/deploy.sh          # code-only change  -> restart (fast, reloads source)
#   scripts/deploy.sh --build  # deps / Dockerfile -> rebuild image + recreate
#
# Waits for the container healthcheck and prints the deployed commit so the
# running code is never silently stale.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--build" ]]; then
  echo "Rebuilding image + recreating data-gateway…"
  docker compose up -d --build gateway
else
  echo "Restarting data-gateway to load current source…"
  docker compose restart gateway
fi

printf "Waiting for healthcheck"
healthy=false
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

sha="$(git rev-parse --short HEAD 2>/dev/null || echo '?')"
subject="$(git log -1 --format='%s' 2>/dev/null || echo '?')"
if [[ "${healthy}" == "true" ]]; then
  echo "✅ Deployed ${sha}: ${subject}"
else
  echo "⚠️  Container not reporting healthy yet — check: docker logs --tail 50 data-gateway"
  echo "    (attempted deploy of ${sha}: ${subject})"
  exit 1
fi
