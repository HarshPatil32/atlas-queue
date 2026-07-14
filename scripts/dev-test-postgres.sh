#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.test.yml"

case "${1:-up}" in
  up)
    docker compose -f "$COMPOSE_FILE" up -d --wait
    echo "Test Postgres ready at postgresql+asyncpg://atlas:atlas@localhost:5434/atlas_queue_test"
    ;;
  down)
    docker compose -f "$COMPOSE_FILE" down -v
    ;;
  status)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  *)
    echo "usage: $0 [up|down|status]" >&2
    exit 1
    ;;
esac
