#!/usr/bin/env bash
# Bootstrap Redpanda topics for AML-Sentinel (doc 02 §1).
# Idempotent: re-running against an already-bootstrapped broker is safe.

set -euo pipefail

BROKER="${REDPANDA_BROKERS:-localhost:9092}"

echo "[bootstrap] connecting to broker: ${BROKER}"

create_topic() {
  local topic="$1"
  local partitions="$2"
  echo "[bootstrap] ensuring topic '${topic}' (partitions=${partitions})"
  rpk topic create "${topic}" \
    --brokers "${BROKER}" \
    --partitions "${partitions}" \
    --replicas 1 \
    2>&1 | grep -v "TOPIC_ALREADY_EXISTS" || true
}

# ── Topics per doc 02 §1 ────────────────────────────────────────────────────
create_topic "client.submitted"    3
create_topic "profile.normalized"  3
create_topic "screening.completed" 3
create_topic "decision.made"       3
create_topic "watchlist.updated"   1

echo "[bootstrap] listing all topics:"
rpk topic list --brokers "${BROKER}"

echo "[bootstrap] done."
