#!/bin/sh
set -eu

compose="docker compose"
addresses="node1:7001,node2:7002,node3:7003"

cleanup() {
  if [ "${KEEP_CLUSTER:-0}" != "1" ]; then
    $compose down -v
  fi
}
trap cleanup EXIT INT TERM

$compose up -d --build

leader=""
attempt=0
while [ "$attempt" -lt 60 ]; do
  for node in 1 2 3; do
    output=$($compose exec -T "node$node" lsmdbctl -addresses="node$node:700$node" -timeout=1s status 2>/dev/null || true)
    if printf '%s' "$output" | grep -q '"role": "leader"'; then
      leader="$node"
      break
    fi
  done
  [ -n "$leader" ] && break
  attempt=$((attempt + 1))
  sleep 1
done

if [ -z "$leader" ]; then
  echo "no leader elected" >&2
  exit 1
fi

$compose exec -T node1 lsmdbctl -addresses="$addresses" put smoke before-failover >/dev/null
$compose stop "node$leader"

survivor=1
if [ "$leader" = "1" ]; then survivor=2; fi
$compose exec -T "node$survivor" lsmdbctl -addresses="$addresses" -timeout=10s put smoke after-failover >/dev/null
$compose start "node$leader"

attempt=0
while [ "$attempt" -lt 30 ]; do
  output=$($compose exec -T "node$survivor" lsmdbctl -addresses="$addresses" -timeout=2s get smoke 2>/dev/null || true)
  # protobuf bytes are rendered as base64 by encoding/json.
  if printf '%s' "$output" | grep -q 'YWZ0ZXItZmFpbG92ZXI='; then
    echo "docker failover smoke test passed (failed leader: node$leader)"
    exit 0
  fi
  attempt=$((attempt + 1))
  sleep 1
done

echo "restarted cluster did not return the committed value" >&2
exit 1
