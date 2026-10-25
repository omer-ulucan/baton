#!/usr/bin/env bash
# Baton demo: brings up the local stack and walks through a DAG, a retry,
# a saga compensation, the DLQ, and a leader failover.
#
#   scripts/demo.sh            # leaves the stack running at the end
#   scripts/demo.sh --down     # tears it down afterwards
#
# Needs Docker with Compose, and Go to build batonctl.
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
ctl() { ./bin/batonctl --server "$BATON_ADDR" "$@"; }
wid() { ctl --output json submit -f "$1" | sed -n 's/.*"workflow_id": *"\([^"]*\)".*/\1/p'; }
wait_status() { # id status...
  local id=$1; shift
  for _ in $(seq 1 60); do
    s=$(ctl --output json status "$id" | sed -n 's/^  "status": *"\([^"]*\)".*/\1/p' | head -1)
    for want in "$@"; do [ "$s" = "$want" ] && { echo "$id -> $s"; return; }; done
    sleep 0.5
  done
  echo "timed out waiting for $id (last status: $s)" >&2; return 1
}
role() { curl -s "localhost:$1/readyz" | sed -n 's/.*"role":"\([a-z]*\)".*/\1/p'; }

step "Building batonctl and starting the stack (Postgres, 2 orchestrators, 3 workers, Toxiproxy, Prometheus, Grafana, Jaeger)"
go build -o bin/batonctl ./cmd/batonctl
docker compose up -d --build --wait baton-1 baton-2 >/dev/null
docker compose up -d >/dev/null
export BATON_ADDR=http://localhost:8082 # a follower is fine: writes are forwarded to the leader
echo "baton-1 is $(role 8081), baton-2 is $(role 8082)"

step "1. A diamond DAG whose first step fails once and is retried"
id=$(wid examples/order.yaml)
wait_status "$id" succeeded
ctl status "$id" | sed -n '/STEP/,$p'

step "2. A saga: ship fails permanently, so reserve and charge are compensated in reverse order"
id=$(wid examples/saga.yaml)
wait_status "$id" compensated
ctl status "$id" --events | grep -E "EVENT|Compensation|WorkflowFailed|TaskDeadLettered"

step "3. A permanent failure lands in the DLQ; retrying it from the DLQ runs it again"
id=$(wid examples/doomed.yaml)
wait_status "$id" failed
ctl dlq list
task=$(ctl dlq list | awk -v w="$id" '$2 == w {print $1}') # this workflow's task
ctl dlq retry "$task"
wait_status "$id" failed
echo "(the handler fails permanently on purpose, so the retried task returns to the DLQ)"

step "4. Kill the leader with workflows in flight"
for _ in $(seq 1 10); do ctl submit -f examples/order.yaml >/dev/null; done
leader=baton-1; port=8081
[ "$(role 8082)" = leader ] && { leader=baton-2; port=8082; }
other=$([ $leader = baton-1 ] && echo 8082 || echo 8081)
export BATON_ADDR=http://localhost:$other
echo "killing $leader (docker kill: no graceful shutdown, its lease must expire)"
docker compose kill "$leader" >/dev/null
start=$(date +%s)
until [ "$(role "$other")" = leader ]; do sleep 0.5; done
echo "the follower took over after $(( $(date +%s) - start ))s (lease TTL 10s)"
for _ in $(seq 1 60); do
  running=$(ctl --output json list --status running | grep -c '"id"' || true)
  [ "$running" = 0 ] && break; sleep 1
done
ctl list --limit 100 | awk 'NR>1 {print $2}' | sort | uniq -c
docker compose start "$leader" >/dev/null
echo "$leader restarted and rejoins as a follower"

step "Where to look"
cat <<INFO
  API          http://localhost:8081  and  http://localhost:8082 (any replica)
  Grafana      http://localhost:3000  (dashboard "Baton")
  Jaeger       http://localhost:16686 (service "baton": one trace per submission, workers included)
  Prometheus   http://localhost:9090
  CLI          BATON_ADDR=http://localhost:8081 ./bin/batonctl list
INFO

if [ "${1:-}" = "--down" ]; then docker compose down -v; fi
