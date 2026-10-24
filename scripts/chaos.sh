#!/usr/bin/env bash
# Runs the chaos suite (internal/chaos, build tag "chaos").
#
#   scripts/chaos.sh                 # random seed, printed at start
#   BATON_CHAOS_SEED=42 scripts/chaos.sh -run TestKillLeader
#
# Needs Docker (Postgres and Toxiproxy run in containers; baton and
# baton-worker are built and run as real processes so kill -9 is real).
# The inference-timeout scenario also needs the verified model and
# libonnxruntime in the cache (`baton model fetch`); it skips otherwise.
set -euo pipefail
cd "$(dirname "$0")/.."
exec go test -tags chaos -count=1 -v -timeout 45m ./internal/chaos "$@"
