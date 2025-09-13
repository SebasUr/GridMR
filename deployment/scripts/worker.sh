#!/usr/bin/env bash
set -euo pipefail

# GridMR worker setup and run (Ubuntu). Builds the C++ worker and runs it natively.

# Config (override via env)
: "${MASTER_HOST:=172.16.1.10}"
: "${MASTER_PORT:=50051}"
: "${SHARED_DATA_ROOT:=/shared}"
: "${N_WORKERS:=1}"

log() { echo "[worker.sh] $*"; }

require_root() {
  if [ "${EUID}" -ne 0 ]; then
    log "This script must run as root (sudo)."; exit 1;
  fi
}

install_deps() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y --no-install-recommends \
    ca-certificates curl build-essential cmake pkg-config git \
    libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
    libgrpc++-dev libabsl-dev libre2-dev libc-ares-dev zlib1g-dev libssl-dev openssl
}

build_worker() {
  local repo_dir="$1"
  log "Building GridMR worker in ${repo_dir}"
  (cd "${repo_dir}" && cmake -S cpp/worker -B build && cmake --build build -j)
  install -m 0755 "${repo_dir}/build/worker" /usr/local/bin/gridmr-worker
  # Build default map/reduce samples too (optional)
  g++ -O2 -std=c++17 -o /usr/local/bin/map "${repo_dir}/cpp/worker/map.cc"
  g++ -O2 -std=c++17 -o /usr/local/bin/reduce "${repo_dir}/cpp/worker/reduce.cc"
}

run_worker() {
  mkdir -p /var/log/gridmr /var/run
  if [ ! -d "${SHARED_DATA_ROOT}" ]; then
    log "SHARED_DATA_ROOT ${SHARED_DATA_ROOT} does not exist. Create/mount it first."; exit 1;
  fi
  log "Starting ${N_WORKERS} worker(s): master=${MASTER_HOST}:${MASTER_PORT} SHARED=${SHARED_DATA_ROOT}"
  for i in $(seq 1 ${N_WORKERS}); do
    local logf="/var/log/gridmr/worker-${i}.out"
    nohup env \
      MASTER_HOST="${MASTER_HOST}" \
      MASTER_PORT="${MASTER_PORT}" \
      SHARED_DATA_ROOT="${SHARED_DATA_ROOT}" \
      MAP_LOCAL_PREFIX="${SHARED_DATA_ROOT}" \
      /usr/local/bin/gridmr-worker \
      > "${logf}" 2>&1 &
    echo $! > "/var/run/gridmr-worker-${i}.pid"
    log "Worker #${i} started. PID $(cat "/var/run/gridmr-worker-${i}.pid"). Logs: ${logf}"
    # Small stagger to avoid thundering herd on master registration
    sleep 0.5
  done
}

main() {
  require_root
  local repo_dir
  repo_dir="$(realpath "$(dirname "$0")/../..")"
  install_deps
  build_worker "${repo_dir}"
  run_worker
}

main "$@"
