#!/usr/bin/env bash
set -euo pipefail

# GridMR master setup and run (Ubuntu). Builds the Java master and runs it natively.

# Config (override via env)
: "${MASTER_PORT:=50051}"
: "${MASTER_HTTP_PORT:=8080}"
: "${SHARED_DATA_ROOT:=/shared}"

log() { echo "[master.sh] $*"; }

require_root() {
  if [ "${EUID}" -ne 0 ]; then
    log "This script must run as root (sudo)."; exit 1;
  fi
}

install_deps() {
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y --no-install-recommends \
    ca-certificates curl \
    openjdk-17-jre-headless maven \
    git
}

build_master() {
  local repo_dir="$1"
  log "Building GridMR master in ${repo_dir}"
  (cd "${repo_dir}" && mvn -q -DskipTests -DskipITs package)
}

run_master() {
  local repo_dir="$1"
  mkdir -p /var/log/gridmr
  log "Starting master: gRPC=${MASTER_PORT} HTTP=${MASTER_HTTP_PORT} SHARED=${SHARED_DATA_ROOT}"
  nohup env \
    MASTER_PORT="${MASTER_PORT}" \
    MASTER_HTTP_PORT="${MASTER_HTTP_PORT}" \
    SHARED_DATA_ROOT="${SHARED_DATA_ROOT}" \
    java -jar "${repo_dir}/target/gridmr-master.jar" \
    > /var/log/gridmr/master.out 2>&1 &
  echo $! > /var/run/gridmr-master.pid
  log "Master started. PID $(cat /var/run/gridmr-master.pid). Logs: /var/log/gridmr/master.out"
}

main() {
  require_root
  # Resolve repo root (this script lives at deployment/scripts/master.sh)
  local repo_dir
  repo_dir="$(realpath "$(dirname "$0")/../..")"
  install_deps
  build_master "${repo_dir}"
  run_master "${repo_dir}"
}

main "$@"
