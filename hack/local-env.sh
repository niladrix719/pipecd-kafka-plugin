#!/usr/bin/env bash
#
#   ./hack/local-env.sh up      # start it all
#   ./hack/local-env.sh down    # tear it all down
#   ./hack/local-env.sh status  # what is running

set -euo pipefail

CLUSTER="${CLUSTER:-pipecd}"
CHART_VERSION="${CHART_VERSION:-v0.52.1}"
NAMESPACE="${NAMESPACE:-pipecd}"
PLUGIN_BIN="${PLUGIN_BIN:-$HOME/.piped/plugins/kafka}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing dependency: $1" >&2; exit 1; }
}

up() {
  require docker; require kind; require kubectl; require helm; require go
  docker info >/dev/null 2>&1 || { echo "Docker is not running. Start Docker Desktop first." >&2; exit 1; }

  log "Building the plugin into $PLUGIN_BIN"
  mkdir -p "$(dirname "$PLUGIN_BIN")"
  ( cd "$REPO_DIR" && CGO_ENABLED=0 go build -o "$PLUGIN_BIN" . )

  log "Starting Kafka (Redpanda) and the schema registry"
  ( cd "$REPO_DIR" && docker compose up -d --wait )

  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    log "kind cluster '$CLUSTER' already exists"
  else
    log "Creating kind cluster '$CLUSTER'"
    kind create cluster --name "$CLUSTER"
  fi

  log "Installing the PipeCD control plane ($CHART_VERSION)"
  helm -n "$NAMESPACE" upgrade --install pipecd \
    "oci://ghcr.io/pipe-cd/chart/pipecd" --version "$CHART_VERSION" \
    --create-namespace -f "$REPO_DIR/hack/control-plane-values.yaml"

  log "Waiting for the control plane to be ready"
  kubectl -n "$NAMESPACE" wait --for=condition=available --timeout=600s deployment --all

  cat <<EOF

Ready.

  Control plane   kubectl -n $NAMESPACE port-forward svc/pipecd 8080
                  http://localhost:8080  (quickstart / hello-pipecd / hello-pipecd)
  Kafka           localhost:9092
  Schema registry http://localhost:8081
  Redpanda console http://localhost:8090
  Plugin binary   $PLUGIN_BIN

Next: register a piped in the UI, put its ID and key in your piped config,
then run piped from the pipecd repo:

  make run/piped CONFIG_FILE=~/piped-config.yaml EXPERIMENTAL=true INSECURE=true
EOF
}

down() {
  log "Removing the kind cluster '$CLUSTER'"
  kind delete cluster --name "$CLUSTER" 2>/dev/null || true
  log "Stopping Kafka"
  ( cd "$REPO_DIR" && docker compose down -v 2>/dev/null || true )
  echo "done"
}

status() {
  echo "docker:  $(docker info >/dev/null 2>&1 && echo running || echo down)"
  echo "kind:    $(kind get clusters 2>/dev/null | tr '\n' ' ')"
  echo "kafka:   $(docker ps --filter name=kafka-plugin-redpanda --format '{{.Status}}' 2>/dev/null)"
  echo "pipecd:  $(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null | grep -c Running || echo 0) pods running"
  echo "ui:      $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080 2>/dev/null) (000 means no port-forward)"
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 {up|down|status}" >&2; exit 1 ;;
esac
