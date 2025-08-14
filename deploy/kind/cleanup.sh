#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ==== Config (override via env) ====
CLUSTER="${CLUSTER:-kind-1}"
NS="${NS:-moonlink}"
MANIFEST_DIR="${MANIFEST_DIR:-deploy/kind}"
DEPLOYMENT_CONFIG_DIR="${MANIFEST_DIR}/deployment/moonlink_deployment.yaml"
SERVICE_CONFIG_DIR="${MANIFEST_DIR}/service/moonlink_service.yaml"
NUKE_NAMESPACE="${NUKE_NAMESPACE:-false}"
NUKE_CLUSTER="${NUKE_CLUSTER:-false}"
QUIET="${QUIET:-false}"

log() { [[ "$QUIET" == "true" ]] || echo "$@"; }

# 0) Quick checks
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "Cluster '$CLUSTER' not found. Nothing to clean."
  exit 0
fi

# 1) Optionally delete the kind cluster first (this will delete everything)
if [[ "$NUKE_CLUSTER" == "true" ]]; then
  log "Deleting kind cluster: $CLUSTER"
  kind delete cluster --name "$CLUSTER"
  log "Cleanup completed."
  exit 0
fi

# 2) Check if namespace exists
if ! kubectl get ns "$NS" >/dev/null 2>&1; then
  log "Namespace '$NS' not found. Nothing to clean."
  exit 0
fi

# 3) Optionally delete the whole namespace (this will delete all resources in it)
if [[ "$NUKE_NAMESPACE" == "true" ]]; then
  log "Deleting namespace: $NS"
  kubectl delete ns "$NS" --ignore-not-found
  log "Cleanup completed."
  exit 0
fi

# 4) Delete specific resources defined by your manifests (preferred, precise)
if [[ -d "$MANIFEST_DIR" ]]; then
  log "Deleting resources from '$DEPLOYMENT_CONFIG_DIR' in namespace '$NS'..."
  kubectl delete -n "$NS" -f "$DEPLOYMENT_CONFIG_DIR" --ignore-not-found --wait=true

  log "Deleting resources from '$SERVICE_CONFIG_DIR' in namespace '$NS'..."
  kubectl delete -n "$NS" -f "$SERVICE_CONFIG_DIR" --ignore-not-found --wait=true
else
  log "Manifest directory '$MANIFEST_DIR' not found; skipping manifest-based deletion."
fi

# Wait for resources to be fully deleted (optional, since we already use --wait=true above)
log "Waiting for resources to be fully deleted..."
kubectl wait --for=delete deployment/moonlink-dev -n "$NS" --timeout=60s 2>/dev/null || true
kubectl wait --for=delete service/moonlink-service -n "$NS" --timeout=60s 2>/dev/null || true

# 5) Show what's left (if anything)
log "Remaining resources in namespace '$NS' (if any):"
kubectl get all,cm,secret,pvc -n "$NS" || true

echo
log "Cleanup completed."