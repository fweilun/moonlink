#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# ==== Config (override via env) ====
CLUSTER="${CLUSTER:-kind-1}"
NS="${NS:-moonlink}"
MANIFEST_DIR="${MANIFEST_DIR:-deploy}"
NUKE_NAMESPACE="${NUKE_NAMESPACE:-false}"
NUKE_CLUSTER="${NUKE_CLUSTER:-false}"
QUIET="${QUIET:-false}"

log() { [[ "$QUIET" == "true" ]] || echo "$@"; }

# 0) Quick checks
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "Cluster '$CLUSTER' not found. Nothing to clean."
  exit 0
fi

if ! kubectl get ns "$NS" >/dev/null 2>&1; then
  log "Namespace '$NS' not found."
  if [[ "$NUKE_CLUSTER" == "true" ]]; then
    log "Deleting kind cluster: $CLUSTER"
    kind delete cluster --name "$CLUSTER"
  fi
  exit 0
fi

# 1) Delete resources defined by your manifests (preferred, precise)
if [[ -d "$MANIFEST_DIR" ]]; then
  log "Deleting resources from '$MANIFEST_DIR' in namespace '$NS'..."
  kubectl delete -n "$NS" -f "$MANIFEST_DIR" --ignore-not-found --wait=true
else
  log "Manifest directory '$MANIFEST_DIR' not found; skipping manifest-based deletion."
fi

# 2) Show what's left (if anything)
log "Remaining resources in namespace '$NS' (if any):"
kubectl get all,cm,secret,pvc -n "$NS" || true

# 3) Optionally delete the whole namespace
if [[ "$NUKE_NAMESPACE" == "true" ]]; then
  log "Deleting namespace: $NS"
  kubectl delete ns "$NS" --ignore-not-found
fi

# 4) Optionally delete the kind cluster
if [[ "$NUKE_CLUSTER" == "true" ]]; then
  log "Deleting kind cluster: $CLUSTER"
  kind delete cluster --name "$CLUSTER"
fi

log "🗑️  Cleanup completed."