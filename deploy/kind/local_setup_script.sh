#!/usr/bin/env bash
set -euo pipefail

# === Configurable parameters (override via environment variables) ===
CLUSTER="${CLUSTER:-kind-1}"
NS="${NS:-moonlink}"
MANIFEST_DIR="${MANIFEST_DIR:-k8s}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-60s}"
PORT_FWD="${PORT_FWD:-false}"

echo "==> Checking if kind cluster exists: $CLUSTER"
if ! kind get clusters | grep -qx "$CLUSTER"; then
  echo "Cluster '$CLUSTER' does not exist. Creating..."
  kind create cluster --name "$CLUSTER"
else
  echo "Cluster '$CLUSTER' already exists."
fi

echo "==> Ensuring namespace: $NS"
kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS"

if docker image inspect moonlink:dev >/dev/null 2>&1; then
    echo "==> Local image 'moonlink:dev' already exists."
else
    echo "==> Building local image 'moonlink:dev'"
    docker build -t moonlink:dev -f Dockerfile.aarch64 .
fi


echo "==> Loading image into kind nodes"
kind load docker-image moonlink:dev --name kind-1

echo "==> Applying Kubernetes manifests from: $MANIFEST_DIR/moonlink_deployment.yaml"
kubectl apply -f $MANIFEST_DIR/moonlink_deployment.yaml -n "$NS"

DEPLOY_NAME="$(yq '.metadata.name' "$MANIFEST_DIR/moonlink_deployment.yaml")"
echo "==> Waiting for deployment rollout: $DEPLOY_NAME"
kubectl rollout status -n "$NS" deploy/"$DEPLOY_NAME" --timeout="$WAIT_TIMEOUT"

echo "==> Current Pods and Services in namespace: $NS"
kubectl get pods,svc -n "$NS"

echo "Setup completed successfully."