#!/usr/bin/env bash
set -e

# Default vars
LOAD_KIND=false
ARCH="linux/amd64"
IMAGE="winterman/badis:latest"

# Parse args
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --kind) LOAD_KIND=true ;;
        --arch) ARCH="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo "🔨 Building $IMAGE for $ARCH..."
docker build --platform "$ARCH" -t "$IMAGE" .

if [ "$LOAD_KIND" = true ]; then
    echo "📦 Loading image into Kind cluster..."
    kind load docker-image "$IMAGE"
fi

echo "🚀 Deploying to Kubernetes (with E2E test pod)..."
kubecfg update k8s/badis.jsonnet --tla-code runTests=true

echo "✅ Deploy complete!"
