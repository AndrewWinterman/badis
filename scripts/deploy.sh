#!/usr/bin/env bash
set -e

# Default vars
LOAD_KIND=false

HOST_ARCH=$(uname -m)
if [ "$HOST_ARCH" = "arm64" ] || [ "$HOST_ARCH" = "aarch64" ]; then
    DEFAULT_ARCH="linux/arm64"
else
    DEFAULT_ARCH="linux/amd64"
fi

ARCH="$DEFAULT_ARCH"
IMAGE="winterman/badis:latest"
KUBE_CONTEXT=""

# TLA defaults
NAMESPACE="default"
REPLICAS=3
PROXY_REPLICAS=2
VOLUME_SIZE="5Gi"
STORAGE_CLASS=""
RUN_TESTS="true"
RUN_BENCHMARKS="false"

show_help() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    -h, --help                 Show this help message
    --kind                     Load image into Kind cluster
    --arch ARCH                Target architecture (default: $DEFAULT_ARCH)
    --context CONTEXT          Kubernetes context to use
    --namespace NS             Kubernetes namespace (default: default)
    --replicas N               Number of storage replicas (default: 3)
    --proxy-replicas N         Number of proxy replicas (default: 2)
    --volume-size SIZE         PVC storage size (default: 5Gi)
    --storage-class CLASS      Storage class name (default: null/cluster default)
    --no-tests                 Do not run E2E tests during deployment
    --benchmarks               Run benchmark suite in the E2E pod
EOF
}

# Parse args
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -h|--help) show_help; exit 0 ;;
        --kind) LOAD_KIND=true ;;
        --arch) ARCH="$2"; shift ;;
        --context) KUBE_CONTEXT="$2"; shift ;;
        --namespace) NAMESPACE="$2"; shift ;;
        --replicas) REPLICAS="$2"; shift ;;
        --proxy-replicas) PROXY_REPLICAS="$2"; shift ;;
        --volume-size) VOLUME_SIZE="$2"; shift ;;
        --storage-class) STORAGE_CLASS="$2"; shift ;;
        --no-tests) RUN_TESTS="false" ;;
        --benchmarks) RUN_BENCHMARKS="true" ;;
        *) echo "Unknown parameter passed: $1"; show_help; exit 1 ;;
    esac
    shift
done

if [ -z "$KUBE_CONTEXT" ]; then
    echo "❌ Error: --context is required to prevent accidental deployments to the wrong cluster."
    echo ""
    show_help
    exit 1
fi

echo "🔨 Building $IMAGE for $ARCH..."
docker build --platform "$ARCH" -t "$IMAGE" .

if [ "$LOAD_KIND" = true ]; then
    echo "📦 Loading image into Kind cluster..."
    KIND_ARGS=""
    if [ -n "$KUBE_CONTEXT" ]; then
        # If context starts with kind-, strip it to get the cluster name
        if [[ "$KUBE_CONTEXT" == kind-* ]]; then
            CLUSTER_NAME="${KUBE_CONTEXT#kind-}"
            KIND_ARGS="-n $CLUSTER_NAME"
        else
            # Best effort if they passed something else
            KIND_ARGS="-n $KUBE_CONTEXT"
        fi
    fi
    go run sigs.k8s.io/kind@latest load docker-image $KIND_ARGS "$IMAGE"
fi

echo "🚀 Deploying to Kubernetes..."
KUBECFG_ARGS="--tla-str namespace=$NAMESPACE --tla-code replicas=$REPLICAS --tla-code proxyReplicas=$PROXY_REPLICAS --tla-str volumeSize=$VOLUME_SIZE --tla-code runTests=$RUN_TESTS --tla-code runBenchmarks=$RUN_BENCHMARKS"

if [ -n "$STORAGE_CLASS" ]; then
    KUBECFG_ARGS="$KUBECFG_ARGS --tla-str storageClass=$STORAGE_CLASS"
fi

if [ -n "$KUBE_CONTEXT" ]; then
    echo "Using Kubernetes context: $KUBE_CONTEXT"
    KUBECFG_ARGS="--context $KUBE_CONTEXT $KUBECFG_ARGS"
fi

kubecfg update k8s/badis.jsonnet $KUBECFG_ARGS

echo "✅ Deploy complete!"
