# Badis Jsonnet Parametrization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parametrize the Kubernetes deployment configuration so that namespace, replicas, proxyReplicas, volume size, and storage class can be configured via Jsonnet Top-Level Arguments (TLAs).

**Architecture:** Wrap the existing array of Kubernetes resources in `k8s/badis.jsonnet` with a Jsonnet function that accepts arguments with default values.

**Tech Stack:** Jsonnet, Kubernetes (`kubecfg`).

---

### Task 1: Refactor `k8s/badis.jsonnet` to use TLAs

**Files:**
- Modify: `k8s/badis.jsonnet`

- [ ] **Step 1: Replace locals with a function wrapper**

```jsonnet
// k8s/badis.jsonnet
function(
  namespace='default',
  replicas=3,
  proxyReplicas=2,
  volumeSize='5Gi',
  storageClass=null
)
  local name = 'badis';
  local proxyName = 'badis-proxy';
  local image = 'winterman/badis:latest';

  local selector = { app: name };
  local proxySelector = { app: proxyName };

  // Helper to generate shard connection strings
  local shards = std.join(',', [name + '-' + i + '.' + name + '-headless:6379' for i in std.range(0, replicas - 1)]);

  [
    // 1. Headless Service for Shards
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: name + '-headless',
        namespace: namespace,
        labels: selector,
      },
      spec: {
        clusterIP: 'None',
        selector: selector,
        ports: [
          { name: 'redis', port: 6379, targetPort: 6379 },
          { name: 'raft', port: 6380, targetPort: 6380 },
        ],
      },
    },
    // 2. Client Service (Now points to proxy)
    {
      apiVersion: 'v1',
      kind: 'Service',
      metadata: {
        name: name,
        namespace: namespace,
        labels: proxySelector, // route to proxy
      },
      spec: {
        selector: proxySelector,
        ports: [
          { name: 'redis', port: 6379, targetPort: 6379 },
        ],
      },
    },
    // 3. Shard StatefulSet
    {
      apiVersion: 'apps/v1',
      kind: 'StatefulSet',
      metadata: {
        name: name,
        namespace: namespace,
        labels: selector,
      },
      spec: {
        serviceName: name + '-headless',
        replicas: replicas,
        selector: { matchLabels: selector },
        template: {
          metadata: { labels: selector },
          spec: {
            securityContext: { fsGroup: 10001 },
            containers: [
              {
                name: name,
                image: image,
                imagePullPolicy: 'IfNotPresent',
                ports: [
                  { containerPort: 6379, name: 'redis' },
                  { containerPort: 6380, name: 'raft' },
                ],
                env: [
                  { name: 'BADIS_DATA_DIR', value: '/data/badis-data' },
                  { name: 'BADIS_PORT', value: ':6379' },
                ],
                volumeMounts: [
                  { name: 'data', mountPath: '/data' },
                ],
              },
            ],
          },
        },
        volumeClaimTemplates: [
          {
            metadata: { name: 'data' },
            spec: {
              accessModes: ['ReadWriteOnce'],
              resources: { requests: { storage: volumeSize } },
            } + (if storageClass != null then { storageClassName: storageClass } else {}),
          },
        ],
      },
    },
    // 4. Proxy Deployment
    {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: proxyName,
        namespace: namespace,
        labels: proxySelector,
      },
      spec: {
        replicas: proxyReplicas,
        selector: { matchLabels: proxySelector },
        template: {
          metadata: { labels: proxySelector },
          spec: {
            containers: [
              {
                name: proxyName,
                image: image,
                imagePullPolicy: 'IfNotPresent',
                ports: [
                  { containerPort: 6379, name: 'redis' },
                ],
                env: [
                  { name: 'BADIS_PROXY_MODE', value: 'true' },
                  { name: 'BADIS_PORT', value: ':6379' },
                  { name: 'BADIS_SHARDS', value: shards },
                ],
              },
            ],
          },
        },
      },
    },
  ]
```

- [ ] **Step 2: Validate jsonnet default evaluation**

Run: `docker run --rm -v $(pwd):/work -w /work bitnami/jsonnet k8s/badis.jsonnet`
Expected: Output showing default values (replicas: 3, proxyReplicas: 2, storage: 5Gi, no storageClassName).

- [ ] **Step 3: Validate jsonnet with TLAs**

Run: `docker run --rm -v $(pwd):/work -w /work bitnami/jsonnet k8s/badis.jsonnet --tla-code replicas=5 --tla-str storageClass=fast-ssd --tla-str volumeSize=10Gi --tla-str namespace=prod`
Expected: Output showing updated values (replicas: 5, namespace: prod, storageClassName: fast-ssd, storage: 10Gi).

- [ ] **Step 4: Commit**

```bash
git add k8s/badis.jsonnet
git commit -m "feat: parameterize jsonnet deployment with TLAs"
```