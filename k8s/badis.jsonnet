// k8s/badis.jsonnet
local name = 'badis';
local proxyName = 'badis-proxy';
local namespace = 'default';
local replicas = 3;
local proxyReplicas = 2;
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
                { name: 'POD_NAME', valueFrom: { fieldRef: { fieldPath: 'metadata.name' } } },
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
            resources: { requests: { storage: '5Gi' } },
          },
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
