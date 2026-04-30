// k8s/badis.jsonnet
local name = 'badis';
local namespace = 'default';
local replicas = 3;
local image = 'winterman/badis:latest';

local selector = {
  app: name,
};

[
  // 1. Headless Service for Raft Peer Discovery
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
  // 2. Client Service
  {
    apiVersion: 'v1',
    kind: 'Service',
    metadata: {
      name: name,
      namespace: namespace,
      labels: selector,
    },
    spec: {
      selector: selector,
      ports: [
        { name: 'redis', port: 6379, targetPort: 6379 },
      ],
    },
  },
  // 3. StatefulSet
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
          securityContext: {
            fsGroup: 10001, // match appuser from Dockerfile
          },
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
                {
                  name: 'BADIS_DATA_DIR',
                  value: '/data/badis-data',
                },
                {
                  name: 'BADIS_PORT',
                  value: ':6379',
                },
                {
                  name: 'POD_NAME',
                  valueFrom: {
                    fieldRef: { fieldPath: 'metadata.name' },
                  },
                },
              ],
              volumeMounts: [
                {
                  name: 'data',
                  mountPath: '/data',
                },
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
            resources: {
              requests: { storage: '5Gi' },
            },
          },
        },
      ],
    },
  },
]
