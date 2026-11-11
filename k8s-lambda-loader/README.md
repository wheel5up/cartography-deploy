# Cartography Kubernetes (EKS) Lambda Loader

Syncs EKS clusters into Neo4j with Cartography's Kubernetes module. A Step
Functions state machine runs one Lambda invocation per cluster. Independent of
`../lambda-loader` (AWS).

## Design

```
EventBridge Scheduler cartography-k8s-lambda-daily (05:00 UTC, DISABLED by default)
  └── Step Functions: cartography-k8s-lambda-sync
        ├── CheckInputClusters / GetDefaultClusters  (/cartography/k8s-scan/clusters)
        ├── SyncClusters (Map, MaxConcurrency=5)
        │     └── Lambda cartography-k8s-lambda-loader  {"cluster_arn": "..."}
        └── EvalResults → Succeed, or Fail if any cluster failed
```

Every cluster runs even if one fails, but the execution ends `FAILED` if any
cluster failed, so failures can't hide behind a green execution.

**Per cluster, the Lambda:**
1. Parses region/account/name from the ARN.
2. Assumes `cartography-k8s-reader` in that account (through
   `IntermediateRoleName` first, if set; see [Role path](#role-path)).
3. Calls `eks:DescribeCluster` to get the endpoint and CA.
4. Generates the EKS bearer token in-process: a presigned STS
   `GetCallerIdentity` call, same as `aws eks get-token`. No AWS CLI is needed.
5. Writes a static-token kubeconfig to `/tmp`, named with the cluster ARN.
6. Runs the `start_k8s_ingestion()` sync sequence with two changes:
   - **secrets before pods** (see [below](#sync-order-secrets-before-pods))
   - `sync_eks()` gets the assumed-role session. Upstream uses a bare
     `boto3.Session()`, which in Lambda would be the Lambda's own role.
7. Emits `SyncDuration` and `SyncSuccess` (namespace `Cartography`, dimensions
   `Module=k8s,Cluster=<name>`).

**Other details:**
- **update_tag:** one per cluster. All k8s cleanups are scoped by `CLUSTER_ID`.
- **Lambda role:** `cartography-k8s-lambda-role-<region>` has a fixed name and
  `DeletionPolicy: Retain`. The reader trust names it by ARN, and IAM pins
  that to the role's unique ID, so deleting and recreating it silently breaks
  trust until the reader roles are updated.
- **Scale:** a few seconds per cluster for small clusters, about 30s for one
  with ~5k pods.

## Requirements in each cluster's account

Each synced cluster needs, in its own account:

1. **A reader role** (`cartography-k8s-reader`, or set `ReaderRoleName`) that
   trusts `cartography-k8s-lambda-role-<region>` (the `LambdaRoleArn` stack output), or
   the intermediate role if you use one. IAM permissions:
   - `eks:DescribeCluster`
   - `eks:ListAccessEntries` and `eks:ListIdentityProviderConfigs` (without
     them, the EKS access-entry and OIDC enrichment is skipped)
2. **An EKS Access Entry** mapping the reader role to a Kubernetes group.
3. **A read-only ClusterRole** bound to that group, with get/list/watch on what
   Cartography reads. Besides the core resources (namespaces, pods, services,
   secrets, nodes, RBAC, ingresses), include:
   - `apps/v1` deployments, replicasets, statefulsets, daemonsets
   - `batch/v1` jobs, cronjobs
   - storageclasses, persistentvolumes, persistentvolumeclaims
   - `networking.k8s.io` networkpolicies
   - `gateway.networking.k8s.io` gateways, httproutes

   Cartography currently skips any of these that return 403 with a warning,
   and says they'll be hard failures in v1.0.0.

## Configure and deploy

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build && sam deploy
```

Parameters:

| Parameter | Default | Notes |
|---|---|---|
| `VpcId`, `SubnetId` | — | Same VPC as Neo4j; private subnet with NAT |
| `DefaultClustersJson` | `[]` | Seeds `/cartography/k8s-scan/clusters` on stack create only |
| `ReaderRoleName` | `cartography-k8s-reader` | Role assumed in each cluster's account |
| `IntermediateRoleName` | `""` | Optional same-account hop; see [Role path](#role-path) |
| `MaxConcurrency` | `5` | Clusters synced in parallel |
| `ScheduleDefault` | `cron(0 5 * * ? *)` | Seeds `/cartography/k8s-lambda/schedule` |
| `ScheduleState` | `DISABLED` | Set `ENABLED` after a manual run succeeds |

Run AWS (`../lambda-loader`) before this loader; see
[Cross-module ordering](#cross-module-ordering-aws-vs-k8s).

### Run manually

```sh
# All clusters in /cartography/k8s-scan/clusters
aws stepfunctions start-execution --state-machine-arn <StateMachineArn>

# Specific clusters
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> \
  --input '{"clusters": ["arn:aws:eks:us-east-1:111111111111:cluster/example"]}'
```

### Add a cluster

Set up the [requirements](#requirements-in-each-clusters-account) in its
account, then append its ARN to the cluster list. No code change or redeploy
is needed:

```sh
aws ssm put-parameter --overwrite --name /cartography/k8s-scan/clusters \
  --value '["arn:aws:eks:us-east-1:111111111111:cluster/example", "..."]'
```

CloudFormation only sets this parameter on create, so later `sam deploy` runs
don't overwrite it.

Clusters in the graph that aren't synced yet:

```cypher
MATCH (a:AWSAccount)-[:RESOURCE]->(c:EKSCluster)
RETURN a.name, a.id, c.name, c.region ORDER BY a.name
```

## Role path

By default the Lambda assumes the reader role directly:

```
cartography-k8s-lambda-role-<region> → <account>/cartography-k8s-reader
```

If the reader roles already trust some other role in this account and you
can't change them yet, set `IntermediateRoleName` to that role:

```
cartography-k8s-lambda-role-<region> → <IntermediateRoleName> → <account>/cartography-k8s-reader
```

- The stack grants the Lambda role `sts:AssumeRole` on the intermediate role.
  The intermediate role's trust policy must allow `cartography-k8s-lambda-role-<region>`.
  Using the account root as principal with an `aws:PrincipalArn` condition
  works before the Lambda role exists and survives it being recreated.
- STS caps chained sessions at 1h. The reader session is 900s.
- The Lambda temporarily holds whatever the intermediate role can do. Treat it
  as a stopgap and switch to `""` once the reader roles trust the Lambda role.

## Sync order: secrets before pods

The handler syncs secrets **before** pods. Upstream `start_k8s_ingestion()`
does the opposite.

**Why:** pod→secret relationships (`USES_SECRET_ENV`, `USES_SECRET_VOLUME`,
`USES_SECRET`) match the target on `KubernetesSecret.composite_id`
(`<cluster>/<namespace>/<name>`). If no node matches, the edge is skipped
silently (`OPTIONAL MATCH … WHERE j IS NOT NULL`). With upstream's order, a
Secret that was recreated since the last run (new UID) either has no node yet
or matches the stale one, which cleanup then deletes. Either way the edge is
missing until a run where the secret already exists.

This hits any secret recreated on every deploy, for example an External
Secrets Operator `ExternalSecret` run as an Argo CD `PreSync` hook with
`creationPolicy: Owner`. Apps that deploy daily never get the edge.

Nothing in `sync_secrets` depends on pods, so reordering is safe.

**Regression check:** did any secret first seen in the last run miss its edge?

```cypher
MATCH (s:KubernetesSecret) WHERE s.firstseen/1000 >= s.lastupdated - 120
OPTIONAL MATCH (p:KubernetesPod)-[:USES_SECRET_ENV|USES_SECRET_VOLUME]->(s)
RETURN split(s.cluster_name,'/')[-1] AS cluster, s.namespace, s.name, count(DISTINCT p) AS pods_using
ORDER BY pods_using, cluster, s.namespace
```

A row with 0 pods is fine for a secret nothing references. If a secret you
know is referenced shows 0, the order regressed.

**Not collected** (upstream gaps, not fixed here):
- secret key names (`KubernetesSecret` has no `data` keys)
- the keys a container consumes (`secretKeyRef.key` is dropped)
- `imagePullSecrets`
- the source of a synced secret (no External Secrets Operator module). The
  only hint is `KubernetesSecret.owner_references` (`kind: ExternalSecret`).

## Cross-module ordering (AWS vs k8s)

- **Nothing gets overwritten.** k8s cleanup only touches `Kubernetes*` data for
  its own cluster, and AWS cleanup only touches AWS data for its own account.
- **AWS must run before k8s**, because k8s links to AWS nodes
  (`(KubernetesNode)-[:IS_INSTANCE]->(EC2Instance)`, EKS `MAPS_TO` → IAM),
  using the same match-or-skip logic as above. Default order: AWS 02:00 →
  k8s 05:00 → ontology 10:00.
- **Unlinked `KubernetesNode`s are expected in two cases:**
  - **Fargate** nodes (`fargate-ip-…`, `instance_id` NULL) never link: there's
    no EC2 instance behind them.
  - **Nodes launched after the AWS sync** (e.g. Karpenter) link after the next
    AWS → k8s cycle.

  ```cypher
  MATCH (n:KubernetesNode) WHERE NOT EXISTS { (n)-[:IS_INSTANCE]->() }
  RETURN split(n.cluster_name,'/')[-1] AS cluster,
         CASE WHEN n.instance_id IS NULL THEN 'fargate/no-instance' ELSE 'ec2' END AS kind,
         count(*) AS unlinked ORDER BY cluster, kind
  ```

## Notes

- **cfn-lint E3601** on the JSONata state machine (`MaxConcurrency`,
  `Arguments`, `Output`, …) is a false positive: the stack deploys and runs.
- **Pinned cartography version:** `Makefile` → `CARTOGRAPHY_GIT_REF`
  (`v0.0.1rc18`). Bump it by hand.
- **Reliance on cartography internals:** the handler copies the sync sequence
  from `start_k8s_ingestion()`. After any bump, diff
  `cartography/intel/kubernetes/__init__.py` against `_run_k8s_sync()` and
  carry over new or reordered steps. **Keep secrets before pods.**

## Files

```
k8s-lambda-loader/
├── README.md
├── template.yaml            # Lambda, IAM, SG, state machine, schedule
├── Makefile                 # trimmed deps + vendored cartography at the pinned fork tag
├── requirements.txt
├── samconfig.example.toml   # copy to samconfig.toml (gitignored)
└── src/handler.py
```
