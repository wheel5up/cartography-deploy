# Cartography Neo4j Stack

One EC2 instance in a private subnet running Neo4j 5 Community and
`cloudflared` as ECS services. Every loader writes to it over Bolt.

## Architecture

- **EC2 instance**, ECS-optimized Amazon Linux 2023, `t3.xlarge` by default
  (4 vCPU / 16 GiB). It's the only container instance in the
  `cartography-neo4j` ECS cluster.
- **ECS service `neo4j`**: Neo4j 5 Community, image pinned by
  `Neo4jImageTag`. No CPU limit, so a bigger instance is all it takes to give
  Neo4j more CPU.
- **ECS service `cloudflared`**: Cloudflare Tunnel, for browser access through
  Cloudflare Access (see [Cloudflare Tunnel](#cloudflare-tunnel)). A separate
  service, so upgrading it doesn't restart Neo4j.
- **Host networking**: both containers use the instance's own network. Neo4j
  listens on the instance's private IP, so the security group and the
  loaders' `bolt://<private-ip>:8443` work as with any host process, and
  `cloudflared` reaches Neo4j on `localhost`.
- **SSM Session Manager** for shell access. No SSH, no bastion.
- **Bolt** on port `8443` with **TLS disabled**
  (`server.bolt.tls_level=DISABLED`): plain Bolt inside the VPC, loaders
  connect with `bolt://<private-ip>:8443`.
- **HTTP browser** on port `7474`.
- **Password auth** for the built-in `neo4j` user (see [Auth](#auth)).
  Network access is also limited by the security group and Cloudflare
  Access.
- **Logs** in CloudWatch, log group `/cartography/neo4j` (stream prefixes
  `neo4j/` and `cloudflared/`), 30-day retention. Neo4j also writes its own
  logs to `/neo4j-data/logs` on the instance.

## Configuration

All Neo4j and `cloudflared` configuration is in the ECS task definitions in
`template.yaml`. UserData only joins the ECS cluster and creates
`/neo4j-data/data` and `/neo4j-data/logs`.

Neo4j settings are `NEO4J_*` environment variables. The image's entrypoint
writes each one into `neo4j.conf` at container start, overriding any default.
The name maps to the setting by replacing `.` with `_` and `_` with `__`:

| Environment variable | Setting |
|---|---|
| `NEO4J_server_bolt_advertised__address` | `server.bolt.advertised_address` |
| `NEO4J_db_tx__log_rotation_retention__policy` | `db.tx_log.rotation.retention_policy` |

To change a setting or an image tag, edit `template.yaml` or `samconfig.toml`
and `sam deploy`. ECS registers a new task definition revision, stops the old
container and starts the new one on the same instance. The graph is on the
instance's root volume, so it survives the restart. Neo4j is unavailable for
the restart, usually under a minute. A deployment that fails to start rolls
back to the previous revision.

The instance isn't touched by these changes. Only `AmiId`, `RootVolumeSize`,
`InstanceType`, `SubnetId` and UserData changes affect it (see
[Updating the stack](#updating-the-stack)).

## Memory

From `neo4j-admin server memory-recommendation --memory=15g --docker` for a
16 GiB host:

| Setting | Value |
|---|---|
| `server.memory.heap.initial_size` | `5g` |
| `server.memory.heap.max_size` | `5g` |
| `server.memory.pagecache.size` | `6400m` |
| `server.jvm.additional` | `-XX:+ExitOnOutOfMemoryError` |

Size the page cache to hold as much of the store as you can; a small page
cache serves most reads from disk. Re-run the recommendation if you change
`InstanceType`, and adjust the container's `MemoryReservation` to match.

## Transaction log retention

`db.tx_log.rotation.retention_policy` is set to `20G size`, not Neo4j's
default of `2 days`.

A time window doesn't bound disk use. Cartography's transaction volume is
spiky: a normal night writes a GB or two of logs, but a night with a large
batch of findings can write tens of GB. Two heavy nights inside a 2-day
window can fill the disk, and a full disk stops Neo4j (every transaction
fails with `TransactionStartFailed`), Docker, the ECS agent and the SSM
agent. A size cap keeps log disk use fixed whatever the workload. Pruning
runs after each checkpoint and doesn't affect running ingestion.

The default 100 GB root volume covers the store, 20 GB of logs and
OS/Docker, with room to grow.

## APOC

Neo4j MCP servers and other schema-introspection tools call `apoc.meta.*`.
The Neo4j image ships APOC Core in `/var/lib/neo4j/labs/` but doesn't load
it by default.

- `NEO4J_PLUGINS='["apoc"]'` makes the entrypoint copy that jar into the
  plugins directory at container start. It uses the jar from the image, with
  no download, so APOC always matches the Neo4j version. This is why
  `Neo4jImageTag` is pinned rather than `5-community`.
- **`apoc.meta.*` is sandboxed by default.** The task sets
  `dbms.security.procedures.unrestricted=apoc.meta.*`, only the `meta`
  package rather than all of `apoc.*`. This overrides the image's APOC
  default of `apoc.*`.

`apoc.meta.data()` on a full cartography graph can take over a minute.

## Auth

The `neo4j` user's password is an SSM SecureString named by
`Neo4jPasswordParameter` (default `/cartography/neo4j/password`). ECS reads it
into the container as `DB_PASSWORD`, and the container's command builds
`NEO4J_AUTH=neo4j/<password>` from it before running the image's entrypoint.
Every loader reads the same parameter and connects as `neo4j`.

- Create the parameter before deploying. If ECS can't read it, the task
  doesn't start.
- Neo4j 5 requires at least 8 characters.
- The image applies `NEO4J_AUTH` only when `/data` is empty. To change the
  password later, change it in Neo4j
  (`ALTER CURRENT USER SET PASSWORD FROM '<old>' TO '<new>'`), then update
  the SSM parameter so the loaders pick it up on their next run. A container
  restart doesn't reset it.
- Bolt is plain inside the VPC (TLS disabled), so the password crosses the
  VPC unencrypted between the loaders and Neo4j.

## Cloudflare Tunnel

`cloudflared` gives browser access to Neo4j through Cloudflare Access. It's
outbound only: the Neo4j security group allows 7844 tcp/udp out
(`../network/template.yaml`).

ECS reads the tunnel token from the SSM SecureString named by
`TunnelTokenParameter` (default `/cartography/cloudflare/tunnel-token`) into
`TUNNEL_TOKEN`, and the container runs `cloudflared tunnel run`. The token
isn't written to disk or into the task definition. Create it before
deploying; without it the `cloudflared` service can't start (Neo4j is
unaffected).

The public hostnames and private network routes are set in the Cloudflare
Zero Trust dashboard (Networks → Tunnels), not in this repo. With host
networking, origins are on `localhost`:

| Public hostname | Service |
|---|---|
| Browser, e.g. `carto.g2.com` | `http://localhost:7474` |
| Bolt, e.g. `carto-bolt.g2.com` | `tcp://localhost:8443` |

Set `BoltAdvertisedAddress` to the Bolt hostname and port (e.g.
`carto-bolt.g2.com:8443`). The browser's discovery endpoint on 7474 returns
it, and the browser connects there for Bolt. With a private network route for
the VPC CIDR, WARP users can also connect to `bolt://<private-ip>:8443`
directly.

## Deploy

Deploy after `../network/`. Create the Neo4j password and the tunnel token
first:

```sh
aws ssm put-parameter --type SecureString \
  --name /cartography/neo4j/password --value '<password>'
aws ssm put-parameter --type SecureString \
  --name /cartography/cloudflare/tunnel-token --value '<token>'
```

Then:

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build
sam deploy
```

| Parameter | Notes |
|---|---|
| `SubnetId` | Private subnet with NAT |
| `AmiId` | ECS-optimized Amazon Linux 2023 x86_64. Look it up with `aws ssm get-parameter --name /aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id` |
| `InstanceType` | `t3.medium`, `t3.large` or `t3.xlarge` (default) |
| `RootVolumeSize` | GB, default 100 |
| `Neo4jImageTag` | Default `5.26.27-community` |
| `CloudflaredImageTag` | Pinned `cloudflare/cloudflared` tag |
| `TunnelTokenParameter` | SSM name of the tunnel token |
| `Neo4jPasswordParameter` | SSM name of the `neo4j` user's password |
| `BoltAdvertisedAddress` | `host:port` the browser uses for Bolt, e.g. the tunnel hostname |

The security group comes from `/cartography/sg/neo4j`, written by the network
stack.

The stack writes two SSM parameters that the loaders read:

| Parameter | Value |
|---|---|
| `/cartography/neo4j/instance-id` | Instance ID |
| `/cartography/neo4j/private-ip` | Private IP |

### Updating the stack

**Changing `AmiId`, `RootVolumeSize` or `SubnetId` replaces the instance**,
and the graph lives on the root volume. The instance has
`UpdateReplacePolicy: Retain` and `DeletionPolicy: Retain`, so a replacement
keeps the old instance instead of terminating it, but the new one starts
with an empty graph and a new private IP. The ECS services are pinned to the
current instance, so the retained one gets no new tasks, but its running
containers keep going until you terminate it by hand. Until then, its
`cloudflared` stays connected to the same tunnel.

Changing `InstanceType` or UserData stops and starts the instance (graph and
private IP kept).

Always preview:

```sh
sam deploy --no-execute-changeset
```

and refuse any change set that shows Replacement on `Neo4jInstance` unless
you mean it.

The ECS-optimized AMI gets updates (ECS agent, OS patches). Moving to a new
one is an instance replacement, so plan it like one.

## Operations

```sh
CLUSTER=cartography-neo4j
INSTANCE_ID=$(aws ssm get-parameter --name /cartography/neo4j/instance-id \
  --query Parameter.Value --output text)

# Service status
aws ecs describe-services --cluster $CLUSTER --services neo4j cloudflared \
  --query 'services[].[serviceName,runningCount,deployments[0].rolloutState]'

# Restart a service (same task definition)
aws ecs update-service --cluster $CLUSTER --service neo4j --force-new-deployment

# Logs
aws logs tail /cartography/neo4j --log-stream-name-prefix neo4j --since 15m --follow

# Shell on the instance
aws ssm start-session --target $INSTANCE_ID
```

Restart outside your sync window, with no Step Functions executions running.

## Verify

```cypher
SHOW SETTINGS YIELD name, value
WHERE name STARTS WITH 'server.memory'
   OR name IN ['db.tx_log.rotation.retention_policy', 'server.bolt.advertised_address']
RETURN name, value;

RETURN apoc.version();
CALL apoc.meta.data() YIELD label RETURN count(*) AS labelCount;
```

The browser discovery endpoint should return the Bolt hostname:

```sh
curl -s http://<private-ip>:7474/ | jq .bolt_direct
```

## TODO

- **Disk alarm.** A CloudWatch alarm at about 80% root volume use would catch
  a filling disk early. EC2 doesn't publish disk use by default; this needs
  the CloudWatch agent on the instance, or a scheduled check.
