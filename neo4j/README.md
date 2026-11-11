# Cartography Neo4j Stack

An EC2 instance in a private subnet running Neo4j 5 Community and
`cloudflared` in Docker. Every loader writes to it over Bolt.

## Architecture

- **EC2 instance**, `t3.xlarge` by default (4 vCPU / 16 GiB). The Neo4j
  container has no `--cpus` limit, so a bigger instance is all it takes to
  give Neo4j more CPU.
- **Neo4j 5 Community** in Docker, image pinned by `Neo4jImageTag`.
- **Cloudflare Tunnel** (`cloudflared`) in Docker, for browser access through
  Cloudflare Access (see [Cloudflare Tunnel](#cloudflare-tunnel)).
- **SSM Session Manager** for shell access. No SSH, no bastion.
- **Bolt** on port `8443` with **TLS disabled**
  (`server.bolt.tls_level=DISABLED`): plain Bolt inside the VPC, loaders
  connect with `bolt://<private-ip>:8443`.
- **HTTP browser** on port `7474`.
- **Password auth** for the built-in `neo4j` user (see [Auth](#auth)).
  Network access is also limited by the security group and Cloudflare
  Access.

All configuration is in UserData, which only runs on first boot. A later
`sam deploy` doesn't rewrite `neo4j.conf` or restart containers; change a
running instance over SSM (see [Changing a running instance](#changing-a-running-instance)).

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
`InstanceType`.

## Transaction log retention

`db.tx_log.rotation.retention_policy` is set to `20G size`, not Neo4j's
default of `2 days`.

A time window doesn't bound disk use. Cartography's transaction volume is
spiky: a normal night writes a GB or two of logs, but a night with a large
batch of findings can write tens of GB. Two heavy nights inside a 2-day
window can fill the disk, and a full disk stops Neo4j (every transaction
fails with `TransactionStartFailed`), Docker and the SSM agent. A size cap
keeps log disk use fixed whatever the workload. Pruning runs after each
checkpoint and doesn't affect running ingestion.

The default 100 GB root volume covers the store, 20 GB of logs and
OS/Docker, with room to grow.

## APOC

Neo4j MCP servers and other schema-introspection tools call `apoc.meta.*`.
The Neo4j image ships APOC Core in `/var/lib/neo4j/labs/` but doesn't load
it. UserData handles three requirements:

1. **The jar version must match Neo4j exactly.** UserData extracts the jar
   from the same pinned image (`docker create` / `docker cp`), so they can't
   drift. This is why `Neo4jImageTag` is pinned rather than `5-community`.
2. **The jar must be readable by the container's `neo4j` user (uid 7474).**
   A root-owned jar with mode `0700` makes Neo4j fail at startup with
   `ZipException: Some jar procedure files (...) are invalid`. It looks like a
   corrupt jar but is a permissions error. UserData runs `chown 7474:7474`
   and `chmod 644` on the jar.
3. **`apoc.meta.*` is sandboxed by default.** UserData sets
   `dbms.security.procedures.unrestricted=apoc.meta.*`, only the `meta`
   package rather than all of `apoc.*`.

`apoc.meta.data()` on a full cartography graph can take over a minute.

## Auth

The `neo4j` user's password is an SSM SecureString named by
`Neo4jPasswordParameter` (default `/cartography/neo4j/password`). UserData
reads it at boot and starts the container with
`NEO4J_AUTH=neo4j/<password>`. Every loader reads the same parameter and
connects as `neo4j`.

- Create the parameter before deploying. If it can't be read, UserData
  fails and Neo4j doesn't start.
- Neo4j 5 requires at least 8 characters.
- The image applies `NEO4J_AUTH` only on first start, when `/data` is
  empty. To change the password later, change it in Neo4j
  (`ALTER CURRENT USER SET PASSWORD FROM '<old>' TO '<new>'`), then update
  the SSM parameter so the loaders pick it up on their next run.
- Bolt is plain inside the VPC (TLS disabled), so the password crosses the
  VPC unencrypted between the loaders and Neo4j.

## Cloudflare Tunnel

`cloudflared` gives browser access to Neo4j through Cloudflare Access. It's
outbound only: the Neo4j security group allows 7844 tcp/udp out
(`../network/template.yaml`).

At boot, UserData reads the tunnel token from the SSM SecureString named by
`TunnelTokenParameter` (default `/cartography/cloudflare/tunnel-token`) and
starts `cloudflare/cloudflared:<CloudflaredImageTag>`. The token is never
written to disk. If it can't be read, Neo4j still starts and a warning is
logged to `/var/log/user-data.log`.

The public hostname → origin mapping (e.g. `http://localhost:7474`) is set in
the Cloudflare Zero Trust dashboard (Networks → Tunnels), not in this repo.

## Deploy

Deploy after `../network/`. Create the Neo4j password first, and the tunnel
token if you want browser access:

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
| `AmiId` | Amazon Linux 2023 x86_64. Look it up with `aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64` |
| `InstanceType` | `t3.medium`, `t3.large` or `t3.xlarge` (default) |
| `RootVolumeSize` | GB, default 100 |
| `Neo4jImageTag` | Default `5.26.27-community` |
| `CloudflaredImageTag` | Pinned `cloudflare/cloudflared` tag |
| `TunnelTokenParameter` | SSM name of the tunnel token |
| `Neo4jPasswordParameter` | SSM name of the `neo4j` user's password |
| `BoltAdvertisedAddress` | `host:port` clients use to reach Bolt, e.g. the tunnel hostname |

The security group comes from `/cartography/sg/neo4j`, written by the network
stack.

The stack writes two SSM parameters that the loaders read:

| Parameter | Value |
|---|---|
| `/cartography/neo4j/instance-id` | Instance ID |
| `/cartography/neo4j/private-ip` | Private IP |

### Updating the stack

**Changing `AmiId` or `RootVolumeSize` replaces the instance**, and the
graph lives on the root volume. The instance has
`UpdateReplacePolicy: Retain` and `DeletionPolicy: Retain`, so a replacement
keeps the old instance instead of terminating it, but the new one starts
with an empty graph. Always preview:

```sh
sam deploy --no-execute-changeset
```

and refuse any change set that shows Replacement on `Neo4jInstance`.

## Changing a running instance

Use SSM Run Command, outside your sync window, with no Step Functions
executions running. Back up `neo4j.conf` first. For example, to change
memory:

```sh
INSTANCE_ID=$(aws ssm get-parameter --name /cartography/neo4j/instance-id \
  --query Parameter.Value --output text)

aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name AWS-RunShellScript \
  --parameters '{"commands":[
    "cp -a /neo4j-data/conf/neo4j.conf /neo4j-data/conf/neo4j.conf.bak-$(date -u +%Y%m%d-%H%M%S)",
    "sed -i s/^server.memory.heap.initial_size=.*/server.memory.heap.initial_size=5g/ /neo4j-data/conf/neo4j.conf",
    "sed -i s/^server.memory.heap.max_size=.*/server.memory.heap.max_size=5g/ /neo4j-data/conf/neo4j.conf",
    "sed -i s/^server.memory.pagecache.size=.*/server.memory.pagecache.size=6400m/ /neo4j-data/conf/neo4j.conf",
    "docker restart neo4j"
  ]}'
```

A restart takes under a minute. Update `template.yaml` to match, so a rebuild
keeps the change.

## Verify

```cypher
SHOW SETTINGS YIELD name, value
WHERE name STARTS WITH 'server.memory' OR name = 'db.tx_log.rotation.retention_policy'
RETURN name, value;

RETURN apoc.version();
CALL apoc.meta.data() YIELD label RETURN count(*) AS labelCount;
```

## TODO

- **Disk alarm.** A CloudWatch alarm at about 80% root volume use would catch
  a filling disk early. EC2 doesn't publish disk use by default; this needs
  the CloudWatch agent on the instance, or a scheduled check.
