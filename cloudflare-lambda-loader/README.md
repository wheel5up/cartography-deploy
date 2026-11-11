# Cartography Cloudflare Lambda Loader

Syncs every Cloudflare account an API token can see into Neo4j by calling
cartography's Python API from a Lambda function. Two Step Functions state
machines drive it, the same shape as `../vercel-lambda-loader`.

## Architecture

- **`cartography-cloudflare-lambda-sync`**: one execution is one Lambda
  invocation, given an explicit step list and, optionally, a shared
  `update_tag` to continue an existing run.
- **`cartography-cloudflare-lambda-orchestrator`**: runs a fixed list of
  splits from SSM (`/cartography/cloudflare-lambda/default-splits`), one sync
  execution per split, in order (Map, `MaxConcurrency: 1`). It generates one
  `update_tag` up front and passes it to every split. The daily schedule
  triggers the orchestrator.

The default splits list has **one entry** containing every step: the whole
sync fits in one 900s invocation. For one small org (1 account, a handful of
zones, ~1,200 DNS records), nightly runs took 25–85 seconds. If it outgrows
that, add more `{"steps": [...]}` entries to the SSM parameter; no template
or handler change is needed.

### Steps

Each `cartography.intel.cloudflare.<module>.sync()` call is its own step in
`handler.py`, in `start_cloudflare_ingestion()`'s order:

```
accounts, roles, members, dnsrecord_migration, zones, dnsrecords,
r2buckets, workerscripts, workerroutes, rulesets
```

Upstream loops over every account and runs the per-account syncs inside that
loop. The handler flattens it: `accounts` syncs the accounts once, and every
later step loops over them itself. If a step needs accounts or zones that
weren't synced earlier in the same invocation, it re-runs that (idempotent)
sync first, so any step can run on its own.

Every step calls cartography's public `sync()` functions, unmodified. There's
no patching and no use of private functions.

## Configuration

One SSM parameter (create before the first run):

| Parameter | Type | Value |
|---|---|---|
| `/cartography/cloudflare/token` | SecureString | Cloudflare API token, read at invocation time |

The token needs read access to the resources cartography syncs. Cloudflare's
`Read all resources` template works; see cartography's Cloudflare module docs
for the individual scopes.

The token stays out of the Lambda's environment variables (they're readable
with `lambda:GetFunctionConfiguration`); only the parameter name is in
`CLOUDFLARE_TOKEN_SSM_PARAM`. There's no account ID to configure: the token
walks every account it can see.

## Deploy

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build --use-container   # needed on non-x86_64 machines (native wheels)
sam deploy
```

The stack creates its own security group and adds one ingress rule (Bolt
8443) to the Neo4j security group.

## Run manually

```sh
# Full default run, through the orchestrator
aws stepfunctions start-execution --state-machine-arn <OrchestratorStateMachineArn> --input '{}'

# Explicit splits
aws stepfunctions start-execution --state-machine-arn <OrchestratorStateMachineArn> \
  --input '{"splits": [{"steps": ["accounts", "zones"]}]}'

# One split, bypassing the orchestrator
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> \
  --input '{"steps": ["r2buckets"]}'

# Result
aws stepfunctions describe-execution --execution-arn <arn> --query '[status,input,output]'

# Nested executions from an orchestrator run
aws stepfunctions list-executions --state-machine-arn <StateMachineArn> \
  --query 'executions[?starts_with(name, `cloudflare-orch-`)].[name,status]'

# Logs
aws logs tail /cartography/cloudflare-lambda-loader --since 5m --follow

# Invoke the Lambda directly
aws lambda invoke --function-name cartography-cloudflare-lambda-loader \
  --payload '{"steps":["accounts"]}' --cli-binary-format raw-in-base64-out out.json
```

The Lambda returns `status`, `completed`, `remaining`, `updateTag` and
`durationSeconds` (plus `error` on failure). A failed step stops the
invocation; `remaining` lists the steps that didn't run.

## Schedule

Daily at 00:00 UTC by default (`/cartography/cloudflare-lambda/schedule`).
The schedule is created `State: ENABLED` in `template.yaml`.

To change the time or the splits:

```sh
aws ssm put-parameter --overwrite --name /cartography/cloudflare-lambda/schedule --value 'cron(0 1 * * ? *)'
aws ssm put-parameter --overwrite --name /cartography/cloudflare-lambda/default-splits --value '[...]'
```

CloudFormation only seeds these on stack create, so later deploys don't
overwrite them. The splits list is read at run time. The schedule is resolved
at deploy time (EventBridge can't read SSM), so run `sam build && sam deploy`
after changing it.

## Packaging

A trimmed `requirements.txt` plus a vendored copy of the `cartography/`
source tree from the pinned fork tag (`CARTOGRAPHY_GIT_REF` in the
`Makefile`), not a `pip install cartography`.

Unlike the other loaders, `requirements.txt` uses **exact pins** (`==`).
cloudflare-python 5.8.0 renamed a keyword argument that cartography's R2 sync
passes, which crashed the sync; `cloudflare==5.7.0` avoids it. Check the SDK's
changelog before relaxing the pins. `boto3`/`botocore`, `backoff` and `statsd`
are listed because cartography's shared modules (`cartography.util`,
`cartography.client.core.tx`, `cartography.stats`) import them.

## Known gaps

- **No `SyncDuration`/`SyncSuccess` metrics** from the handler.
- **No `ModuleSyncMetadata` node.** `start_cloudflare_ingestion()` never calls
  `merge_module_sync_metadata()`, so cartography writes no sync receipt for
  Cloudflare. This is an upstream gap; the handler doesn't invent one.

## Files

```
cloudflare-lambda-loader/
├── README.md
├── template.yaml            # Lambda, IAM, SG, sync + orchestrator state machines, schedule
├── Makefile                 # exact-pinned deps + vendored cartography at the pinned fork tag
├── requirements.txt
├── samconfig.example.toml   # copy to samconfig.toml (gitignored)
└── src/handler.py
```
