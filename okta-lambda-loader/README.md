# Cartography Okta Lambda Loader

Syncs one Okta org into Neo4j by calling cartography's Python API from a
Lambda function. A full Okta sync doesn't fit in one 900s invocation, so it's
broken into **steps** (cartography's own sub-sync functions, in
`start_okta_ingestion()`'s order). The one step too big for a single
invocation, `applications`, is further broken into page-range **batches**.

## Architecture

- **`cartography-okta-lambda-sync`**: one execution is one Lambda
  invocation, given an explicit step list and, optionally, a shared
  `update_tag` to continue an existing run.
- **`cartography-okta-lambda-orchestrator`**: runs a fixed list of splits
  from SSM (`/cartography/okta-lambda/default-splits`), one sync execution
  per split, in order (Map, `MaxConcurrency: 1`). It generates one
  `update_tag` up front and passes it to every split. The daily schedule
  triggers the orchestrator.

### Default splits

| Split | Steps |
|---|---|
| 1 | `organization`, `users`, `groups`, `user_types`, `factors`, `log_streams`, `coverage` |
| 2–5 | `applications:0:100`, `applications:100:200`, `applications:200:300`, `applications:300:400` (one per split) |
| 6 | `applications:cleanup`, `origins`, `authenticators`, `awssaml` |
| 7 | `devices`, `network_zones`, `api_tokens`, `policies` |
| 8 | `sync_complete` |

The splits are sized by hand from measured timing. For an org with ~1,300
users, ~750 groups and ~370 apps, `applications` alone takes about 1,170s
serially, and the whole run takes 25–30 minutes. Size your own splits by
running steps on their own and timing them.

Ordering constraints:
- `coverage` must run in the **same invocation** as, and after,
  `organization`, `users`, `groups` and `log_streams`. It uses their return
  values, which aren't stored anywhere. The handler fails if they're missing.
- `network_zones` before `api_tokens` and `policies`; `applications:cleanup`
  before `policies`.
- `sync_complete` last, in its own split.

## Design

### All Okta API calls are serial

Okta rate-limits per API token (by default a token gets 50% of a bucket's
org-wide limit). Fetching group or app details concurrently makes things
worse: even 2 concurrent requests can trigger `429`s, and the Okta SDK's
retry budget (2 attempts by default) can't recover, so the run stalls with no
error. The SDK's own rate-limit log line is also disabled by default
(`okta/logger.py` sets `logging.getLogger("okta-sdk-python").disabled = True`
on every `OktaClient` construction), so the stall is invisible in the logs.

Concurrency doesn't help when the bottleneck is a per-token rate limit. The
handler keeps cartography's serial behavior and spreads the work across
invocations instead.

### Why fixed splits, not a runtime time-budget loop

A Lambda that checks `get_remaining_time_in_millis()` between steps and hands
the rest back to a looping state machine doesn't protect against a single
step running long, which is the actual risk. It also makes the number and
contents of invocations vary from run to run. A fixed list read up front
makes every split a separately named execution with its input visible in
`describe-execution`.

### Invocation contract (`cartography-okta-lambda-sync`)

```json
{
  "steps": ["users", "groups", "applications:0:100"],
  "update_tag": 1792017089
}
```

Returns:

```json
{
  "status": "SUCCEEDED" | "FAILED",
  "completed": ["users", "groups"],
  "remaining": [],
  "updateTag": 1792017089,
  "durationSeconds": 123.4,
  "error": "..."
}
```

Omit `update_tag` to start a new run; the Lambda generates one and returns
it. On `FAILED`, `remaining` lists the steps that never ran. It's for
diagnostics only: retry a failed run from the start with a fresh tag.

### One shared `update_tag` per run

`start_okta_ingestion()` uses one `update_tag` for every sub-sync. A legacy
cleanup (`cleanup_okta_groups`) deletes every `OktaGroup` not stamped with the
tag it's given. If each split used its own tag, the last split would delete
every group the earlier splits wrote.

The orchestrator computes the tag (`$floor($millis() / 1000)`) once, before
the Map, as an outer-scope variable that every iteration reads. A variable
assigned inside one Map iteration isn't visible to the next, so the tag can't
be threaded from one split's result to the next.

### `applications`: batches, a separate cleanup, and a coverage check

Okta has no bulk endpoint for group membership or app assignments, so these
are fetched per item. `applications` is split into ranges of the app list
(`applications:<start>:<end>`). Okta pagination is cursor-based, with no
offset, so each batch lists every app and slices out its range. Listing is
cheap (about 11s for ~370 apps) compared with the per-app calls.

Upstream `sync_okta_applications()` writes and cleans up in one call. Calling
it per batch would make each batch delete the apps the other batches wrote.
The handler calls cartography's lower-level (private, unmodified)
transform and load functions per batch, and runs cleanup only in the
separate `applications:cleanup` step.

Fixed batch boundaries stop covering the org once it grows: with the default
four batches, apps past 400 would silently never be synced. Before
`applications:cleanup` deletes anything, the handler compares Okta's current
app count with the number of `OktaApplication` nodes stamped with this run's
tag. On a mismatch it fails with the missing count and the
`applications:<n>:<m>` range to add. See `_verify_applications_batch_coverage`
in `src/handler.py`.

### Failures stop the run

If any batch fails, cleanup never runs. The orchestrator stops at the first
failed split with a real `Fail` state, so a partial sync can't end green.

### `sync_complete`

`start_okta_ingestion()` ends by writing a `ModuleSyncMetadata` node
(`merge_module_sync_metadata()`), which tooling uses to answer "when did Okta
last sync?". `sync_complete` writes it, in its own split, after every other
split succeeded.

### `awssaml` is skipped unless `OKTA_SAML_ROLE_REGEX` is set

`cartography.intel.okta.awssaml._parse_regex()` calls
`mapping_regex.replace(...)` without a `None` check, so it crashes with
`AttributeError: 'NoneType' object has no attribute 'replace'` when no regex
is configured. The cartography CLI doesn't hit this because
`--okta-saml-role-regex` has a default
(`^aws\#\S+\#(?{{role}}[\w\-]+)\#(?{{accountid}}\d+)$`); a direct API call
has no default. The handler skips `awssaml` with a log line when the env var
is unset. If your Okta groups use the AWS Account Federation naming format,
set `OKTA_SAML_ROLE_REGEX` in `template.yaml`.

### Admin role lookups

Per-user and per-group role lookups fail fast with Okta error `E0000006`
unless the API token can read admin roles. Cartography catches this and moves
on, and the `coverage` step records whether roles and log streams were
readable, so "no permission" is distinguishable from "none exist". If you
grant the token that access, re-time split 1: those lookups run per item.

### Private cartography functions

Every step calls a public cartography function except the `applications`
batch and cleanup steps, `policies` (re-lists apps with
`_get_okta_applications`, since apps are batched across invocations), and
`sync_complete`. These are documented in `src/handler.py`. Check them after
bumping cartography.

## Configuration

| Item | Where |
|---|---|
| Okta API token | SSM `/cartography/okta/token` (SecureString), read at invocation time |
| Org subdomain | `OktaOrgId` parameter (`my-org` for `my-org.okta.com`) |
| VPC and subnet | `VpcId`, `SubnetId` parameters |

The token stays out of the Lambda's environment variables (they're readable
with `lambda:GetFunctionConfiguration`); only the parameter name is in
`OKTA_TOKEN_SSM_PARAM`.

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
  --input '{"splits": [{"steps": ["applications:100:200"]}]}'

# One split, bypassing the orchestrator
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> \
  --input '{"steps": ["organization"]}'

# Result
aws stepfunctions describe-execution --execution-arn <arn> --query '[status,input,output]'

# Nested executions from an orchestrator run
aws stepfunctions list-executions --state-machine-arn <StateMachineArn> \
  --query 'executions[?starts_with(name, `okta-orch-`)].[name,status]'

# Logs
aws logs tail /cartography/okta-lambda-loader --since 5m --follow

# Invoke the Lambda directly
aws lambda invoke --function-name cartography-okta-lambda-loader \
  --payload '{"steps":["organization"]}' --cli-binary-format raw-in-base64-out out.json
```

## Schedule

Daily at 03:00 UTC by default (`/cartography/okta-lambda/schedule`). To
change the time or the splits:

```sh
aws ssm put-parameter --overwrite --name /cartography/okta-lambda/schedule --value 'cron(0 4 * * ? *)'
aws ssm put-parameter --overwrite --name /cartography/okta-lambda/default-splits --value '[...]'
```

CloudFormation only seeds these on stack create, so later deploys don't
overwrite them. The splits list is read at run time. The schedule is resolved
at deploy time (EventBridge can't read SSM), so run `sam build && sam deploy`
after changing it.

## Packaging

A trimmed `requirements.txt` (`boto3`, `neo4j`, `okta`, `backoff`, `statsd`)
plus a vendored copy of the whole `cartography/` source tree (~17 MB) from
the pinned fork tag (`CARTOGRAPHY_GIT_REF` in the `Makefile`), not a
`pip install cartography`.

- **Whole package, not just `intel/okta`:** `cartography.intel.okta`
  transitively imports `cartography.util`, `client.core.tx`, `graph.*`,
  `stats`, `models.core.*`, `config`, `sync`, and the cleanup job JSON files
  (via `importlib.resources`). A hand-picked subset breaks on the next
  transitive import.
- **`boto3` with no AWS calls:** `cartography/util.py` imports
  `boto3`/`botocore` at module level for shared helpers.

## Known gaps

- **No `SyncDuration`/`SyncSuccess` metrics** from the handler.
- **The `awssaml` `None`-regex bug** is worked around here, not fixed
  upstream.

## Files

```
okta-lambda-loader/
├── README.md
├── template.yaml            # Lambda, IAM, SG, sync + orchestrator state machines, schedule
├── Makefile                 # trimmed deps + vendored cartography at the pinned fork tag
├── requirements.txt
├── samconfig.example.toml   # copy to samconfig.toml (gitignored)
└── src/handler.py           # step/batch dispatch, shared update_tag, coverage check
```
