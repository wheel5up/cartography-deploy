# Cartography GCP Lambda Loader

Syncs one or more GCP organizations into Neo4j. One Lambda invocation = one
org (or one project batch of a large org).

| Piece | Name |
|---|---|
| Lambda | `cartography-gcp-lambda-loader` (`src/handler.py`) |
| Sync state machine | `cartography-gcp-lambda-sync` (one execution = one org) |
| Orchestrator | `cartography-gcp-lambda-orchestrator` (all splits from SSM, serial, one update_tag) |
| Splits | SSM `/cartography/gcp-lambda/default-splits` |
| Schedule | `cartography-gcp-lambda-daily`, 01:00 UTC (`ScheduleState`, default `ENABLED`) |
| GCP identity | `../gcp/` (Terraform): a GCP project, SA `cartography-reader`, WIF pool `aws-cartography` |

## Auth

No keys or secrets. Workload Identity Federation: the Lambda's role
(`cartography-gcp-lambda-loader-role-<region>`, name is load-bearing, see
`../gcp/main.tf`) signs an AWS STS GetCallerIdentity request, GCP STS exchanges
it, and the Lambda impersonates `GcpServiceAccount`
(`cartography-reader@<project>.iam.gserviceaccount.com`).

Neo4j is the exception: the Lambda reads the `neo4j` user's password from SSM
`/cartography/neo4j/password` at invocation time (see `../neo4j/README.md`).

## Splits

```json
[{"org_id": "123456789012", "name": "example.com"}, ...]
```

Optional `"requested_syncs": "iam,policy_bindings,permission_relationships,compute"`
limits an entry to those resource types, so a large org can be split into
several entries. Keep `iam`, `policy_bindings`, `permission_relationships` and
`compute` together. A failed split does not stop the others; the orchestrator
fails at the end if any split failed.

A large org is split into project batches (`"project_batch_index"` /
`"project_batch_count"`, every index present). A project too slow to share a
batch gets its own split, and every other split of that org excludes it:

```json
{"org_id": "234567890123", "name": "example.org/big-project", "include_projects": ["big-project"]},
{"org_id": "234567890123", "name": "example.org/batch-0", "exclude_projects": ["big-project"], "project_batch_index": 0, "project_batch_count": 8},
...
```

List isolated projects first (failures show up early). Finalize is skipped
unless the org's exclude lists are identical and equal the union of its
include lists, so a project can't silently drop out.

Each split is safe in isolation: cleanup is scoped per org and per project,
and the handler refuses to run if the org isn't visible (lost grant), so
existing data is preserved.

The live splits are in SSM. Keep the source of truth in
`default-splits.json` (gitignored; start from `default-splits.example.json`),
then push it:

```bash
aws ssm put-parameter --overwrite \
  --name /cartography/gcp-lambda/default-splits \
  --value file://default-splits.json
```

One-off run with the file (without changing SSM):

```bash
aws stepfunctions start-execution \
  --state-machine-arn <OrchestratorStateMachineArn> \
  --input "$(jq -c '{splits: .}' default-splits.json)"
```

## Deploy

Deploy `../gcp/` (Terraform) first; its outputs give `GcpServiceAccount`
(`service_account_email`) and `GcpWifAudience` (`lambda_credential_config`
`.audience`).

```bash
cd gcp-lambda-loader
cp samconfig.example.toml samconfig.toml   # then edit
sam build --use-container
sam deploy
```

`DefaultSplitsJson` only seeds the SSM parameter on stack create.

## Test

```bash
# One org
aws stepfunctions start-execution \
  --state-machine-arn <StateMachineArn> \
  --input '{"org_id": "123456789012"}'

# All orgs
aws stepfunctions start-execution \
  --state-machine-arn <OrchestratorStateMachineArn> \
  --input '{}'
```

Local: leave `GCP_WIF_AUDIENCE` unset and set `GCP_SERVICE_ACCOUNT`; the
handler falls back to ADC plus impersonation.
