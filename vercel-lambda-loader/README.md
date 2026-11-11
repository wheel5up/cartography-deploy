# Cartography Vercel Lambda Loader

Syncs one Vercel team into Neo4j by calling cartography's Python API from a
Lambda function. Two Step Functions state machines drive it, the same shape
as `../okta-lambda-loader`.

## Architecture

- **`cartography-vercel-lambda-sync`**: one execution is one Lambda
  invocation, given an explicit step list and, optionally, a shared
  `update_tag` to continue an existing run.
- **`cartography-vercel-lambda-orchestrator`**: runs a fixed list of splits
  from SSM (`/cartography/vercel-lambda/default-splits`), one sync execution
  per split, in order (Map, `MaxConcurrency: 1`). It generates one
  `update_tag` up front and passes it to every split. The daily schedule
  triggers the orchestrator.

The default splits list has **one entry** containing every step: the whole
sync fits in one 900s invocation. A team with ~70 projects and ~40k
deployments takes about 6 minutes. If it outgrows that, add more
`{"steps": [...]}` entries to the SSM parameter; no template or handler
change is needed. The per-project loop (`project_resources`) is the step most
likely to need splitting first, since it scales with project count and
deployment volume.

### Steps

Each `cartography.intel.vercel.<module>.sync()` call is its own step in
`handler.py`, in `start_vercel_ingestion()`'s order:

```
teams, users, projects, domains, edge_configs, shared_env_vars, integrations,
access_groups, webhooks, log_drains, secure_compute_networks, aliases,
project_resources, domain_resources, edge_config_resources, label_migration
```

`project_resources`, `domain_resources` and `edge_config_resources` each loop
over every project, domain or edge config in one step. See the
`handler.py` docstring for details.

Every step calls cartography's public `sync()` functions, unmodified. There's
no patching and no use of private functions.

## Configuration

SSM parameters (create before deploying):

| Parameter | Type | Value |
|---|---|---|
| `/cartography/vercel/token` | SecureString | Vercel API token, read at invocation time |
| `/cartography/vercel/team-slug` | String | The team's **ID** (`team_...`), not its slug. Resolved at deploy time |

**Use the real team ID, not the slug.** Vercel's read endpoints accept
either, so the sync appears to work with the slug. But every Vercel node's
`RESOURCE` relationship matches `(:VercelTeam {id: $TEAM_ID})`, and
`VercelTeam.id` is always the real ID from the API. With the slug, no node
gets the `RESOURCE` edge, and since cleanup is scoped through that edge,
stale nodes are never deleted. The parameter name is historical.

The token stays out of the Lambda's environment variables (they're readable
with `lambda:GetFunctionConfiguration`); only the parameter name is in
`VERCEL_TOKEN_SSM_PARAM`. The team ID isn't sensitive, so it's a deploy-time
`{{resolve:ssm:...}}` reference. If you change it, run `sam build && sam
deploy` again to pick it up.

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
  --input '{"splits": [{"steps": ["aliases"]}]}'

# One split, bypassing the orchestrator
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> \
  --input '{"steps": ["teams"]}'

# Result
aws stepfunctions describe-execution --execution-arn <arn> --query '[status,input,output]'

# Nested executions from an orchestrator run
aws stepfunctions list-executions --state-machine-arn <StateMachineArn> \
  --query 'executions[?starts_with(name, `vercel-orch-`)].[name,status]'

# Logs
aws logs tail /cartography/vercel-lambda-loader --since 5m --follow

# Invoke the Lambda directly
aws lambda invoke --function-name cartography-vercel-lambda-loader \
  --payload '{"steps":["teams"]}' --cli-binary-format raw-in-base64-out out.json
```

## Schedule

Daily at 06:00 UTC by default (`/cartography/vercel-lambda/schedule`). The
schedule is created **`State: DISABLED`** in `template.yaml`. After a manual
run succeeds, change it to `ENABLED` and redeploy.

To change the time or the splits:

```sh
aws ssm put-parameter --overwrite --name /cartography/vercel-lambda/schedule --value 'cron(0 7 * * ? *)'
aws ssm put-parameter --overwrite --name /cartography/vercel-lambda/default-splits --value '[...]'
```

CloudFormation only seeds these on stack create, so later deploys don't
overwrite them. The splits list is read at run time. The schedule is resolved
at deploy time (EventBridge can't read SSM), so run `sam build && sam deploy`
after changing it.

## Packaging

A trimmed `requirements.txt` (`boto3`, `neo4j`, `requests`, `urllib3`,
`backoff`, `statsd`) plus a vendored copy of the `cartography/` source tree
from the pinned fork tag (`CARTOGRAPHY_GIT_REF` in the `Makefile`), not a
`pip install cartography`. `requests` and `urllib3` are listed explicitly
because `cartography.intel.vercel` imports them directly.

## Known gaps

- **No `SyncDuration`/`SyncSuccess` metrics** from the handler.
- **No `ModuleSyncMetadata` node.** `start_vercel_ingestion()` never calls
  `merge_module_sync_metadata()`, so cartography writes no sync receipt for
  Vercel. This is an upstream gap; the handler doesn't invent one.
- **One team per stack.** `start_vercel_ingestion()` syncs a single team.
- **The team ID is resolved at deploy time.** Reading it at run time,
  alongside the token, would avoid the redeploy when it changes.

## Files

```
vercel-lambda-loader/
├── README.md
├── template.yaml            # Lambda, IAM, SG, sync + orchestrator state machines, schedule (DISABLED)
├── Makefile                 # trimmed deps + vendored cartography at the pinned fork tag
├── requirements.txt
├── samconfig.example.toml   # copy to samconfig.toml (gitignored)
└── src/handler.py
```
