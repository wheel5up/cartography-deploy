# Cartography Lambda Loader (AWS)

Syncs every account in an AWS Organization into Neo4j with Cartography's AWS
modules, one Lambda invocation per account (or per split of an account).

Two Step Functions state machines:

| State machine | What it does |
|---|---|
| `cartography-lambda-sync` | Syncs one account: validates input, invokes the Lambda once, fails the execution if the sync failed |
| `cartography-lambda-orchestrator` | Lists every ACTIVE org account and runs `cartography-lambda-sync` for each, serially per account when the account has splits |

`../lambda-orchestrator-schedule/` triggers the orchestrator daily.

## Why Lambda, and why splits

A full sync of one account (about 55 modules) can take longer than Lambda's
900-second limit. Most modules are fast on most accounts, so the stack runs
each account as one invocation by default and lets slow accounts be split
into several invocations, run one after another (see "Account splits").

## Design

### Calls cartography's Python API, not the CLI

`src/handler.py` imports cartography's AWS `sync()` functions and calls them
directly for one account. This gives full control over which modules run in
each invocation. The cost: the handler depends on cartography's internal
module layout, so it vendors a pinned fork tag (see "Packaging") and has
drift-guard tests for the upstream functions it replaces.

The handler also creates the target account's `AWSAccount` and
`AWSRootPrincipal` nodes itself (`_ensure_account_node()`), looking up the
account name with `organizations:DescribeAccount` so it doesn't overwrite the
real name with the account ID.

### Module selection: include / exclude

`DEFAULT_SYNCS` (in `src/splits.py`) is upstream's `RESOURCE_FUNCTIONS` order
with two changes: `resourcegroupstaggingapi` runs in a separate pass, and
`permission_relationships` runs last. The event's `include` / `exclude` arrays
resolve against it:

- `include` non-empty: run exactly those modules, in the order given.
- `include` empty, `exclude` non-empty: run `DEFAULT_SYNCS` minus `exclude`.
- Both empty: run `DEFAULT_SYNCS`.
- Both non-empty: `cartography-lambda-sync` fails fast with
  `AmbiguousIncludeExclude`, before invoking the Lambda.

There's no auto-include. Upstream's private `_normalize_requested_syncs()`
adds `ec2:load_balancer_v2:expose` when you ask for `ec2:load_balancer_v2`;
this handler doesn't. List both if you want both.

The dispatch table comes from `cartography.intel.aws.resources.RESOURCE_FUNCTIONS`,
so every upstream AWS module key is available. The handler replaces a few
entries:

| Module key | Replaced by | Why |
|---|---|---|
| `iam` | `_iam_core_sync` | Everything upstream `iam.sync()` does except IAM Access Advisor, plus account-scoped cleanup (below) |
| `iam:service_access` | `_iam_service_access_sync` | Not an upstream key. The Access Advisor phase alone, with the AWS calls in a thread pool |
| `permission_relationships` | `_permission_relationships_prefiltered_sync` | Same output, much faster (below) |
| `inspector` | `_inspector_self_only_sync` | Syncs only the account's own findings, skipping the delegated-admin member enumeration |

### `iam` split

Upstream's `sync_service_last_accessed_details()` calls IAM Access Advisor
once per principal, serially, at about 2.5 seconds per principal. On an
account with ~550 principals that alone exceeds 900s.

- `iam` (core) runs every other IAM sub-phase in upstream's order.
- `iam:service_access` runs the Access Advisor phase. The AWS polling call
  runs in a `ThreadPoolExecutor`; the Neo4j writes stay on the calling thread
  because Neo4j sessions aren't thread-safe. Thread count:
  `IAM_SERVICE_ACCESS_MAX_WORKERS` (template sets `4`).

### Account-scoped managed-policy cleanup

Upstream's IAM cleanup removes stale `(:AWSPrincipal)-[:POLICY]->(:AWSManagedPolicy)`
edges with no account filter, because `AWSManagedPolicy` nodes are shared
across accounts:

```cypher
MATCH (n:AWSManagedPolicy)
MATCH (n)<-[r:POLICY]-(:AWSPrincipal)
WHERE r.lastupdated <> $UPDATE_TAG
WITH r LIMIT $LIMIT_SIZE
DELETE r;
```

Each Lambda invocation has its own `update_tag`, so every account's IAM
cleanup deleted every other account's attached-policy edges.
`_cleanup_iam_scoped()` runs the same cleanup jobs in the same order, but
swaps that one for:

```cypher
MATCH (:AWSAccount {id: $AWS_ID})-[:RESOURCE]->(:AWSPrincipal)-[r:POLICY]->(:AWSManagedPolicy)
WHERE r.lastupdated <> $UPDATE_TAG
WITH r LIMIT $LIMIT_SIZE
DELETE r
```

`tests/test_cleanup_iam_scoped.py` fails if upstream's cleanup sequence
changes, and also if upstream's query becomes scoped (then this override can
be dropped).

Health check (healthy = many accounts; broken = exactly one):

```cypher
MATCH (a:AWSAccount)-[:RESOURCE]->(:AWSPrincipal)-[r:POLICY]->(p:AWSManagedPolicy)
RETURN a.name, count(r) AS edges, collect(DISTINCT r.lastupdated) AS tags
ORDER BY edges DESC
```

### Permission mapping

`permission_relationships` builds `CAN_*` edges from principals to resources
using a mapping file. This stack packages its own,
`permission_relationships.yaml`:

- Section 1 is upstream's default file, copied verbatim.
- Section 2 adds:
  - `AWSSSMParameter` `CAN_READ`: `ssm:GetParameter`, `GetParameters`,
    `GetParametersByPath`, `GetParameterHistory`.
  - `AWSSSMParameter` `CAN_WRITE`: `ssm:PutParameter`, `DeleteParameter`,
    `DeleteParameters`, `LabelParameterVersion`.
  - `AWSKMSKey` `CAN_DECRYPT`: `kms:Decrypt`. IAM side only; key policies
    aren't evaluated, so read it as "IAM allows it".

The Makefile copies the file to `/var/task/permission_relationships.yaml`, so
a fork-ref bump can't change the mapping. If `permission_relationships` is
requested and the file is missing or invalid, the invocation fails before any
module runs (upstream would log a warning and build nothing).
`PERMISSION_RELATIONSHIPS_FILE` overrides the path for local runs.

The engine only evaluates a principal's own identity policies. SCPs,
permission boundaries and resource policies aren't evaluated; conditions are
recorded on the edge (`has_condition`, `condition_keys`, `conditions`) but not
evaluated. See `PERMISSIONS.md` for how to query the edges and what they get
wrong.

**Prefilter.** Upstream evaluates every principal against every target ARN and
statement. `_prune_principals_for_permissions()` first drops statements whose
actions can't match the entry, then empty policies, then principals with no
remaining Allow. Those can't create or block an edge, so the output is the
same. `tests/test_permission_relationships_prefilter.py` checks equivalence on
edge cases and randomized policy sets, and fails if upstream `sync()` changes.

**Changing the mapping:** edit the file, run the tests, `sam build` and
`sam deploy`. Edges no longer produced are removed per account on the next
run. After a fork-ref bump, re-copy upstream's file into section 1;
`test_contains_every_upstream_entry` fails until you do.

## Account splits

### Rules

Splits live in SSM `/cartography/lambda-orchestrator/account-splits`:

```json
{
  "<account_id>": {"splits": [
    {"exclude": ["inspector", "permission_relationships"]},
    {"include": ["inspector"]},
    {"include": ["permission_relationships"]}
  ]}
}
```

A split has `include` or `exclude` (a missing key means `[]`). An account
with no entry gets one invocation of the full `DEFAULT_SYNCS`.

`src/splits.py` `validate()` enforces the rules:

1. Every module in `DEFAULT_SYNCS` runs exactly once per account, and every
   name is real.
2. Modules run in `DEFAULT_SYNCS` order, except `MAY_RUN_LATE` (`inspector`,
   `sagemaker`, `securityhub`, `secretsmanager`), which no other synced
   module links to.
3. `permission_relationships` runs last.
4. The stored value is at most 8,192 bytes (SSM Advanced tier).

To change splits:

```sh
cp account-splits.example.json account-splits.json   # first time
make splits-check   # validate locally
make splits-put     # validate locally and with the deployed Lambda, then store
```

`splits-put` (`scripts/put-account-splits.sh`) needs `AWS_REGION` and refuses
to write while an orchestrator run is in progress.

### Why order matters, and why splitting is safe

Upstream's `resources.py` says module dependencies are enforced only by the
iteration order of `RESOURCE_FUNCTIONS` (e.g. `kms` before `s3` so
`ENCRYPTED_BY` edges find the key). A module that runs before its target's
module just doesn't create the edge, with no error.

Edges don't need the two modules to share a transaction, `update_tag` or
invocation. The edge queries built by `cartography/graph/querybuilder.py` use
`OPTIONAL MATCH` against whatever target nodes are in the graph. So splitting
an account across invocations is safe as long as the order is kept.
Re-check this when bumping the fork ref; the dependency comments in
`resources.py` aren't tested upstream.

## Orchestrator

```
CheckSyncOverrideInput
  -> ListAccounts (organizations:ListAccounts, ACTIVE, paginated)
       or the "accounts" list from the input
  -> GetExcludedAccounts / FilterExcludedAccounts
  -> GetAccountSplits
  -> SyncAccounts (Map across accounts, MaxConcurrency from SSM)
       -> RunSplits (Map, MaxConcurrency 1: one split at a time per account)
            -> states:startExecution.sync:2 on cartography-lambda-sync
  -> SummarizeResults
```

Input:

| Input | Effect |
|---|---|
| `{}` | Full org |
| `{"accounts": ["<id>", ...]}` | Only those accounts (still filtered by excluded-accounts) |
| `{"include": [...], "exclude": [...], "concurrency": N}` | Sync-override: every in-scope account gets one invocation with this include/exclude, ignoring account-splits. `concurrency` overrides max-concurrency for this run only. Combines with `accounts` |

### SSM parameters

CloudFormation seeds these on stack create only. Change them with
`aws ssm put-parameter --overwrite`; no redeploy, and a later deploy won't
overwrite them.

| Parameter | Format | Seed parameter |
|---|---|---|
| `/cartography/lambda-orchestrator/account-splits` | JSON object (above) | `AccountSplitsDefaultJson` (`{}`) |
| `/cartography/lambda-orchestrator/excluded-accounts` | JSON array of account IDs never synced | `ExcludedAccountsDefaultJson` (`[]`) |
| `/cartography/lambda-orchestrator/max-concurrency` | Integer string | `MaxConcurrencyDefault` (`3`, max 40) |

Raising max-concurrency adds concurrent writers on the one Neo4j instance.
`resourcegroupstaggingapi` deadlocks at high concurrency (it `MERGE`s
`AWSTag` nodes linked to ~34 resource types), which is why
`../lambda-orchestrator-schedule/` runs it in a separate low-concurrency pass
after the main run.

### JSONata: filters that match one item return a scalar

In JSONata, a filter that matches one element returns that element, not a
one-element array, and a filter that matches nothing returns `undefined`.
`Map` `Items` needs an array, so the state machine wraps filters:

```jsonata
$append([], $accounts[$not($ in $excludedAccounts)])
```

### `cartography-lambda-sync`

`ValidateInput -> InvokeLoader -> EvalInvokeResult -> AccountSyncComplete | AccountSyncFailed`

- `AccountSyncFailed` is a `Fail` state, so a failed sync shows as a failed
  execution in the console, metrics and EventBridge.
- The state machine computes `role_arn`: the hub account (the one the stack
  runs in) uses the Lambda's own role; every other account assumes
  `cartography-read-only`.
- Retries cover only Lambda service errors (throttling,
  `Lambda.ServiceException`, ...), not a sync the handler reported as
  `FAILED`, so a slow account isn't retried.

Lambda event:

```json
{
  "account_id": "111111111111",
  "include": ["iam"],
  "exclude": [],
  "role_arn": "arn:aws:iam::111111111111:role/cartography-read-only",
  "regions": ["us-east-1"]
}
```

`role_arn` and `regions` are optional (omit `role_arn` for the hub account;
omit `regions` to auto-discover with `ec2:DescribeRegions`).

## Packaging

The fork's `pyproject.toml` pulls in every provider's dependencies, so the
build doesn't `pip install cartography`. Instead the Makefile:

1. installs a trimmed `requirements.txt`;
2. shallow-clones `CARTOGRAPHY_FORK_URL` at `CARTOGRAPHY_GIT_REF` and copies
   only the `cartography/` package directory;
3. copies `handler.py`, `splits.py` and `permission_relationships.yaml`.

Importing `cartography.intel.aws` imports every AWS submodule, but not other
providers. The unzipped package is about 104 MB, under Lambda's 250 MB limit.

Build with `sam build --use-container` on anything other than Linux x86_64:
compiled dependencies such as `cryptography` are platform-specific.

## Networking and IAM

- Runs in `VpcId` / `SubnetId` with its own security group
  (`cartography-lambda-loader-sg`): HTTPS out (via NAT) and Bolt 8443 to the
  Neo4j security group, plus an ingress rule on the Neo4j SG from this one.
- Connects to `bolt://<neo4j-private-ip>:8443` (SSM
  `/cartography/neo4j/private-ip`) as user `neo4j`, with the password read
  from SSM `/cartography/neo4j/password` at invocation time.
- Execution role `cartography-lambda-loader-role`: `SecurityAudit` (for the
  hub account), `sts:AssumeRole` on `cartography-read-only` in spoke accounts
  (`../stackset/`), `ec2:DescribeRegions`, `organizations:DescribeAccount`,
  `lambda:GetFunction`, `codebuild:BatchGetBuilds`, `ssm:GetParameter` on
  `/cartography/neo4j/password`, and `AWSLambdaVPCAccessExecutionRole`.

## Known gaps

- **No `organizations.sync()`.** Each invocation creates only its own
  account's `AWSAccount` node.
- **No cross-account post-processing.** Upstream runs
  `aws_post_ingestion_principals_cleanup.json` and `_perform_aws_analysis()`
  (internet exposure, foreign-account linking) once after all accounts; there's
  no equivalent here.
- **One `update_tag` per invocation**, not per run. Cleanup that relies on a
  shared tag across accounts won't line up.
- **No `SyncDuration` / `SyncSuccess` CloudWatch metrics** from the handler.
  `../dashboard/` shows Lambda and Step Functions metrics instead.
- **SecureString parameters aren't linked to their KMS key** when `keyid` is
  an alias, so read-and-decrypt has to be joined on `keyid` by hand.
- **Dead code:** `_resolve_role_arn()` is unused.

## Files

```
lambda-loader/
├── template.yaml                    Lambda, IAM, both state machines, SSM parameters
├── Makefile                         SAM build (trimmed deps + vendored cartography), splits-check/put
├── requirements.txt                 Trimmed runtime dependencies
├── permission_relationships.yaml    AWS permission mapping (CAN_* edges)
├── PERMISSIONS.md                   Querying the CAN_* edges and their limits
├── account-splits.example.json      Example splits (copy to account-splits.json, gitignored)
├── samconfig.example.toml           Example deploy config (copy to samconfig.toml, gitignored)
├── scripts/put-account-splits.sh    Validate and store account-splits
├── src/handler.py                   Lambda handler
├── src/splits.py                    DEFAULT_SYNCS and split rules
└── tests/                           Offline pytest
```

## Build, deploy and test

Prerequisites: `../network/`, `../neo4j/` and `../stackset/` are deployed.

```sh
cd lambda-loader
cp samconfig.example.toml samconfig.toml   # set region, VpcId, SubnetId
sam build --use-container
sam deploy
```

`sam deploy` deploys `.aws-sam/build/`, so re-run `sam build` after every
change to `template.yaml` or `src/`.

Note: changing a parameter's `Default` in `template.yaml` doesn't change an
existing stack, which keeps its previous value. Set it in
`parameter_overrides` instead.

Tests (need the cartography fork importable, e.g. its virtualenv):

```sh
python -m pytest tests -q
```

Run one account:

```sh
SM=arn:aws:states:$AWS_REGION:<hub-account-id>:stateMachine
aws stepfunctions start-execution --state-machine-arn $SM:cartography-lambda-sync \
  --input '{"account_id":"111111111111","include":["iam"],"exclude":[]}'
```

Run the orchestrator for a few accounts, or one module across the org:

```sh
aws stepfunctions start-execution --state-machine-arn $SM:cartography-lambda-orchestrator \
  --input '{"accounts":["111111111111","222222222222"]}'

aws stepfunctions start-execution --state-machine-arn $SM:cartography-lambda-orchestrator \
  --input '{"include":["resourcegroupstaggingapi"],"concurrency":3}'
```

Check a result and follow logs:

```sh
aws stepfunctions describe-execution --execution-arn <arn> --query output --output text
aws logs tail /cartography/lambda-loader --since 5m --follow
```
