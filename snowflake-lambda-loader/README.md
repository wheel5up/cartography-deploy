# Cartography Snowflake Lambda Loader

Syncs one Snowflake account into Neo4j by calling cartography's Python API
from a Lambda function. cartography's Snowflake module has no
`--snowflake-requested-syncs` option: `start_snowflake_ingestion()` is one
call through every sub-module, which takes about an hour on a large account.
This loader runs each sub-module as a named **step**, groups the steps into a
fixed list of **splits** (one Lambda invocation each), and runs cleanup once
at the end.

Grants and network access for the collector role are in
[`SNOWFLAKEROLE.md`](SNOWFLAKEROLE.md). The optional procedure for reading
security integrations is in [`sql/`](sql/README.md).

## Architecture

- **`cartography-snowflake-lambda-sync`**: one execution is one Lambda
  invocation, given an explicit step list.
- **`cartography-snowflake-lambda-orchestrator`**: runs the splits from SSM
  (`/cartography/snowflake-lambda/default-splits`) serially under one shared
  `update_tag`, then one `cleanup` invocation, then one `sync_complete`
  invocation. The daily schedule triggers the orchestrator.

`src/handler.py` wraps every `cartography.intel.snowflake.<module>.sync()`
call, unmodified, in `start_snowflake_ingestion()`'s order. Several syncs take
earlier steps' output as input (`schemas` needs `databases`, `grants` needs
`roles`, service users and `database_roles`). Lambda keeps no state between
invocations, so the handler re-runs missing prerequisites within the
invocation that needs them; for example `tables` on its own also lists
databases and schemas. The exception is `grants_cleanup`: it fails unless
`grants` and `account_grants` are in the same invocation, rather than
silently re-running the expensive `grants` sync.

### Default splits

| # | Steps |
|---|---|
| 1 | `account`, `account_parameters`, `databases`, `schemas` |
| 2 | `network_rules`, `network_policies`, `roles`, `database_roles` |
| 3 | `users`, `access_tokens`, `credentials` |
| 4 | `resource_monitors`, `warehouses`, `compute_pools`, `external_volumes` |
| 5 | `storage_integrations`, `api_integrations`, `catalog_integrations`, `notification_integrations`, `security_integrations`, `external_access_integrations` |
| 6 | `tags`, `file_formats`, `stages`, `secrets`, `sequences` |
| 7 | `tables` |
| 8 | `views`, `materialized_views`, `external_tables`, `event_tables` |
| 9 | `iceberg_tables`, `dynamic_tables`, `streams` |
| 10 | `password_policies`, `session_policies`, `authentication_policies`, `data_policies` |
| 11 | `functions`, `procedures`, `pipes`, `tasks`, `alerts` |
| 12 | `cortex_search_services`, `notebooks`, `streamlits`, `artifact_repositories`, `image_repositories`, `services` |
| 13 | `shares`, `listings`, `replication_groups`, `policy_references`, `grants`, `account_grants`, `grants_cleanup` |

Sized from measured timing on an account with ~95 databases, ~470 schemas,
~10,600 tables, ~7,200 views and ~1.1 million grant rows. A full run takes
about an hour. `tables` is the long pole at about 770s, close to the 900s
cap, so it runs alone. Most of the time is many serial per-schema round
trips, not one slow query. Re-time the splits if your account is larger:
`scripts/test_snowflake_module.py` runs any list of steps locally.

`grants` reads every grant row into memory: it ran out of memory at 512 MB
on the account above, so `LambdaMemoryMb` defaults to 2048. CloudFormation
keeps an existing stack's parameter value when the template default changes,
so set it in `samconfig.toml`.

### Cleanup: only modules that ran, and ran completely

Upstream runs one central cleanup at the end, in reverse dependency order,
and skips any module whose sync reported itself **incomplete** (a 403 on one
database, an unreadable `ACCOUNT_USAGE` view). Cleanup deletes whatever the
run didn't touch, so cleaning a partially read module would delete real
objects.

Across invocations, every split returns its `completed` and `incomplete`
lists. After the Map, the orchestrator takes the union of each and passes
both to the one `cleanup` invocation. **A module is cleaned up only if it's in
`completed` and not in `incomplete`.**

Both conditions are needed. Gating only on `incomplete` treats a module that
never ran as "read in full, found nothing": an orchestrator run with a
two-split override would then delete every table, view, user, role, schema
and database in the graph. With the `completed` gate, partial runs only clean
up what they ran, and a missing `completed` list cleans nothing.

Grant edges have their own three-way completeness gate (object grants, role
assignments, inherited grants), run by `grants_cleanup` in split 13, not by
the central `cleanup`.

**Cleanup cascade.** Cleanup of schema-scoped modules is account-wide: if the
schema walk can't read one database or schema, every schema-scoped module is
marked incomplete, and none of their stale nodes are deleted, in any
database. Dropped objects then stay in the graph. Use the skip lists below to
keep out-of-scope databases and schemas from blocking cleanup, and check the
`incomplete` list a run passes to `cleanup` (it's in the orchestrator's
execution history).

## Configuration

### Credentials

`scripts/bootstrap-snowflake-ssm.sh` loads a key-pair credential handoff into
SSM:

```sh
./scripts/bootstrap-snowflake-ssm.sh <path-to-creds-json>
```

The input JSON has `private_key_b64` (base64 of a DER-encoded, encrypted
PKCS8 key), `private_key_passphrase`, `role`, `username`, `warehouse` and
`account_identifier`. The script converts the key to PEM (cartography's
`SnowflakeClient` only accepts PEM) and writes six parameters under
`/cartography/snowflake/`: `account`, `user`, `role`, `warehouse`,
`private-key` and `private-key-passphrase`. Secrets go through temp files,
never `--value`. See the script header for details.

**Check the stored passphrase has no trailing newline.** A shell's `$(...)`
strips trailing newlines and boto3 doesn't, so a passphrase stored with one
works from the CLI but fails in the Lambda with
`ValueError: Incorrect password, could not decrypt key`.

### Skip lists (SSM, no redeploy)

Skipped objects are still inventoried but not walked, and don't block
cleanup.

| Parameter | Matched against | Example |
|---|---|---|
| `/cartography/snowflake/skip-databases` | database name | `SANDBOX_*,LEGACY_DB` |
| `/cartography/snowflake/skip-schemas` | `DATABASE.SCHEMA` | `SNOWFLAKE_LEARNING_DB.*@*` (per-user tutorial schemas) |

Comma-separated, case-insensitive shell globs; quotes Snowflake puts around
names are ignored. The fork always skips the `DROPPED_USER$*` placeholder
databases Snowflake creates when a user who owns objects is dropped. The
handler reads both lists at invocation time:

```sh
aws ssm put-parameter --overwrite --type String \
  --name /cartography/snowflake/skip-schemas --value 'SNOWFLAKE_LEARNING_DB.*@*,MYDB.BIGSCHEMA'
```

A missing parameter means skip nothing. That's safe: unreadable objects make
cleanup skip, never run wrongly.

## Deploy

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build --use-container   # needed on non-x86_64 machines (native wheels)
sam deploy
```

The stack creates its own security group and adds one ingress rule (Bolt
8443) to the Neo4j security group. If the Snowflake account has a network
policy, allow the NAT gateway's public IP (see `SNOWFLAKEROLE.md`).

The daily schedule (07:00 UTC, `/cartography/snowflake-lambda/schedule`) is
created `ENABLED`. Change `State` in `template.yaml`, not with the CLI: the
next `sam deploy` reverts a CLI change.

## Run manually

```sh
# Full run
aws stepfunctions start-execution --state-machine-arn <OrchestratorStateMachineArn> --input '{}'

# One split, no cleanup
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> \
  --input '{"steps": ["databases", "schemas", "tables"]}'

# Logs
aws logs tail /cartography/snowflake-lambda-loader --since 5m --follow
```

Locally, against real Snowflake and Neo4j (needs AWS credentials for the SSM
parameters and `NEO4J_URI`):

```sh
python scripts/test_snowflake_module.py databases schemas tables
```

It calls `handler.handler()` directly (so prerequisites are filled in the
same way) and checks the total time against the 900s budget.

## Optional grants

Without a few extra grants (`MONITOR` on services, `USAGE` on integrations),
the affected modules log a warning, skip those objects and are marked
incomplete. See [`SNOWFLAKEROLE.md`](SNOWFLAKEROLE.md#optional-grants).

## Known limits

- **`SHOW TABLES` returns at most 10,000 rows**, with no pagination. A schema
  with more tables logs `Snowflake error 090153`, its tables are missing from
  the graph, and table cleanup is skipped. Fixing it would mean paging with
  `SHOW TABLES ... LIMIT <n> FROM '<last_name>'` or reading
  `INFORMATION_SCHEMA.TABLES` instead.
- **All requests are serial** (single thread, blocked on socket reads). Some
  concurrency might cut run time, but hasn't been tried. Snowflake returns
  429 for too many concurrent requests and doesn't publish the limit.
  cartography's `SnowflakeClient` retries 429s (`urllib3.Retry`, 5 attempts,
  honours `Retry-After`).

## Files

```
snowflake-lambda-loader/
├── README.md
├── SNOWFLAKEROLE.md                  # collector role grants and network policy
├── template.yaml                     # Lambda, IAM, SG, sync + orchestrator state machines, schedule
├── Makefile                          # trimmed deps + vendored cartography at the pinned fork tag
├── requirements.txt
├── samconfig.example.toml            # copy to samconfig.toml (gitignored)
├── src/handler.py                    # step runners, completeness tracking, cleanup gates
├── scripts/
│   ├── bootstrap-snowflake-ssm.sh    # load a key-pair credential into SSM
│   └── test_snowflake_module.py      # run steps locally
└── sql/                              # optional DESCRIBE_SECURITY_INTEGRATIONS procedure
```
