"""
Cartography Snowflake sync — Lambda handler.

Calls cartography's Python API directly (not the CLI), same approach as
../okta-lambda-loader/src/handler.py and ../lambda-loader/src/handler.py use
for Okta and AWS respectively. Snowflake has NO --snowflake-requested-syncs
CLI flag (confirmed — unlike AWS's --aws-requested-syncs, there is no way to
run a subset of start_snowflake_ingestion()'s sub-modules through the CLI at
all). This handler recreates that call, sub-module by sub-module, as a
dispatch table keyed by step name, driven by a FIXED, pre-defined split list
(same pattern as Okta's orchestrator / AWS's account-splits.json) — not a
runtime decision made by this handler. See
../README.md "Default splits" for the measured-timing-derived split this
handler's steps are grouped into by the orchestrator.

THIS HANDLER DOES NOT DECIDE HOW TO GROUP STEPS ACROSS INVOCATIONS. That is
the orchestrator state machine's job (template.yaml's
CartographySnowflakeLambdaOrchestrator). This handler's only job is: run
exactly the steps it's given, in order, start to finish, and fail loudly if
any one of them fails.

======================================================================
COVERAGE: all 55 real sub-modules cartography.intel.snowflake.__init__
.start_snowflake_ingestion() calls, in that function's own order
======================================================================
Every step below wraps exactly one cartography.intel.snowflake.<module>
sync() call, unmodified — no patching, no reimplementation. The step order
in STEP_RUNNERS/ALL_STEPS_IN_ORDER matches start_snowflake_ingestion()'s own
call order exactly (account -> account_parameters -> databases -> schemas ->
network_rules -> network_policies -> roles/database_roles/users ->
access_tokens/credentials -> compute (resource_monitors/warehouses/
compute_pools) -> external_volumes -> 5 integrations -> 17 schema-level
modules -> external_access_integrations -> 11 workload modules -> data
sharing (shares/listings/replication_groups) -> policy_references ->
grants/account_grants), because several steps' sync() calls take earlier
steps' OUTPUT as required arguments (e.g. schemas.sync() needs databases;
network_policies.sync() needs both account_parameters' network-policy name
AND network_rules' rule list; grants.sync() needs roles + service user names
+ database_roles). This handler exposes those dependencies as explicit
PARAMETERS on each run_* function rather than hiding them, so a caller (the
local test harness, or the orchestrator's per-invocation step list) can see
exactly what has to run first.

======================================================================
COMPLETENESS TRACKING AND CENTRALIZED CLEANUP — the one real design
difference from ../okta-lambda-loader/src/handler.py
======================================================================
Unlike Okta (which has exactly one special cleanup step,
"applications:cleanup", scoped to one module), Snowflake's REAL
start_snowflake_ingestion() runs a SINGLE centralized _cleanup() call at the
very end, which walks ~28 node-cleanup modules in a fixed reverse-dependency
order (_CLEANUP_ORDER in cartography/intel/snowflake/__init__.py) and SKIPS
any module whose sync() reported incomplete=True this run — so a 403 on one
database, or an unreadable ACCOUNT_USAGE view, does not cause THAT module's
stale nodes to be deleted on the strength of a partial read. Separately,
grants.cleanup() (MatchLink edge cleanup, not node cleanup) has its own
three-way completeness gate (object grants, role assignments, inherited
grants) combining BOTH grants.sync() and account_grants.sync()'s results.

Replicating this faithfully across MULTIPLE Lambda invocations means the
"incomplete" set has to be the UNION of every module's completeness across
EVERY invocation in the whole run, not just the last one — a module skipped
as incomplete in invocation 2 must still be skipped during the cleanup
invocation that runs last, even though cleanup itself happens in a later,
separate invocation with no memory of invocation 2's own execution. This
handler's run() functions each return their own module's completeness
(mirroring the real sync() return values exactly); run_cleanup() takes the
ACCUMULATED incomplete-set as an explicit parameter (threaded through the
orchestrator's Step Functions Map output, the same mechanism
../okta-lambda-loader/template.yaml already uses to thread one shared
update_tag across invocations — see that template's ResolveSplits/RunSplits
comments) rather than trying to recompute or re-derive it.

Invocation contract (event):
{
  "steps": ["databases", "schemas", ...],
  "update_tag": 1792017089,       # REQUIRED except on the very first
                                    # invocation of a run
  "incomplete": ["tables", ...]    # OPTIONAL — modules already known
                                    # incomplete from EARLIER invocations in
                                    # this same run, so this invocation's own
                                    # completeness tracking starts from the
                                    # right baseline rather than from empty.
                                    # Only meaningful for the "cleanup" step
                                    # (see run_cleanup below) and for any
                                    # invocation that might itself be asked
                                    # to run "cleanup" alongside other steps.
}

Returns:
{
  "status": "SUCCEEDED" | "FAILED",
  "completed": [...],
  "remaining": [...],
  "updateTag": <int>,
  "incomplete": [...],   # this invocation's OWN newly-incomplete modules,
                          # to be unioned by the orchestrator into the
                          # accumulated set it passes to later invocations
  "durationSeconds": <float>,
}
"""

from __future__ import annotations

import importlib
import logging
import os
import time
from typing import Any

import boto3
import neo4j
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Quiet the very chatty neo4j driver notification logger; cartography's own
# sync logging already covers what we need at INFO. Same pattern as
# ../okta-lambda-loader/src/handler.py.
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)


def _get_ssm_parameter(name: str, with_decryption: bool = False) -> str:
    """Fetch one SSM parameter at invoke time.

    Mirrors ../okta-lambda-loader/src/handler.py's _get_okta_api_key()
    pattern: secrets are fetched at invocation time via ssm:GetParameter,
    never baked into Lambda environment variables. The six
    /cartography/snowflake/* parameters this reads are the ones
    ../scripts/bootstrap-snowflake-ssm.sh writes.

    This function does NOT strip whitespace defensively. A stored value
    with trailing newlines (easy to miss: bash command substitution strips
    them silently, so shell callers never notice) should be fixed at the
    source rather than papered over here, so the bad data stays visible.
    """
    ssm = boto3.client("ssm")
    response = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
    return response["Parameter"]["Value"]


def _get_snowflake_credentials() -> dict[str, str]:
    """Fetch all six /cartography/snowflake/* SSM parameters at once."""
    return {
        "account": _get_ssm_parameter("/cartography/snowflake/account"),
        "user": _get_ssm_parameter("/cartography/snowflake/user"),
        "role": _get_ssm_parameter("/cartography/snowflake/role"),
        "warehouse": _get_ssm_parameter("/cartography/snowflake/warehouse"),
        "private_key": _get_ssm_parameter(
            "/cartography/snowflake/private-key", with_decryption=True
        ),
        "private_key_passphrase": _get_ssm_parameter(
            "/cartography/snowflake/private-key-passphrase", with_decryption=True
        ),
    }


def _build_snowflake_client(credentials: dict[str, str]) -> Any:
    """Build the SnowflakeClient used for every step in this invocation."""
    from cartography.intel.snowflake.util import SnowflakeClient

    return SnowflakeClient(
        account_id=credentials["account"],
        user=credentials["user"],
        private_key=credentials["private_key"],
        private_key_passphrase=credentials["private_key_passphrase"],
        role=credentials["role"],
        warehouse=credentials["warehouse"],
    )


def _get_neo4j_password() -> str:
    """Neo4j password from SSM at invoke time, never a Lambda env var.
    NEO4J_PASSWORD_SSM_PARAM is the parameter NAME only."""
    response = boto3.client("ssm").get_parameter(
        Name=os.environ.get(
            "NEO4J_PASSWORD_SSM_PARAM", "/cartography/neo4j/password"
        ),
        WithDecryption=True,
    )
    return response["Parameter"]["Value"]


def _get_neo4j_driver() -> neo4j.Driver:
    uri = os.environ["NEO4J_URI"]  # e.g. bolt://<neo4j-private-ip>:8443
    return neo4j.GraphDatabase.driver(
        uri,
        auth=("neo4j", _get_neo4j_password()),
        max_connection_lifetime=int(
            os.environ.get("NEO4J_MAX_CONNECTION_LIFETIME", "200")
        ),
    )


def _common_job_parameters(client: Any, update_tag: int) -> dict[str, Any]:
    return {"UPDATE_TAG": update_tag, "ACCOUNT_ID": client.account_id}


# ======================================================================
# Context object — carries every prerequisite OUTPUT a later step's sync()
# call needs as an argument, plus this invocation's own accumulated
# completeness tracking. One Context is built fresh per invocation (NOT
# persisted across invocations — see module docstring "COMPLETENESS
# TRACKING" for how cross-invocation state is threaded instead, via the
# orchestrator's Step Functions Map output, not via this object).
# ======================================================================


class Context:
    def __init__(
        self,
        incomplete_from_earlier_invocations: set[str] | None = None,
        completed_from_earlier_invocations: set[str] | None = None,
    ):
        # Prerequisite outputs, populated as the steps that produce them run.
        self.databases: list[dict[str, Any]] | None = None
        self.schema_list: list[dict[str, Any]] | None = None
        self.account_network_policy: str | None = None
        self.network_rule_list: list[dict[str, Any]] | None = None
        self.role_list: list[dict[str, Any]] | None = None
        self.database_role_list: list[dict[str, Any]] | None = None
        self.humans: list[dict[str, Any]] | None = None
        self.service_users: list[dict[str, Any]] | None = None
        # grants.sync()/account_grants.sync()'s own completeness booleans,
        # needed (separately from the general `incomplete` set below) by
        # run_grants_cleanup()'s three-way gate — see that function.
        self.grants_complete: bool | None = None
        self.role_assignments_complete: bool | None = None
        self.inherited_grants_complete: bool | None = None
        self.account_grants_complete: bool | None = None

        # Modules this invocation's own steps found incomplete, UNIONED with
        # whatever the orchestrator told us was already incomplete from
        # earlier invocations in this same run (see module docstring). This
        # is what gets returned to the orchestrator as "incomplete" in the
        # response, for it to accumulate forward into later invocations.
        self.incomplete: set[str] = set(incomplete_from_earlier_invocations or [])

        # Steps that actually ran to completion in this run — this
        # invocation's own steps (added by handler() as each finishes) plus
        # whatever earlier invocations completed (passed in by the
        # orchestrator). run_cleanup only cleans modules in this set — see
        # its docstring "WHY BOTH GATES".
        self.completed: set[str] = set(completed_from_earlier_invocations or [])

    def record(self, module_name: str, complete: bool) -> None:
        if not complete:
            self.incomplete.add(module_name)


# ======================================================================
# Per-module step functions — each wraps exactly one
# cartography.intel.snowflake.<module>.sync() call, unmodified. Ordered to
# match start_snowflake_ingestion()'s own call order (see module docstring).
# ======================================================================


def run_account(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
    from cartography.intel.snowflake import account

    complete = account.sync(neo4j_session, client, _common_job_parameters(client, update_tag))
    ctx.record("account", complete)


def run_account_parameters(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    from cartography.intel.snowflake import account_parameters

    network_policy, complete = account_parameters.sync(
        neo4j_session, client, _common_job_parameters(client, update_tag)
    )
    ctx.account_network_policy = network_policy
    ctx.record("account_parameters", complete)


SKIP_DATABASES_PARAM = "/cartography/snowflake/skip-databases"
SKIP_SCHEMAS_PARAM = "/cartography/snowflake/skip-schemas"


def _skip_patterns(param_name: str) -> frozenset[str]:
    """Comma-separated shell-style skip patterns from SSM (empty if unset).

    Read at invoke time so the lists can change with `aws ssm put-parameter`,
    no redeploy. Skipped databases/schemas are still inventoried but not
    walked, so they don't mark the walk incomplete and don't block cleanup
    account-wide. Schema patterns match DATABASE.SCHEMA. See cartography's
    Snowflake config docs.

    A missing parameter means "skip nothing" - the safe direction: every
    unreadable object then marks the walk incomplete and cleanup is skipped,
    never wrongly run. Any other SSM error is raised.
    """
    from cartography.intel.snowflake import _parse_patterns

    try:
        raw = _get_ssm_parameter(param_name)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "ParameterNotFound":
            raise
        logger.warning("SSM parameter %s not found; skipping nothing.", param_name)
        return frozenset()
    patterns = _parse_patterns(raw)
    logger.info("Snowflake skip list %s: %s", param_name, sorted(patterns))
    return patterns


def run_databases(
    ctx: Context,
    client: Any,
    neo4j_session: neo4j.Session,
    update_tag: int,
    allowed_databases: set[str] | None = None,
) -> None:
    from cartography.intel.snowflake import databases

    walkable, complete = databases.sync(
        neo4j_session,
        client,
        allowed_databases,
        _common_job_parameters(client, update_tag),
        _skip_patterns(SKIP_DATABASES_PARAM),
    )
    ctx.databases = walkable
    ctx.record("databases", complete)


def run_schemas(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
    from cartography.intel.snowflake import schemas

    schema_list, complete = schemas.sync(
        neo4j_session,
        client,
        ctx.databases,
        _common_job_parameters(client, update_tag),
        _skip_patterns(SKIP_SCHEMAS_PARAM),
    )
    ctx.schema_list = schema_list
    ctx.record("schemas", complete)

    # Mirrors start_snowflake_ingestion()'s own propagation exactly: if
    # EITHER databases or schemas was incomplete, every schema-scoped
    # surface's cleanup must ALSO be skipped, because schema_list itself is
    # then silently truncated and every downstream sync walks exactly the
    # (incomplete) list it is handed, reporting complete=True in good faith
    # while holding no objects for the unread database/schema. See
    # cartography/intel/snowflake/__init__.py's own comment above its
    # `if not databases_and_schemas_complete(incomplete):` block — this is
    # the same logic, reproduced here since this handler calls each
    # schema-scoped module as a separate step rather than in one function.
    if {"databases", "schemas"} & ctx.incomplete:
        for name in SCHEMA_SCOPED_SURFACES:
            ctx.incomplete.add(name)


def run_network_rules(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    from cartography.intel.snowflake import network_rules

    rule_list, complete = network_rules.sync(
        neo4j_session, client, ctx.schema_list, _common_job_parameters(client, update_tag)
    )
    ctx.network_rule_list = rule_list
    ctx.record("network_rules", complete)


def run_network_policies(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    from cartography.intel.snowflake import network_policies

    # network_policies.sync() returns None (not a completeness bool) — see
    # cartography/intel/snowflake/network_policies.py; cartography's own
    # start_snowflake_ingestion() does not record its completeness either.
    network_policies.sync(
        neo4j_session,
        client,
        ctx.account_network_policy,
        ctx.network_rule_list,
        _common_job_parameters(client, update_tag),
    )


def run_roles(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
    from cartography.intel.snowflake import account_usage
    from cartography.intel.snowflake import roles

    account_usage_roles = account_usage.get_roles(client)
    role_list, complete = roles.sync(
        neo4j_session, client, account_usage_roles, _common_job_parameters(client, update_tag)
    )
    ctx.role_list = role_list
    ctx.record("roles", complete)


def run_database_roles(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    from cartography.intel.snowflake import account_usage
    from cartography.intel.snowflake import database_roles

    account_usage_roles = account_usage.get_roles(client)
    database_role_list, complete = database_roles.sync(
        neo4j_session,
        client,
        ctx.databases,
        account_usage_roles,
        _common_job_parameters(client, update_tag),
    )
    ctx.database_role_list = database_role_list
    ctx.record("database_roles", complete)


def run_users(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
    from cartography.intel.snowflake import users

    humans, service_users = users.sync(
        neo4j_session, client, _common_job_parameters(client, update_tag)
    )
    ctx.humans = humans
    ctx.service_users = service_users
    # users.sync() has no completeness return (see
    # cartography/intel/snowflake/users.py) — matches
    # start_snowflake_ingestion()'s own behavior of not recording it either.


def run_access_tokens(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    from cartography.intel.snowflake import access_tokens

    all_users = (ctx.humans or []) + (ctx.service_users or [])
    complete = access_tokens.sync(
        neo4j_session, client, all_users, _common_job_parameters(client, update_tag)
    )
    ctx.record("access_tokens", complete)


def run_credentials(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
    from cartography.intel.snowflake import credentials

    complete = credentials.sync(neo4j_session, client, _common_job_parameters(client, update_tag))
    ctx.record("credentials", complete)


def _make_account_scoped_runner(module_name: str):
    """Factory for modules with the simple
    sync(neo4j_session, client, common_job_parameters) -> bool shape and no
    prerequisite inputs beyond the client — covers resource_monitors,
    warehouses, compute_pools, external_volumes, and the 5 integration
    modules. Mirrors _make_schema_scoped_runner below but for the
    account-scoped (not schema-scoped) shape."""

    def _runner(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
        module = importlib.import_module(f"cartography.intel.snowflake.{module_name}")
        complete = module.sync(neo4j_session, client, _common_job_parameters(client, update_tag))
        ctx.record(module_name, complete)

    _runner.__name__ = f"run_{module_name}"
    return _runner


ACCOUNT_SCOPED_MODULES = [
    "resource_monitors",
    "warehouses",
    "compute_pools",
    "external_volumes",
    "storage_integrations",
    "api_integrations",
    "catalog_integrations",
    "notification_integrations",
    "security_integrations",
    "external_access_integrations",
]
for _m in ACCOUNT_SCOPED_MODULES:
    globals()[f"run_{_m}"] = _make_account_scoped_runner(_m)
del _m


def _make_schema_scoped_runner(module_name: str):
    """Factory for the ~26 schema-scoped modules sharing the identical
    sync(neo4j_session, client, schemas, common_job_parameters) -> bool
    signature."""

    def _runner(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
        module = importlib.import_module(f"cartography.intel.snowflake.{module_name}")
        complete = module.sync(
            neo4j_session, client, ctx.schema_list, _common_job_parameters(client, update_tag)
        )
        ctx.record(module_name, complete)

    _runner.__name__ = f"run_{module_name}"
    return _runner


# The 17 "schema-level" data-object modules (tags through data_policies in
# start_snowflake_ingestion()'s own _SCHEMA_LEVEL_MODULES tuple) plus the 11
# "workload" modules (_WORKLOAD_MODULES) — both share the identical
# schema-scoped signature, so one factory covers all 28.
SCHEMA_LEVEL_MODULES = [
    "tags",
    "file_formats",
    "stages",
    "secrets",
    "sequences",
    "tables",
    "views",
    "materialized_views",
    "external_tables",
    "event_tables",
    "iceberg_tables",
    "dynamic_tables",
    "streams",
    "password_policies",
    "session_policies",
    "authentication_policies",
    "data_policies",
]
WORKLOAD_MODULES = [
    "functions",
    "procedures",
    "pipes",
    "tasks",
    "alerts",
    "cortex_search_services",
    "notebooks",
    "streamlits",
    "artifact_repositories",
    "image_repositories",
    "services",
]
for _m in SCHEMA_LEVEL_MODULES + WORKLOAD_MODULES:
    globals()[f"run_{_m}"] = _make_schema_scoped_runner(_m)
del _m

# Every surface whose contents are enumerated by walking the database or
# schema list — mirrors cartography/intel/snowflake/__init__.py's own
# _SCHEMA_SCOPED_SURFACES exactly (used by run_schemas()'s incompleteness
# propagation above).
SCHEMA_SCOPED_SURFACES = frozenset(
    SCHEMA_LEVEL_MODULES + WORKLOAD_MODULES + ["network_rules", "database_roles", "schemas"]
)


def _make_account_scoped_no_prereq_runner(module_name: str):
    """Factory for shares/listings/replication_groups — same shape as
    _make_account_scoped_runner but kept as a separate, explicitly-named
    group since these three are conceptually "data sharing and
    replication" in start_snowflake_ingestion()'s own comments, run after
    every schema-scoped module, not alongside the compute/integration
    group above."""

    def _runner(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
        module = importlib.import_module(f"cartography.intel.snowflake.{module_name}")
        complete = module.sync(neo4j_session, client, _common_job_parameters(client, update_tag))
        ctx.record(module_name, complete)

    _runner.__name__ = f"run_{module_name}"
    return _runner


DATA_SHARING_MODULES = ["shares", "listings", "replication_groups"]
for _m in DATA_SHARING_MODULES:
    globals()[f"run_{_m}"] = _make_account_scoped_no_prereq_runner(_m)
del _m


def run_policy_references(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    from cartography.intel.snowflake import policy_references

    complete = policy_references.sync(
        neo4j_session, client, _common_job_parameters(client, update_tag)
    )
    ctx.record("policy_references", complete)


def run_grants(ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int) -> None:
    from cartography.intel.snowflake import grants

    service_user_names = {u["name"] for u in (ctx.service_users or [])}
    grants_complete, role_assignments_complete, inherited_grants_complete = grants.sync(
        neo4j_session,
        client,
        ctx.role_list,
        service_user_names,
        ctx.database_role_list,
        _common_job_parameters(client, update_tag),
    )
    ctx.grants_complete = grants_complete
    ctx.role_assignments_complete = role_assignments_complete
    ctx.inherited_grants_complete = inherited_grants_complete
    # grants (the node-level, non-MatchLink surfaces grants.sync() may also
    # touch) is NOT added to ctx.incomplete here — grants.sync()'s three
    # return values feed grants.cleanup()'s own three-way MatchLink gate
    # directly (see run_grants_cleanup below), mirroring
    # start_snowflake_ingestion()'s own handling exactly: it does not fold
    # these into the general `incomplete` set either.


def run_account_grants(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    from cartography.intel.snowflake import account_grants

    complete = account_grants.sync(neo4j_session, client, _common_job_parameters(client, update_tag))
    ctx.account_grants_complete = complete


# ======================================================================
# Centralized cleanup — mirrors cartography/intel/snowflake/__init__.py's
# own _cleanup() + grants.cleanup() exactly. See module docstring
# "COMPLETENESS TRACKING" for why this takes the ACCUMULATED incomplete set
# as an input rather than only this invocation's own ctx.incomplete.
# ======================================================================

# Reverse dependency order — copied from cartography/intel/snowflake/
# __init__.py's own _CLEANUP_ORDER (module-name strings here instead of
# imported module objects, since this handler calls each module's cleanup()
# via importlib rather than a static import list).
CLEANUP_ORDER = (
    ["policy_references", "listings", "shares", "replication_groups"]
    + list(reversed(WORKLOAD_MODULES))
    + ["external_access_integrations"]
    + list(reversed(SCHEMA_LEVEL_MODULES))
    + ["schemas", "database_roles", "databases", "external_volumes"]
    + list(
        reversed(
            [
                "storage_integrations",
                "api_integrations",
                "catalog_integrations",
                "notification_integrations",
                "security_integrations",
            ]
        )
    )
    + [
        "compute_pools",
        "warehouses",
        "resource_monitors",
        "access_tokens",
        "credentials",
        "users",
        "roles",
        "network_policies",
        "network_rules",
        "account_parameters",
        "account",
    ]
)


def run_cleanup(
    client: Any,
    neo4j_session: neo4j.Session,
    update_tag: int,
    accumulated_incomplete: set[str],
    accumulated_completed: set[str],
) -> None:
    """Runs node cleanup in CLEANUP_ORDER for ONLY the modules that were
    actually synced in full during THIS run (same update_tag).

    A module is cleaned up iff it is in `accumulated_completed` AND NOT in
    `accumulated_incomplete`:

    - `accumulated_completed`: union of every invocation's `completed` step
      list in this run. REQUIRED SAFETY GATE — see "WHY BOTH GATES" below.
    - `accumulated_incomplete`: union of every invocation's `incomplete`
      list — modules that DID run but could only be partially read (e.g. a
      permission gap), so their stale nodes must be kept. Mirrors
      cartography's own `_cleanup()` skip logic.

    WHY BOTH GATES: gating ONLY on `incomplete` is not safe. A 2-split
    orchestrator run (e.g. compute + integrations, fresh update_tag) would
    run cleanup across every module. Modules that never ran in that
    execution (tables, views, users, roles, ...) never report anything, so
    are not "incomplete" — cleanup would treat "not run" as "read in full,
    found nothing" and delete every node lacking the new tag: all tables,
    views, users, roles, schemas and databases. "Not run" and "complete"
    are different states; only the latter is safe to clean.
    The orchestrator's own fail-fast (any failed split aborts before
    cleanup) covers HARD failures only — it cannot cover modules that were
    simply never requested, which is what this gate does.
    """
    common_job_parameters = _common_job_parameters(client, update_tag)
    for name in CLEANUP_ORDER:
        if name not in accumulated_completed:
            logger.info(
                "Skipping Snowflake %s cleanup: module was not synced in this "
                "run (update_tag=%s), so its nodes were not re-observed.",
                name,
                update_tag,
            )
            continue
        if name in accumulated_incomplete:
            logger.warning(
                "Skipping Snowflake %s cleanup: this run could not read the "
                "whole surface, so stale nodes are kept rather than deleted.",
                name,
            )
            continue
        module = importlib.import_module(f"cartography.intel.snowflake.{name}")
        module.cleanup(neo4j_session, common_job_parameters)


def run_grants_cleanup(
    ctx: Context, client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    """grants.cleanup() — the MatchLink edge cleanup, with its own
    three-way completeness gate (object grants, role assignments, inherited
    grants), combining BOTH grants.sync() and account_grants.sync()'s
    results exactly as start_snowflake_ingestion() does. Requires
    run_grants() and run_account_grants() to have populated
    ctx.grants_complete/role_assignments_complete/inherited_grants_complete/
    account_grants_complete first — in the SAME invocation, since these are
    plain booleans on ctx, not threaded via the orchestrator's accumulated
    `incomplete` set the way run_cleanup()'s module-skip list is.

    Deliberately NOT auto-resolved the way schema-scoped prerequisites are
    (see _resolve_steps_with_prerequisites) — "grants" is expensive
    (minutes on a large account) and silently re-running it just to satisfy
    this step would be wasteful and surprising. Instead this raises loudly
    if "grants" and "account_grants" were not ALSO explicitly requested in
    this same invocation's step list. "grants", "account_grants", and
    "grants_cleanup" must always be grouped into the SAME orchestrator
    split/invocation — see ../README.md "Default splits", which already
    groups them together for exactly this reason.
    """
    if ctx.grants_complete is None or ctx.account_grants_complete is None:
        raise ValueError(
            "grants_cleanup requires 'grants' and 'account_grants' to have "
            "run in THIS SAME invocation (found "
            f"grants_complete={ctx.grants_complete!r}, "
            f"account_grants_complete={ctx.account_grants_complete!r}). "
            "Add 'grants' and 'account_grants' to this invocation's step "
            "list before 'grants_cleanup' — see run_grants_cleanup's "
            "docstring for why this is not auto-resolved."
        )

    from cartography.intel.snowflake import grants

    grants.cleanup(
        neo4j_session,
        client.account_id,
        update_tag,
        object_grants_complete=bool(ctx.grants_complete) and bool(ctx.account_grants_complete),
        role_assignments_complete=bool(ctx.role_assignments_complete),
        inherited_grants_complete=bool(ctx.inherited_grants_complete),
    )


def run_sync_complete(
    client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    """Writes the same ModuleSyncMetadata "receipt" node a normal unsplit
    CLI sync writes as its last action — mirrors
    ../okta-lambda-loader/src/handler.py's own "sync_complete" step exactly
    (see that handler's docstring for the full rationale). Snowflake's
    start_snowflake_ingestion() itself does NOT currently call
    merge_module_sync_metadata — this
    step is NEW bookkeeping this Lambda split adds, not a gap-fill for
    something the real CLI path already does. Worth filing upstream
    eventually so the CLI path gets the same receipt node Okta's already
    has; not yet done. Placed in its own split, LAST, same as Okta's."""
    from cartography.stats import get_stats_client
    from cartography.util import merge_module_sync_metadata

    merge_module_sync_metadata(
        neo4j_session,
        group_type="SnowflakeAccount",
        group_id=client.account_id,
        synced_type="SnowflakeAccount",
        update_tag=update_tag,
        stat_handler=get_stats_client(__name__),
    )


# ======================================================================
# Step dispatch table and canonical order
# ======================================================================

ALL_STEPS_IN_ORDER = (
    ["account", "account_parameters", "databases", "schemas", "network_rules", "network_policies"]
    + ["roles", "database_roles", "users", "access_tokens", "credentials"]
    + ["resource_monitors", "warehouses", "compute_pools", "external_volumes"]
    + [
        "storage_integrations",
        "api_integrations",
        "catalog_integrations",
        "notification_integrations",
        "security_integrations",
    ]
    + SCHEMA_LEVEL_MODULES
    + ["external_access_integrations"]
    + WORKLOAD_MODULES
    + DATA_SHARING_MODULES
    + ["policy_references", "grants", "account_grants", "grants_cleanup", "cleanup", "sync_complete"]
)

# Steps whose runner takes the (ctx, client, neo4j_session, update_tag)
# shape (every per-module step). "cleanup" and "sync_complete" are handled
# specially in run_step() below since they don't fit this shape (cleanup
# needs the accumulated incomplete set, not ctx; sync_complete needs
# neither ctx nor a completeness return).
STEP_RUNNERS: dict[str, Any] = {
    name: globals()[f"run_{name}"]
    for name in ALL_STEPS_IN_ORDER
    if name not in ("cleanup", "sync_complete")
}


# ======================================================================
# Cross-invocation prerequisite auto-resolution
# ======================================================================
#
# Context (above) is built FRESH inside every single handler() call — a
# Lambda container's state (and certainly a brand-new container's) does NOT
# persist ctx.databases/ctx.schema_list/etc. across SEPARATE invocations.
# Running a whole prerequisite chain in ONE process (e.g. the local test
# harness) hides this; the orchestrator's split, where e.g. "tables" is in a
# DIFFERENT invocation than "schemas" (see ../README.md "Default splits"),
# exposes it. Without this, that invocation's fresh Context would have
# ctx.schema_list = None, and run_tables() would crash passing None where
# schemas.sync() expects a list.
#
# So any step requiring prerequisite OUTPUT (not just ordering) from an
# earlier step gets that prerequisite chain silently re-run first, within
# THIS invocation, if the needed ctx field is still empty. This trades a
# small amount of redundant work (databases+schemas are quick) for
# correctness that does not depend on which other steps happened to already
# run in the same invocation. grants_cleanup is the one deliberate
# EXCEPTION — see its own docstring for why it raises instead of silently
# re-running the expensive grants sync.
PREREQUISITES: dict[str, list[str]] = {
    "schemas": ["databases"],
    "network_rules": ["databases", "schemas"],
    "network_policies": ["account_parameters", "databases", "schemas", "network_rules"],
    "database_roles": ["databases"],
    "access_tokens": ["users"],
    "grants": ["roles", "users", "databases", "database_roles"],
    **{name: ["databases", "schemas"] for name in SCHEMA_LEVEL_MODULES},
    **{name: ["databases", "schemas"] for name in WORKLOAD_MODULES},
}

# Which Context attribute each step's own run_* function actually populates
# — used to detect whether a prerequisite has ALREADY run in this
# invocation (and so should not be silently re-run a second time).
_PRODUCES_CTX_FIELD: dict[str, str] = {
    "databases": "databases",
    "schemas": "schema_list",
    "account_parameters": "account_network_policy",
    "network_rules": "network_rule_list",
    "roles": "role_list",
    "database_roles": "database_role_list",
    "users": "humans",  # humans is set alongside service_users; either is a fine proxy
}


def _resolve_steps_with_prerequisites(steps: list[str]) -> list[str]:
    """Expand a requested step list into prerequisite-first run order,
    inserting any missing prerequisite step immediately before the step
    that needs it, de-duplicated. Does NOT reorder or duplicate a
    prerequisite that is already present in `steps` — only fills genuine
    gaps, so a normal orchestrator split that already lists its steps in
    correct dependency order is returned completely unchanged."""
    resolved: list[str] = []
    seen: set[str] = set()

    def add(step: str) -> None:
        if step in seen:
            return
        for prereq in PREREQUISITES.get(step, []):
            add(prereq)
        seen.add(step)
        resolved.append(step)

    for step in steps:
        add(step)
    return resolved


def run_step(
    step: str,
    ctx: Context,
    client: Any,
    neo4j_session: neo4j.Session,
    update_tag: int,
) -> None:
    """Dispatch one step by name. "cleanup" and "sync_complete" are handled
    here directly since their signatures differ from the common per-module
    shape every other step shares."""
    if step == "cleanup":
        run_cleanup(client, neo4j_session, update_tag, ctx.incomplete, ctx.completed)
        return
    if step == "sync_complete":
        run_sync_complete(client, neo4j_session, update_tag)
        return
    # Skip re-running a prerequisite step that already populated its ctx
    # field earlier in THIS SAME invocation (e.g. a split that legitimately
    # lists "databases" before "schemas" itself, or "schemas" twice because
    # two different requested steps both needed it as a prerequisite).
    produces = _PRODUCES_CTX_FIELD.get(step)
    if produces is not None and getattr(ctx, produces) is not None:
        logger.info("Step '%s' already satisfied in this invocation; skipping re-run.", step)
        return
    runner = STEP_RUNNERS[step]
    runner(ctx, client, neo4j_session, update_tag)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Lambda entrypoint.

    Input: {"steps": [...], "update_tag"?: <int>, "incomplete"?: [...]}.
    See module docstring for the full contract and why "incomplete" exists.
    """
    start = time.time()
    requested_steps: list[str] = event.get("steps") or []
    completed: list[str] = []
    result: dict[str, Any] = {
        "status": "FAILED",
        "completed": completed,
        "remaining": list(requested_steps),
    }

    if not requested_steps:
        result["error"] = "event.steps must be a non-empty list"
        result["durationSeconds"] = round(time.time() - start, 1)
        return result

    unknown = [s for s in requested_steps if s not in ALL_STEPS_IN_ORDER]
    if unknown:
        result["error"] = f"Unknown step(s) {unknown}. Valid: {ALL_STEPS_IN_ORDER}."
        result["durationSeconds"] = round(time.time() - start, 1)
        return result

    # Expand with any missing cross-invocation prerequisites — see
    # _resolve_steps_with_prerequisites's own docstring for why this exists
    # (a real bug: Context does not persist across separate Lambda
    # invocations, so a split like "tables" alone needs "databases"/
    # "schemas" silently re-run first). A split that already lists its
    # steps in correct order is returned unchanged.
    steps = _resolve_steps_with_prerequisites(requested_steps)
    if steps != requested_steps:
        logger.info(
            "Expanded requested steps %s with missing prerequisites -> %s",
            requested_steps,
            steps,
        )
    result["remaining"] = list(steps)

    update_tag = event.get("update_tag")
    if update_tag is None:
        update_tag = int(time.time())
        logger.info("No update_tag provided - starting a NEW run with tag %s", update_tag)
    else:
        update_tag = int(update_tag)
        logger.info("Continuing existing run with shared update_tag %s", update_tag)
    result["updateTag"] = update_tag

    incomplete_from_earlier = set(event.get("incomplete") or [])
    # Modules completed by EARLIER invocations of this same run — the
    # orchestrator passes the union in for the "cleanup" invocation. Absent
    # (e.g. an old caller) means nothing is cleaned: the safe default. See
    # run_cleanup's docstring "WHY BOTH GATES".
    completed_from_earlier = set(event.get("completed") or [])

    driver = None
    try:
        credentials = _get_snowflake_credentials()
        client = _build_snowflake_client(credentials)
        driver = _get_neo4j_driver()
        ctx = Context(
            incomplete_from_earlier_invocations=incomplete_from_earlier,
            completed_from_earlier_invocations=completed_from_earlier,
        )

        with driver.session(
            database=os.environ.get("NEO4J_DATABASE") or None
        ) as neo4j_session:
            for step in steps:
                logger.info("Running step '%s' (update_tag=%s)", step, update_tag)
                run_step(step, ctx, client, neo4j_session, update_tag)
                completed.append(step)
                ctx.completed.add(step)

        result["remaining"] = []
        result["status"] = "SUCCEEDED"
        result["incomplete"] = sorted(ctx.incomplete)
    except Exception as exc:  # noqa: BLE001 - surface any failure to the caller
        remaining_after_failure = steps[len(completed) :]
        logger.exception(
            "Step failed. completed=%s remaining=%s.", completed, remaining_after_failure
        )
        result["remaining"] = remaining_after_failure
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)

    return result
