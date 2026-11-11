"""
Cartography AWS sync — Lambda handler.

Calls cartography's *Python API directly* (not the CLI) for a single AWS
account, for a caller-supplied list of `--aws-requested-syncs`-style module
names. This intentionally bypasses `cartography.cli`/`cartography.sync` (the
CLI/Sync-stage machinery designed for a long-running host process) and instead
replicates the small slice of `cartography.intel.aws.start_aws_ingestion()`
that is needed to sync one account's requested modules — see README.md
"Calls cartography's Python API, not the CLI" for the rationale.

Invocation contract (event):
{
  "account_id": "111111111111",      # required — target AWS account ID
  "include": ["iam"],                # module names to run, EXPLICIT — same
                                      # vocabulary as --aws-requested-syncs (e.g.
                                      # "ec2:instance"). No auto-include of
                                      # dependent modules (e.g. requesting
                                      # "ec2:load_balancer_v2" does NOT
                                      # auto-add "ec2:load_balancer_v2:expose" —
                                      # list both explicitly if you want both).
                                      # This deliberately does not replicate
                                      # cartography.intel.aws._normalize_requested_syncs()'s
                                      # auto-include behavior, which is a
                                      # private/unversioned function — see
                                      # README.md "Module selection:
                                      # include / exclude".
  "exclude": [],                     # module names to OMIT from DEFAULT_SYNCS
                                      # Ignored if "include" is
                                      # non-empty. Exactly one of
                                      # include/exclude may be non-empty —
                                      # the state machine (template.yaml)
                                      # validates this and fails fast,
                                      # before ever invoking this Lambda, if
                                      # both are populated.
  "role_arn": "arn:aws:iam::111111111111:role/cartography-read-only",
                                      # optional — omit to scan the Lambda's own
                                      # account using its execution role directly
                                      # (hub-account behavior)
  "regions": ["us-east-1"]           # optional — omit to auto-discover via
                                      # ec2:DescribeRegions
}

Returns: {"accountId", "syncs", "durationSeconds", "status", "error"?}

Neo4j connection info and credentials come from environment variables (set by
the SAM template from SSM parameters), not from the event, so the state
machine cannot leak them into execution history/logs.
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import boto3
import neo4j

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Quiet the very chatty neo4j driver notification logger; the account loop
# already logs what we need at INFO.
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

# AWS permission-relationships (RPR) mapping file, owned by THIS repo
# (lambda-loader/permission_relationships.yaml), not the vendored fork's
# cartography/data/ copy. The Makefile packages it next to handler.py, so in
# Lambda it resolves to /var/task/permission_relationships.yaml. Absolute on
# purpose: permission_relationships.parse_permission_relationships_file()
# joins a relative path onto os.getcwd(). The env var is only an override
# for local runs/tests; template.yaml does not set it.
#
# Lookup: next to handler.py (the Lambda build layout), else one level up
# (the repo layout: src/handler.py, lambda-loader/permission_relationships.yaml)
# so tests and local runs from the repo use the same file without an override.
def _default_permission_relationships_file() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    packaged = os.path.join(here, "permission_relationships.yaml")
    repo = os.path.join(os.path.dirname(here), "permission_relationships.yaml")
    return packaged if os.path.isfile(packaged) or not os.path.isfile(repo) else repo


PERMISSION_RELATIONSHIPS_FILE: str = os.environ.get(
    "PERMISSION_RELATIONSHIPS_FILE", _default_permission_relationships_file()
)

# ---------------------------------------------------------------------------
# Module dispatch table: see _build_sync_functions(). It starts from
# upstream's full `cartography.intel.aws.resources.RESOURCE_FUNCTIONS` map
# and replaces a few entries with the functions below. Each replacement's
# signature matches upstream exactly:
#   (neo4j_session, boto3_session, regions, current_aws_account_id,
#    update_tag, common_job_parameters) -> None
# ---------------------------------------------------------------------------


# Account-scoped replacement for the one unscoped job in upstream
# cleanup_iam(). Upstream builds it from AWSManagedPolicySchema, which has no
# sub_resource_relationship (one AWSManagedPolicy node per ARN, shared by
# every account), so the generated query is:
#   MATCH (n:AWSManagedPolicy)<-[r:POLICY]-(:AWSPrincipal)
#   WHERE r.lastupdated <> $UPDATE_TAG ... DELETE r
# i.e. EVERY account's POLICY edges to AWS- and customer-managed policies that
# this invocation's update_tag didn't touch. With one update_tag per Lambda
# invocation and accounts synced concurrently, each account's iam cleanup
# wiped every other account's edges (after a full run, only the last
# account synced had any). Here the edge is only deleted if the
# principal belongs to $AWS_ID. Every POLICY edge to an AWSManagedPolicy is
# written by its principal's own account's sync (principal ARN match), so
# this deletes exactly the stale edges this account owns.
SCOPED_MANAGED_POLICY_EDGE_CLEANUP = """
MATCH (:AWSAccount {id: $AWS_ID})-[:RESOURCE]->(:AWSPrincipal)-[r:POLICY]->(:AWSManagedPolicy)
WHERE r.lastupdated <> $UPDATE_TAG
WITH r LIMIT $LIMIT_SIZE
DELETE r
"""


def _cleanup_iam_scoped(
    neo4j_session: neo4j.Session,
    common_job_parameters: dict[str, Any],
) -> None:
    """Upstream cartography.intel.aws.iam.cleanup_iam(), same jobs in the same
    order, with ONE change: the AWSManagedPolicySchema job is replaced by
    SCOPED_MANAGED_POLICY_EDGE_CLEANUP (see its comment). Everything else is
    the upstream code path. tests/test_cleanup_iam_scoped.py fails if
    upstream's cleanup_iam() job list changes, so a fork-ref bump can't
    silently diverge from this copy. Drop this once upstream scopes it.

    Note what upstream's AWSManagedPolicySchema job does NOT do, and so this
    doesn't either: delete stale AWSManagedPolicy NODES (shared, no owner).
    """
    from cartography.graph.job import GraphJob
    from cartography.graph.statement import GraphStatement
    # Same names, same objects iam.py's cleanup_iam() uses.
    from cartography.intel.aws.iam import (
        _get_policies_in_current_account,
        AWSFederatedPrincipalSchema,
        AWSGroupSchema,
        AWSInlinePolicySchema,
        AWSMfaDeviceSchema,
        AWSPolicyStatementSchema,
        AWSRoleSchema,
        AWSRootPrincipalSchema,
        AWSSAMLProviderSchema,
        AWSServerCertificateSchema,
        AWSServicePrincipalSchema,
        AWSUserSchema,
    )

    # 1. Statements of policies in this account (upstream, unchanged).
    for policy_id in _get_policies_in_current_account(
        neo4j_session, common_job_parameters["AWS_ID"]
    ):
        GraphJob.from_node_schema(
            AWSPolicyStatementSchema(),
            {**common_job_parameters, "POLICY_ID": policy_id},
        ).run(neo4j_session)

    # 2. AWSManagedPolicy POLICY edges: SCOPED (the only change).
    GraphJob(
        "Cleanup AWSManagedPolicy POLICY edges (account-scoped)",
        [
            GraphStatement(
                SCOPED_MANAGED_POLICY_EDGE_CLEANUP,
                parameters={
                    "AWS_ID": common_job_parameters["AWS_ID"],
                    "UPDATE_TAG": common_job_parameters["UPDATE_TAG"],
                },
                iterative=True,
                iterationsize=10000,
                parent_job_name="AWSManagedPolicy",
                parent_job_sequence_num=1,
            )
        ],
        "AWSManagedPolicy",
    ).run(neo4j_session)

    # 3. The rest, upstream order, unchanged.
    for schema in (
        AWSInlinePolicySchema(),
        AWSRoleSchema(),
        AWSFederatedPrincipalSchema(),
        AWSServicePrincipalSchema(),
        AWSMfaDeviceSchema(),
        AWSUserSchema(),
        AWSGroupSchema(),
        AWSServerCertificateSchema(),
        AWSSAMLProviderSchema(),
        AWSRootPrincipalSchema(),
    ):
        GraphJob.from_node_schema(schema, common_job_parameters).run(neo4j_session)


# Upstream cleanup_iam()'s job sequence this copy mirrors, by schema label, in
# order (policy statements first, managed-policy job second). Checked against
# the vendored cleanup_iam() source by tests/test_cleanup_iam_scoped.py.
CLEANUP_IAM_UPSTREAM_SEQUENCE: tuple[str, ...] = (
    "AWSPolicyStatementSchema",
    "AWSManagedPolicySchema",
    "AWSInlinePolicySchema",
    "AWSRoleSchema",
    "AWSFederatedPrincipalSchema",
    "AWSServicePrincipalSchema",
    "AWSMfaDeviceSchema",
    "AWSUserSchema",
    "AWSGroupSchema",
    "AWSServerCertificateSchema",
    "AWSSAMLProviderSchema",
    "AWSRootPrincipalSchema",
)


def _iam_core_sync(
    neo4j_session: neo4j.Session,
    boto3_session: boto3.Session,
    regions: list[str],
    current_aws_account_id: str,
    update_tag: int,
    common_job_parameters: dict[str, Any],
) -> None:
    """Everything upstream `cartography.intel.aws.iam.sync()` does EXCEPT
    `sync_service_last_accessed_details()` — see `_iam_service_access_sync()`
    below and README.md "`iam` split" for why. Calls the same importable sub-functions
    `iam.sync()` itself calls, in the same order, straight from cartography's
    source — this is not a fork or a reimplementation, just a different
    caller.
    """
    from cartography.intel.aws.iam import (
        get_account_authorization_details,
        sync_root_principal,
        sync_users,
        sync_groups,
        sync_roles,
        sync_assumerole_relationships,
        sync_user_access_keys,
        get_saml_providers,
        load_saml_providers,
        sync_server_certificates,
        sync_account_summary,
    )
    from cartography.intel.aws.util.arns import get_account_partition
    from cartography.util import merge_module_sync_metadata
    from cartography.stats import get_stats_client

    logger.info("Syncing IAM (core) for account '%s'.", current_aws_account_id)
    # Root principal ARN is partition-aware (GovCloud support).
    sync_root_principal(
        neo4j_session, current_aws_account_id, update_tag,
        get_account_partition(regions, boto3_session.region_name),
    )
    # Fetch every principal's policies once via
    # GetAccountAuthorizationDetails and share a managed-policy cache across
    # users/groups/roles, exactly as upstream iam.sync() now does. Returns
    # None without iam:GetAccountAuthorizationDetails, in which case the
    # sync_* calls fall back to the old per-principal API calls.
    policy_statement_cache: dict[str, Any] = {}
    authorization_details = get_account_authorization_details(
        boto3_session, policy_statement_cache
    )
    sync_users(
        neo4j_session, boto3_session, current_aws_account_id, update_tag,
        common_job_parameters, policy_statement_cache, authorization_details,
    )
    sync_groups(
        neo4j_session, boto3_session, current_aws_account_id, update_tag,
        common_job_parameters, policy_statement_cache, authorization_details,
    )
    sync_roles(
        neo4j_session, boto3_session, current_aws_account_id, update_tag,
        common_job_parameters, policy_statement_cache, authorization_details,
    )
    # Deliberately OMITTED: sync_service_last_accessed_details() — the slow,
    # serial, per-principal AWS Access Advisor polling loop. See
    # _iam_service_access_sync() and README.md.
    sync_assumerole_relationships(
        neo4j_session, current_aws_account_id, update_tag, common_job_parameters,
    )
    sync_user_access_keys(
        neo4j_session, boto3_session, current_aws_account_id, update_tag,
        common_job_parameters,
    )
    saml_providers = get_saml_providers(boto3_session)
    load_saml_providers(neo4j_session, saml_providers, current_aws_account_id, update_tag)
    sync_server_certificates(
        neo4j_session, boto3_session, current_aws_account_id, update_tag,
        common_job_parameters,
    )
    sync_account_summary(neo4j_session, boto3_session, current_aws_account_id, update_tag)
    # NOT upstream cleanup_iam(): its AWSManagedPolicy edge cleanup has no
    # account filter. See _cleanup_iam_scoped().
    _cleanup_iam_scoped(neo4j_session, common_job_parameters)
    merge_module_sync_metadata(
        neo4j_session,
        group_type="AWSAccount",
        group_id=current_aws_account_id,
        synced_type="AWSPrincipal",
        update_tag=update_tag,
        stat_handler=get_stats_client(__name__),
    )


def _iam_service_access_sync(
    neo4j_session: neo4j.Session,
    boto3_session: boto3.Session,
    regions: list[str],
    current_aws_account_id: str,
    update_tag: int,
    common_job_parameters: dict[str, Any],
) -> None:
    """The slow phase split out of `iam.sync()`: for every AWSUser/AWSRole/
    AWSGroup in the account, calls AWS IAM Access Advisor
    (generate_service_last_accessed_details + a polling get_ call, ~2s
    minimum AWS-side latency per principal) to find each principal's most
    recently used service.

    This phase alone runs at about 2.5s/principal serial, so accounts with
    350+ principals exceed Lambda's 900s cap on this phase alone even
    though the REST of iam.sync() finishes in under 2 minutes. See
    README.md "`iam` split".

    THREADING: `get_service_last_accessed_details()` is safe to parallelize
    — it's a pure function (boto3_session in, dict out), creates its own
    boto3 client internally, and its only cost is I/O wait (the AWS-side
    polling loop's `time.sleep(2)` calls). Neo4j `Session` objects are NOT
    thread-safe, so all `load_service_last_accessed_details()` writes stay
    on the calling thread — only the slow AWS API polling is parallelized,
    via a small ThreadPoolExecutor (see IAM_SERVICE_ACCESS_MAX_WORKERS).
    """
    from cartography.intel.aws.iam import get_service_last_accessed_details
    from cartography.intel.aws.iam import load_service_last_accessed_details
    from cartography.graph.job import GraphJob
    from cartography.models.aws.iam.principal_service_access import (
        AWSPrincipalServiceAccessSchema,
    )

    logger.info(
        "Syncing IAM service-last-accessed details for account '%s'.",
        current_aws_account_id,
    )

    principals_query = """
    MATCH (account:AWSAccount{id: $AWS_ACCOUNT_ID})-[:RESOURCE]->(principal)
    WHERE principal:AWSUser OR principal:AWSRole OR principal:AWSGroup
    RETURN principal.arn as arn
    """
    results = neo4j_session.run(
        principals_query, AWS_ACCOUNT_ID=current_aws_account_id
    )
    principal_arns = [record["arn"] for record in results]

    max_workers = int(os.environ.get("IAM_SERVICE_ACCESS_MAX_WORKERS", "2"))
    logger.info(
        "Found %d principals to process for service last accessed details "
        "(max_workers=%d)",
        len(principal_arns),
        max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_arn = {
            pool.submit(get_service_last_accessed_details, boto3_session, arn): arn
            for arn in principal_arns
        }
        for future in as_completed(future_to_arn):
            arn = future_to_arn[future]
            try:
                service_details = future.result()
            except Exception:
                logger.exception(
                    "Error getting service last accessed details for %s", arn
                )
                continue
            if service_details:
                # Neo4j write stays on this (the main) thread — driver
                # Sessions are not thread-safe.
                load_service_last_accessed_details(
                    neo4j_session, service_details, arn, current_aws_account_id,
                    update_tag,
                )

    GraphJob.from_node_schema(
        AWSPrincipalServiceAccessSchema(), common_job_parameters
    ).run(neo4j_session)


def _statement_action_may_match(statement: dict[str, Any], permissions: list[str]) -> bool:
    """Could this (compiled) statement apply to ANY of `permissions`,
    ignoring its resource clauses? Mirrors upstream's action predicates
    exactly: statement_applies_to_permission() requires
    `not evaluate_notaction_for_permission(...) and evaluate_action_for_permission(...)`,
    so a statement that fails this for every permission can never match any
    (permission, resource) pair and contributes nothing to
    principal_allowed_on_resource() or collect_edge_conditions().
    """
    from cartography.intel.aws.permission_relationships import (
        evaluate_action_for_permission,
        evaluate_notaction_for_permission,
    )

    return any(
        not evaluate_notaction_for_permission(statement, p)
        and evaluate_action_for_permission(statement, p)
        for p in permissions
    )


def _prune_principals_for_permissions(
    principals: dict[str, dict[str, list[dict[str, Any]]]],
    permissions: list[str],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Drop what can't affect the result for this RPR entry, before the
    O(principals x resources x statements) evaluation:
      1. statements whose action can't match any of `permissions`
         (Allow or Deny: an irrelevant Deny can't deny, an irrelevant Allow
         can't grant, and neither is read by collect_edge_conditions());
      2. policies left with no statements (they'd return (False, False));
      3. principals left with no Allow statement (can't be granted anything).
    Output is identical to evaluating the unpruned set: same edges, same
    has_condition/condition_keys/conditions. Order of remaining statements
    and policies is preserved. Inputs are not mutated.
    """
    pruned: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for principal_arn, policies in principals.items():
        kept: dict[str, list[dict[str, Any]]] = {}
        has_allow = False
        for policy_id, statements in policies.items():
            relevant = [s for s in statements if _statement_action_may_match(s, permissions)]
            if relevant:
                kept[policy_id] = relevant
                has_allow = has_allow or any(s.get("effect") == "Allow" for s in relevant)
        if has_allow:
            pruned[principal_arn] = kept
    return pruned


def _permission_relationships_prefiltered_sync(
    neo4j_session: neo4j.Session,
    boto3_session: boto3.Session,
    regions: list[str],
    current_aws_account_id: str,
    update_tag: int,
    common_job_parameters: dict[str, Any],
) -> None:
    """Replaces upstream `permission_relationships.sync()`. Same upstream
    functions, same order, same arguments, with ONE added step: each RPR
    entry's principals are passed through _prune_principals_for_permissions()
    before calculate_permission_relationships(). Output is unchanged; only
    work that can't produce or block an edge is skipped.

    Why: upstream checks every principal x every target ARN x every
    statement. With SSM parameters as targets, one account with ~280
    principals and ~3,400 parameters has ~1M principal/resource pairs, and
    its permission_relationships split timed out at 900s (SSM CAN_READ
    alone ~825s). Offline on that account's policies: SSM CAN_READ 96s ->
    7.5s (41 of 277 principals left), CAN_WRITE 93s -> 0.2s (15 of 277),
    identical edges. See README.md "Permission mapping".
    tests/test_permission_relationships_prefilter.py checks equivalence and
    that upstream sync() hasn't changed shape.
    """
    from cartography.intel.aws.permission_relationships import (
        calculate_permission_relationships,
        cleanup_rpr,
        get_principals_for_account,
        get_resource_arns,
        is_valid_rpr,
        load_principal_mappings,
        parse_permission_relationships_file,
    )

    logger.info(
        "Syncing Permission Relationships (prefiltered) for account '%s'.",
        current_aws_account_id,
    )
    principals = get_principals_for_account(neo4j_session, current_aws_account_id)
    pr_file = common_job_parameters["permission_relationships_file"]
    if not pr_file:
        # Unreachable via handler() (it always sets a path and checks it);
        # kept to match upstream's contract for direct callers.
        logger.warning("Permission relationships file was not specified, skipping.")
        return
    relationship_mapping = parse_permission_relationships_file(pr_file)
    for rpr in relationship_mapping:
        if not is_valid_rpr(rpr):
            raise ValueError(f"Resource permission relationship is missing fields: {rpr}")
        permissions = rpr["permissions"]
        relationship_name = rpr["relationship_name"]
        target_label = rpr["target_label"]
        resource_arns = get_resource_arns(
            neo4j_session,
            current_aws_account_id,
            target_label,
            rpr.get("target_precondition"),
        )
        start = time.time()
        relevant = _prune_principals_for_permissions(principals, permissions)
        allowed_mappings = calculate_permission_relationships(
            relevant,
            resource_arns,
            permissions,
        )
        logger.info(
            "Relationship '%s' for '%s': %d of %d principals relevant, %d targets, "
            "%d edges, evaluated in %.1fs",
            relationship_name,
            target_label,
            len(relevant),
            len(principals),
            len(resource_arns),
            len(allowed_mappings),
            time.time() - start,
        )
        load_principal_mappings(
            neo4j_session,
            allowed_mappings,
            target_label,
            relationship_name,
            update_tag,
        )
        cleanup_rpr(
            neo4j_session,
            target_label,
            relationship_name,
            update_tag,
            current_aws_account_id,
        )


def _inspector_self_only_sync(
    neo4j_session: neo4j.Session,
    boto3_session: boto3.Session,
    regions: list[str],
    current_aws_account_id: str,
    update_tag: int,
    common_job_parameters: dict[str, Any],
) -> None:
    """Replaces upstream `inspector.sync()` for ALL accounts (not a
    delegated-admin-specific branch). Skips `get_member_accounts()` — the
    delegated-admin-only `list_members` API call upstream's `sync()` makes
    once per region before always appending `current_aws_account_id` to
    whatever it returns — and calls `_sync_findings_for_account()` directly,
    once per region, for `current_aws_account_id` only.

    Why this is safe for every account, not just the delegated admin: a regular member
    account's `list_members()` call already returns `[]` (it has no
    delegated visibility), so upstream's loop was already a no-op membership
    list for every non-admin caller — the Inspector delegated admin is
    the ONLY account where that call does anything, and what it
    does is pure duplicate work once every org account independently syncs
    its own findings via its own Lambda invocation. Removing the call is therefore a strict
    speedup (one fewer AWS API round-trip) for every account, and a
    correctness fix (no more redundant re-fetch/re-write of every other
    account's findings) specifically for the delegated admin.

    Region filtering/looping and the cleanup-safety bookkeeping
    (`cleanup_safe`, skipped on a transient per-region connection failure to
    preserve last-known-good findings) are copied verbatim from upstream
    `inspector.sync()` — unchanged. Only the account-enumeration layer
    (`get_member_accounts()` + the loop over `member_accounts`) is removed;
    every account still only ever gets its own findings synced, exactly as
    before for non-admin accounts, and newly-correctly for the admin.

    Does NOT change outcomes for accounts whose own Inspector finding volume
    already exceeds the Lambda's 900s budget in isolation — this
    function removes a redundant API call and redundant re-sync of OTHER
    accounts' findings, it does not speed up a single account's own finding
    volume. That per-account data-volume problem is a separate, unresolved
    issue.

    RISK NOTE (higher risk tier than the `iam` split above): calls
    `_sync_findings_for_account` — a PRIVATE, underscore-prefixed function,
    unlike `_iam_core_sync`/`_iam_service_access_sync`'s calls to iam.py's
    public sub-functions. Python convention marks this as internal-only; no
    stability commitment from cartography's maintainers about its continued
    existence or signature. Mitigated the same way as the rest of this
    handler: the fork is pinned to an exact tag (CARTOGRAPHY_GIT_REF in
    Makefile), so nothing breaks silently on an upstream change — a future
    tag bump is the deliberate point at which this function's continued
    existence/signature would be checked. Failure mode if cartography ever
    removes/renames it is a loud ImportError at Lambda cold start, not
    silent wrong behavior.
    """
    from cartography.intel.aws.inspector import (
        AWS_INSPECTOR_REGIONS,
        _sync_findings_for_account,
        cleanup,
    )

    batch_size = common_job_parameters.get("experimental_aws_inspector_batch")
    inspector_regions = [r for r in regions if r in AWS_INSPECTOR_REGIONS]
    cleanup_safe = True
    for region in inspector_regions:
        logger.info(
            "Syncing AWS Inspector findings (self only, no delegated-admin "
            "member-account loop) for account %s and region %s",
            current_aws_account_id,
            region,
        )
        kwargs: dict[str, Any] = {}
        if batch_size is not None:
            kwargs["batch_size"] = batch_size
        synced = _sync_findings_for_account(
            neo4j_session,
            boto3_session,
            region,
            current_aws_account_id,
            update_tag,
            current_aws_account_id,
            **kwargs,
        )
        if not synced:
            cleanup_safe = False
    if cleanup_safe:
        common_job_parameters["ACCOUNT_ID"] = current_aws_account_id
        common_job_parameters["UPDATE_TAG"] = update_tag
        cleanup(neo4j_session, common_job_parameters)
    else:
        logger.warning(
            "Skipping AWS Inspector cleanup for account %s because one or "
            "more regions were transiently skipped. Preserving "
            "last-known-good data.",
            current_aws_account_id,
        )


def _build_sync_functions() -> dict[str, Any]:
    """Import cartography lazily (inside the handler's cold start, not at
    module load time controlled by AWS) so a broken/oversized dependency
    tree fails inside a Lambda invocation with a clear traceback in
    CloudWatch, rather than as an opaque Lambda INIT_REPORT failure.

    Sourced from cartography.intel.aws.resources.RESOURCE_FUNCTIONS — the
    same dispatch table cartography's own CLI/--aws-requested-syncs path
    uses — rather than hand-listing individual module imports one at a
    time. This is the full ~50-module dict cartography ships (every module
    key it supports, in the correct sync order for dependencies like
    kms-before-s3), not a hand-picked subset. Every entry has the identical
    (neo4j_session, boto3_session, regions, current_aws_account_id,
    update_tag, common_job_parameters) signature this handler calls with.

    "iam" is overridden to NOT be the upstream `iam.sync` — split into two
    separately-invocable entries (see _iam_core_sync / _iam_service_access_sync
    above) so the fast, always-valuable IAM data (roles/users/groups/
    policies/assume-role trust) never times out on an account whose IAM
    Access Advisor enrichment alone would exceed Lambda's 15-minute cap.

    "inspector" is overridden to skip the delegated-admin member-account
    enumeration loop (see _inspector_self_only_sync above) — applied
    unconditionally to every account, not just the delegated-admin
    one, since the skipped list_members() call was already a no-op for
    every non-admin caller.
    """
    from cartography.intel.aws.resources import RESOURCE_FUNCTIONS

    sync_functions = dict(RESOURCE_FUNCTIONS)
    sync_functions["iam"] = _iam_core_sync
    sync_functions["iam:service_access"] = _iam_service_access_sync
    sync_functions["inspector"] = _inspector_self_only_sync
    sync_functions["permission_relationships"] = _permission_relationships_prefiltered_sync
    return sync_functions


# DEFAULT_SYNCS INCLUDES "inspector". This Lambda calls
# inspector.sync() once PER ACCOUNT via the orchestrator's per-account
# fan-out. Confirmed
# (AWS docs, "Understanding the delegated administrator account and member
# account in Amazon Inspector") that non-admin member accounts are
# permitted to call inspector2:ListFindings for their own findings — the
# module's sync() calls list_members first (delegated-admin-only; returns
# [] via @aws_handle_regions's AccessDenied handling for a non-admin
# caller) then always appends the calling account itself, so each
# member-account invocation naturally syncs just its own findings with no
# code change required.
#
# Ordered to match cartography.intel.aws.resources.RESOURCE_FUNCTIONS's
# canonical OrderedDict order (e.g. `kms` immediately after `iam`, before
# `s3`/`rds`; `ec2:vpc_endpoint` before `ec2:route_table`). `resources.py`'s
# own comment is
# explicit that iteration order IS the enforced dependency order ("Module
# dependencies are enforced by iterating over this dict in order, even when
# users request a subset of modules"); `kms` before `s3`/`rds`/etc. is
# specifically so `ENCRYPTED_BY` edges can match an already-existing
# `AWSKMSKey` node by ARN. Running `s3` before `kms` in a single pass means
# any bucket encrypted with a KMS key that hasn't been synced by an EARLIER
# invocation gets no edge this cycle — safe to fix across invocations (see
# README.md "Why order matters, and why splitting is safe") but NOT safe to leave mis-ordered within one invocation, since
# there's no later step in the same pass that re-visits `s3`. When adding
# modules, keep this list in resources.py order.
# "resourcegroupstaggingapi" is NOT in DEFAULT_SYNCS. Its _load_tags_tx
# write hits Neo.TransientError.Transaction.DeadlockDetected under
# concurrent writers (MERGE on shared AWSTag nodes keyed by "Key:Value" —
# hot nodes like "Environment:prod" are written by many accounts/regions at
# once). On one org, MaxConcurrency=10 produced hundreds of deadlock events
# and 15 caused account timeouts. It runs as a SEPARATE, low-concurrency
# orchestrator pass instead of mixed into the main per-account fan-out —
# see README.md "Orchestrator" (sync-override input) / the
# OrchestratorStateMachine's CheckSyncOverrideInput branch in template.yaml.
# Still a fully valid `include` value for cartography-lambda-sync directly —
# NOT removed from RESOURCE_FUNCTIONS/_build_sync_functions(), only from the
# default set a normal (no-override) run executes.
# "permission_relationships" is moved from after "cloudformation" (upstream
# RESOURCE_FUNCTIONS position) to the END. It only
# reads the graph; its targets are whatever (:AWSAccount)-[:RESOURCE]->(:X)
# nodes exist when it runs. In the upstream position it ran before
# secretsmanager, ssm and ecs, so their CAN_*/GET_SECRET edges lagged a day.
# Every module it targets (s3, dynamodb, secretsmanager, redshift, rds, ecs,
# iam, cloudformation, ec2:instance + ssm instance info, ssm parameters, kms)
# now runs before it. In account-splits, each split account runs it in its
# own final split for the same reason; see README.md "Permission mapping".
# Defined in splits.py (one copy, shared with the account-splits validator).
from splits import DEFAULT_SYNCS  # noqa: E402
from splits import validate as validate_account_splits  # noqa: E402


def _resolve_requested_syncs(
    include: list[str] | None, exclude: list[str] | None
) -> list[str]:
    """Resolves the event's include/exclude arrays into the actual ordered
    list of sync module names to run.

    Fully explicit — deliberately does NOT replicate
    cartography.intel.aws._normalize_requested_syncs()'s auto-include
    behavior (e.g. auto-adding "ec2:load_balancer_v2:expose" when
    "ec2:load_balancer_v2" is requested). That function is private
    (leading underscore, no __all__, not called from anywhere outside its
    own module) and its one auto-include rule is not something this handler
    commits to keeping in sync with upstream — if a caller wants both
    modules, it must list both explicitly. See README.md.

    include non-empty -> run exactly those modules (validated against
      _build_sync_functions()'s dispatch table elsewhere; this function
      does not validate module names itself).
    include empty, exclude non-empty -> run DEFAULT_SYNCS minus exclude.
    both empty -> run DEFAULT_SYNCS as-is.
    both non-empty -> the STATE MACHINE rejects this before invoking the
      Lambda at all (see template.yaml ValidateInput). This function does
      not re-validate that here; if it's ever reached with both non-empty
      (e.g. a direct Lambda invoke bypassing the state machine), include
      wins, matching the precedence documented in the event contract above.

    Order is always DEFAULT_SYNCS's order (the same dependency-correct
    order cartography.intel.aws.resources.RESOURCE_FUNCTIONS defines,
    e.g. kms-before-s3/rds for ENCRYPTED_BY edges) for the exclude and
    default paths. For an explicit `include`, the order the caller supplied
    is preserved as given — an explicit include list is assumed to already
    be dependency-ordered by whoever wrote it.
    """
    include = include or []
    exclude = exclude or []
    if include:
        return list(dict.fromkeys(include))  # order-preserving dedup
    return [s for s in DEFAULT_SYNCS if s not in set(exclude)]


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


def _resolve_role_arn(account_id: str, explicit_role_arn: str | None) -> str | None:
    """Hub-vs-spoke role resolution, moved here (out of the caller/state
    machine) so `{"account_id": ..., "syncs": [...]}` is a self-contained
    invocation the caller doesn't need to reason about roles for.

    An explicit `role_arn` in the event always wins (ad-hoc/debug override).
    Otherwise: the hub account (HUB_ACCOUNT_ID) uses the Lambda's own
    execution role directly (no assume-role needed, it already has
    SecurityAudit). Every other account assumes
    CARTOGRAPHY_READ_ONLY_ROLE_NAME, the org-wide StackSet role
    (../stackset/)."""
    if explicit_role_arn:
        return explicit_role_arn
    hub_account_id = os.environ.get("HUB_ACCOUNT_ID", "")
    if account_id == hub_account_id:
        return None
    role_name = os.environ.get("CARTOGRAPHY_READ_ONLY_ROLE_NAME", "cartography-read-only")
    return f"arn:aws:iam::{account_id}:role/{role_name}"


def _get_boto3_session(role_arn: str | None, region: str) -> boto3.Session:
    """Does the assume-role in-process instead of via a generated AWS CLI
    profile file, since there's no persistent filesystem to write
    per-account config files to in Lambda."""
    if not role_arn:
        # Hub account: use the Lambda's own execution role directly (no
        # assume-role).
        return boto3.Session(region_name=region)

    sts = boto3.client("sts")
    creds = sts.assume_role(
        RoleArn=role_arn,
        RoleSessionName="cartography-lambda-loader",
        DurationSeconds=int(os.environ.get("ASSUME_ROLE_DURATION_SECONDS", "3600")),
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )


def _autodiscover_regions(boto3_session: boto3.Session) -> list[str]:
    from cartography.intel.aws.ec2 import get_ec2_regions

    return get_ec2_regions(boto3_session)


def _lookup_account_name(account_id: str) -> str:
    """Resolves the account's real friendly name via
    organizations:DescribeAccount, called with the Lambda's own execution
    role (the hub account's role has this permission org-wide — no
    assume-role needed).

    Why a lookup instead of accepting `account_name` in the event: keeps the
    Lambda invocable standalone with just {"account_id", "syncs"} — no
    caller is required to already know/pass the name. The cost is one extra
    AWS API call per invocation.

    Falls back to the account_id itself (previous behavior) if the lookup
    fails for any reason (e.g. throttling, the account was just deleted from
    the org) — the sync should not fail outright just because the display
    name couldn't be resolved; it degrades to the same behavior this handler
    had before this fix.
    """
    try:
        org_client = boto3.client("organizations")
        response = org_client.describe_account(AccountId=account_id)
        return response["Account"]["Name"]
    except Exception:
        logger.warning(
            "Could not resolve account name for %s via organizations:DescribeAccount; "
            "falling back to using the account ID as the name.",
            account_id,
            exc_info=True,
        )
        return account_id


def _check_permission_relationships_file(path: str) -> None:
    """Fail the invocation if the RPR mapping is missing or would map nothing.

    Upstream permission_relationships.sync() does not raise for these: a
    missing file logs a warning and maps nothing, and an empty file loops
    over nothing. Both look like a green run with zero CAN_* edges. Raising
    here turns that into a FAILED execution with a clear cause. Runs before
    any sync module, so a broken package fails fast instead of after `iam`.
    Validates each entry with the module's own is_valid_rpr(), which sync()
    only checks after it has already started.
    """
    import yaml
    from cartography.intel.aws.permission_relationships import is_valid_rpr

    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Permission relationships file not found: {path}. It should be "
            "packaged by lambda-loader/Makefile next to handler.py."
        )
    with open(path) as f:
        mapping = yaml.safe_load(f)
    if not isinstance(mapping, list) or not mapping:
        raise ValueError(f"Permission relationships file {path} has no entries.")
    invalid = [rpr for rpr in mapping if not isinstance(rpr, dict) or not is_valid_rpr(rpr)]
    if invalid:
        raise ValueError(
            f"Permission relationships file {path} has invalid entries: {invalid}"
        )


def _ensure_account_node(
    neo4j_session: neo4j.Session,
    account_id: str,
    update_tag: int,
    regions: list[str],
    fallback_region: str | None,
) -> None:
    """Creates the (:AWSAccount)-[:RESOURCE]->(:AWSRootPrincipal) scaffold that
    every per-account sync function assumes already exists. Mirrors
    organizations.load_aws_accounts() for a single account, since this
    handler syncs one account per invocation and does not run the full
    org-wide organizations.sync() stage.

    Looks up the account's real name (see _lookup_account_name) rather than
    writing the account_id as a placeholder name — doing that silently
    clobbers the real name written by organizations.sync() with the raw
    account ID on every Lambda-driven sync.
    """
    from cartography.intel.aws.organizations import load_aws_account_nodes_from_config
    from cartography.intel.aws.iam import sync_root_principal
    from cartography.intel.aws.util.arns import get_account_partition

    account_name = _lookup_account_name(account_id)
    load_aws_account_nodes_from_config(
        neo4j_session,
        [{"id": account_id, "name": account_name, "foreign": None}],
        update_tag,
    )
    # `partition` is required (GovCloud support). Same derivation
    # upstream iam.sync() uses, so the root principal ARN written here
    # matches the one _iam_core_sync() writes.
    sync_root_principal(
        neo4j_session, account_id, update_tag,
        get_account_partition(regions, fallback_region),
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    # Validate-only mode: {"validate_account_splits": <config>}. No AWS, no
    # Neo4j. Used by `make splits-put` (and later the orchestrator) so the
    # deployed rules judge a config before it's stored or run.
    if "validate_account_splits" in event:
        errors = validate_account_splits(event["validate_account_splits"])
        return {"status": "INVALID" if errors else "VALID", "errors": errors}

    start = time.time()
    account_id = event["account_id"]
    requested_syncs: list[str] = _resolve_requested_syncs(
        event.get("include"), event.get("exclude")
    )
    role_arn: str | None = event.get("role_arn")
    regions_override: list[str] | None = event.get("regions")

    result: dict[str, Any] = {
        "accountId": account_id,
        "syncs": requested_syncs,
        "status": "FAILED",
    }

    sync_functions = _build_sync_functions()
    unknown = [s for s in requested_syncs if s not in sync_functions]
    if unknown:
        result["error"] = (
            f"Unsupported sync module(s) {unknown} for this Lambda "
            f"loader. Supported: {sorted(sync_functions)}. See README.md to add more."
        )
        result["durationSeconds"] = round(time.time() - start, 1)
        return result

    driver = None
    try:
        boto3_session = _get_boto3_session(role_arn, os.environ.get("AWS_HOME_REGION", "us-east-1"))
        regions = regions_override or _autodiscover_regions(boto3_session)

        driver = _get_neo4j_driver()
        update_tag = int(time.time())
        common_job_parameters = {
            "UPDATE_TAG": update_tag,
            "AWS_ID": account_id,
            # cartography.intel.aws.permission_relationships.sync() does a
            # direct (non-.get()) dict lookup on this key and raises KeyError
            # if it's absent. It must also be a real path: a falsy value
            # makes sync() log a warning and return early, and a missing
            # file makes it log and map nothing. Either way the module
            # "succeeds" with zero CAN_* edges, silently. The CLI's
            # --permission-relationships-file defaults to the bundled yaml,
            # so a default CLI run DOES build edges; this matches that, but
            # with our own file. See README.md "Permission mapping".
            "permission_relationships_file": PERMISSION_RELATIONSHIPS_FILE,
        }
        if "permission_relationships" in requested_syncs:
            _check_permission_relationships_file(PERMISSION_RELATIONSHIPS_FILE)

        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as neo4j_session:
            _ensure_account_node(
                neo4j_session, account_id, update_tag, regions,
                boto3_session.region_name,
            )

            for sync_name in requested_syncs:
                logger.info(
                    "Running sync module '%s' for account %s (regions=%s)",
                    sync_name,
                    account_id,
                    regions,
                )
                sync_functions[sync_name](
                    neo4j_session,
                    boto3_session,
                    regions,
                    account_id,
                    update_tag,
                    common_job_parameters,
                )

        result["status"] = "SUCCEEDED"
        result["updateTag"] = update_tag
        result["regions"] = regions
    except Exception as exc:  # noqa: BLE001 — surface any failure to the state machine
        logger.exception("Sync failed for account %s", account_id)
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)

    return result
