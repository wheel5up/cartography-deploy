"""
Cartography Okta sync — Lambda handler.

Calls cartography's Python API directly (not the CLI), same approach as
../lambda-loader/src/handler.py uses for AWS. Okta is a single-tenant
module (one org, one API token) — there is no per-account fan-out. Instead,
a single Okta sync is broken into STEPS (cartography's own existing
sub-sync functions, called in the same order start_okta_ingestion() already
calls them) and, for the one step too big to fit in one invocation
(`applications`), further into BATCHES within that step.

THIS HANDLER DOES NOT DECIDE HOW TO GROUP STEPS ACROSS INVOCATIONS. That is
the orchestrator state machine's job (template.yaml's
CartographyOktaLambdaOrchestrator), using a FIXED, pre-defined split list
(same pattern as ../lambda-loader's account-splits.json) — not a runtime
decision made by this handler. An earlier version of this handler tried to
make that decision itself, checking its own remaining Lambda time and
stopping early when running low — that was REJECTED (see README.md "Why
fixed splits, not a runtime time-budget loop") because it made
invocation counts unpredictable run-to-run and, more importantly, provided
no real benefit over simply pre-computing the same groupings by hand from
the measured timing data below: either way, a step/batch that runs
unexpectedly long can still hit the hard 900s Lambda timeout mid-step, so
the runtime check bought predictability-loss without buying real safety.
This handler's only job is: run exactly the steps it's given, in order,
start to finish, and fail loudly if any one of them fails.

======================================================================
WHY THIS EXISTS: measured timing
======================================================================
A plain, unmodified `start_okta_ingestion()` call does NOT finish within
Lambda's 900s cap for a mid-sized org. Serial timing for one org with about
1,300 users and 370 apps:

  organization+users+groups+user_types  ~270s
  applications (all apps)              ~1,170s  <- exceeds 900s ALONE
  factors (all users)                   ~270s
  origins+authenticators+awssaml        unmeasured, but org-level config
                                         reads, not per-item loops — low
                                         risk, not split

`applications` is the step that cannot fit in a single invocation: even
with a fully fresh 900s budget and zero other work, it needs more. It must
be split into multiple invocations.

A CONCURRENCY-BASED FIX WAS TRIED FIRST AND REJECTED. Pre-fetching
multiple groups/apps at once (bounded by a semaphore) was implemented,
tested locally, and deployed — but a real run revealed it made things
WORSE, not better: Okta enforces a real per-token
rate limit on these endpoints (confirmed via Okta's own docs: a single API
token is capped at 50% of a bucket's org-wide limit by default), and
firing even 2 requests at once reliably triggered it. The SDK's own
automatic backoff/retry (capped at 2 attempts by default) could not
recover reliably at that error rate, causing silent multi-minute stalls
with no further progress and no visible error (the SDK's own rate-limit
log line is disabled by default — see _build_okta_client's docstring).
Concurrency was removed entirely. All Okta API calls in this handler are
SERIAL, exactly matching cartography's own unmodified behavior — the fix
here is splitting WORK ACROSS INVOCATIONS over time, not parallelizing it.

======================================================================
DESIGN: steps, batches, one shared update_tag, fail loudly
======================================================================
This handler is a SINGLE-TASK wrapper: given a list of steps, it runs ALL
of them, start to finish, in one invocation — it does not decide to stop
partway through. Grouping steps into invocation-sized chunks (e.g. "which
applications batches go together") is the ORCHESTRATOR's job (a fixed,
pre-defined split list in template.yaml — see
CartographyOktaLambdaOrchestrator), not something this handler figures out
at runtime. See "THIS HANDLER DOES NOT DECIDE..." above for why an earlier
runtime-self-checkpointing design was tried and rejected.

Invocation contract (event):
{
  "steps": ["users", "groups", "applications:100:200", ...],
  "update_tag": 1792017089   # REQUIRED except on the very first invocation
                              # of a run — see "Why one shared update_tag"
}

Returns:
{
  "status": "SUCCEEDED" | "FAILED",
  "completed": [...],   # steps that finished, in order
  "remaining": [...],   # ALWAYS [] on SUCCEEDED (every requested step ran).
                          # On FAILED, the steps that were never attempted —
                          # present for diagnostics only; per "Why a batch
                          # failure must FAIL THE WHOLE RUN" below, a
                          # FAILED result is NOT meant to be retried by
                          # feeding "remaining" back in as-is.
  "updateTag": 1792017089,
  "durationSeconds": ...,
  "error"?: "..."
}

### Why one shared update_tag across ALL invocations of one run

cartography's `start_okta_ingestion()` normally generates exactly ONE
update_tag per run and passes that SAME tag to every sub-sync. This
matters because of a legacy cleanup step
(`cartography.intel.okta.cleanup_okta_groups`, called at the very end of
`start_okta_ingestion()` for backward compatibility) that deletes any
`OktaGroup` node NOT stamped with the tag in that final call. If each
invocation generated its OWN fresh tag independently, the LAST
invocation's legacy cleanup would see every group written by EARLIER
invocations (stamped with a different, now "wrong" tag) as stale and
delete them all — a real, silent data-loss bug, not a theoretical one.
Fix: the first invocation of a run generates the tag and returns it in its
result; the caller (the state machine) must pass that same tag as
"update_tag" in every subsequent invocation of the same run. This handler
does not generate a fresh tag on any invocation where "update_tag" is
already provided in the event.

### Why `applications` needs explicit batches AND a separate cleanup step

Okta's API has no bulk endpoint for app user/group assignments — fetching
them is a real per-app, serial cost (see timing above). To split this
work, the full app list is paginated by whole result pages (not by
arbitrary item count — see _list_applications_batch's docstring for why),
batches named `applications:<start>:<end>` processing one page range at a
time.

cartography's real, unmodified `sync_okta_applications()` ALWAYS does
"write this batch" and "delete anything not in this batch" together, as
one call. Calling it per-batch as-is would make batch 1 delete every app
NOT in batch 1 (apps 101+), since they have no tag yet — not stale data,
just not-yet-processed data. This handler therefore calls
cartography's lower-level (still intended-for-reuse, but underscore-
prefixed/private) transform+load functions per batch WITHOUT their
coupled cleanup, and only runs cleanup via a separate, explicit
`"applications:cleanup"` step once every batch has been confirmed to have
already succeeded. This is a deliberate, narrow exception to "only call
cartography's public per-module sync_* functions" for every other step in
this handler — flagged here explicitly, not hidden.

### Why a batch failure must FAIL THE WHOLE RUN, not continue

If any `applications:<n>:<m>` batch fails partway through a run, cleanup
is NEVER attempted — there is no "cleanup is safe because most batches
succeeded" judgment call here, and no silent skip-and-continue. The
calling state machine is responsible for treating this invocation's
"FAILED" status as a hard stop for the whole run (do not advance to
remaining steps, do not run "applications:cleanup"). Recovering from a
partial `applications` failure means re-running the ENTIRE applications
step sequence from `applications:0:100` onward with a FRESH update_tag —
not resuming the partial batch list with the old tag. This was a
deliberate, explicit decision: an earlier draft of this design treated a
partial batch failure as "keep already-synced data, skip cleanup, treat
as a soft success" (mirroring how the AWS loader's Inspector module
handles a transient per-region failure) — that pattern was REJECTED here
because it can silently mask a real, ongoing failure as green ("partial
failure treated as success" logic can hide an outage for months).

### What does NOT change

Every step other than the `applications` batches/cleanup calls
cartography's real, PUBLIC, unmodified sync_* function directly — no
patching, no reimplementation, no concurrency. `organization`, `users`,
`groups`, `user_types`, `origins`, `authenticators`, `factors`, `devices`,
`log_streams`, `network_zones`, `api_tokens`, `policies`, `coverage`,
`awssaml` all run exactly as they do inside a normal, non-split
`start_okta_ingestion()` call. (`policies` re-lists application IDs itself,
since applications are batched across invocations; `coverage` requires
`organization`/`users`/`groups`/`log_streams` earlier in the same
invocation — see _run_simple_step.)

Upstream's `_cleanup_okta_organizations()` (legacy migration cleanup job +
`cleanup_okta_groups`) is NOT called here.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import boto3
import neo4j

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Quiet the very chatty neo4j driver notification logger; cartography's own
# sync logging already covers what we need at INFO.
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

# Page size for the applications list call. 200 is the Okta API's documented
# max for this endpoint (same limit cartography's own _get_okta_applications
# uses). Batches are named by PAGE RANGE (e.g. "applications:0:200"), not by
# arbitrary item count, because Okta's pagination is cursor-based (an opaque
# "after" token), not offset-based — there is no way to jump directly to
# "item 150" without first walking the pages before it. See
# _list_applications_batch's docstring.
APPLICATIONS_PAGE_SIZE = 100

# The fixed step order this handler supports, matching
# cartography.intel.okta.__init__.start_okta_ingestion()'s own call
# sequence exactly. "applications" itself is never a valid step name here —
# callers must use "applications:<start>:<end>" batches (see
# _parse_applications_batch_step) and the separate "applications:cleanup"
# step once all batches succeed.
KNOWN_SIMPLE_STEPS = frozenset(
    {
        "organization",
        "users",
        "groups",
        "user_types",
        "origins",
        "authenticators",
        "factors",
        "devices",
        "log_streams",
        "network_zones",
        "api_tokens",
        "policies",
        "coverage",
        "awssaml",
        "applications:cleanup",
        "sync_complete",
    }
)

# Steps whose outputs "coverage" needs, and which therefore MUST run earlier
# in the SAME invocation as "coverage" (see the "coverage" branch of
# _run_simple_step). cartography returns these values from the
# sync_* calls instead of persisting them anywhere, so there is no way to
# recover them in a later invocation.
COVERAGE_PREREQUISITES = ("organization", "users", "groups", "log_streams")


def _get_okta_api_key() -> str:
    """Fetches the Okta API token from SSM at invoke time (with decryption)
    rather than from a Lambda environment variable. Environment variables
    are visible in plaintext to anyone with lambda:GetFunctionConfiguration,
    which would defeat the point of storing the token as a SecureString in
    the first place.
    OKTA_TOKEN_SSM_PARAM is just the parameter NAME, set at deploy time; it
    is not sensitive."""
    ssm = boto3.client("ssm")
    param_name = os.environ.get("OKTA_TOKEN_SSM_PARAM", "/cartography/okta/token")
    response = ssm.get_parameter(Name=param_name, WithDecryption=True)
    return response["Parameter"]["Value"]


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


def _build_okta_client(okta_api_key: str) -> Any:
    """Builds the Okta SDK client used for every step in this invocation.

    Explicitly ensures the SDK's own internal logger ("okta-sdk-python") is
    enabled at INFO, set AFTER construction — OktaClient.__init__() calls
    the SDK's setup_logging(), which unconditionally sets
    logging.getLogger("okta-sdk-python").disabled = True regardless of any
    logger configuration done BEFORE the client is built (confirmed by
    reading okta/logger.py and okta/client.py directly). The SDK's own
    rate-limit backoff message ("Hit rate limit. Retry request in %s
    seconds.") logs at INFO on exactly this logger — if left disabled, a
    real, multi-minute rate-limit backoff is completely invisible in
    CloudWatch (see README.md "All Okta API calls are serial"). Setting
    this AFTER
    OktaClient(...) is constructed is required; setting it before does
    nothing, since the constructor resets it.
    """
    from okta.client import Client as OktaClient

    client = OktaClient(
        {
            "orgUrl": f"https://{os.environ['OKTA_ORG_ID']}.{os.environ.get('OKTA_BASE_DOMAIN', 'okta.com')}",
            "token": okta_api_key,
        }
    )
    okta_logger = logging.getLogger("okta-sdk-python")
    okta_logger.disabled = False
    okta_logger.setLevel(logging.INFO)
    return client


def _parse_applications_batch_step(step: str) -> tuple[int, int] | None:
    """Parses "applications:<start>:<end>" into (start, end) page-offset
    ints, e.g. "applications:100:200" -> (100, 200). Returns None for any
    other step string (including the bare "applications", which is
    deliberately NOT a valid step name — callers must always specify an
    explicit batch range or "applications:cleanup")."""
    parts = step.split(":")
    if len(parts) != 3 or parts[0] != "applications":
        return None
    try:
        return int(parts[1]), int(parts[2])
    except ValueError:
        return None


async def _list_applications_page_range(
    okta_client: Any, start: int, end: int
) -> list[Any]:
    """Returns the applications in page-offset range [start, end).

    IMPORTANT: Okta's list_applications pagination is CURSOR-based (an
    opaque "after" token from the previous response), not offset-based —
    confirmed directly from the SDK's own method signature
    (ApplicationApi.list_applications has no "offset" parameter, only
    "after": an opaque string "treat...as an opaque value obtained through
    the next link relationship"). There is no way to jump straight to "the
    101st application" without first walking every page before it. This
    function therefore ALWAYS re-walks pages from the beginning up to
    `start` before actually collecting [start, end) — cheap, since listing
    (not per-app enrichment) is fast (seconds for a few hundred apps),
    unlike the per-app assignment calls this is deliberately
    avoiding re-doing out of range.
    """
    from cartography.intel.okta.applications import _get_okta_applications

    # _get_okta_applications collects the FULL list in one call (it already
    # paginates internally via collect_paginated). Re-fetching the full
    # list per batch is simpler and still cheap relative to the per-app
    # work; slicing happens in-memory after.
    all_applications = await _get_okta_applications(okta_client)
    return all_applications[start:end]


def _run_applications_batch(
    okta_client: Any,
    neo4j_session: neo4j.Session,
    common_job_parameters: dict[str, Any],
    start: int,
    end: int,
) -> int:
    """Processes ONE batch of applications: list (re-walked, see
    _list_applications_page_range), transform, and load — WITHOUT running
    cleanup. Calls cartography's own transform/load functions directly
    (private to cartography.intel.okta.applications, but unmodified,
    called with their own documented signatures) — see this module's
    docstring "Why `applications` needs explicit batches AND a separate
    cleanup step" for why cleanup cannot run per-batch.

    Returns the number of applications processed in this batch (for
    logging/diagnostics only).
    """
    import asyncio

    from cartography.intel.okta.applications import _load_okta_applications
    from cartography.intel.okta.applications import _load_okta_reply_uris
    from cartography.intel.okta.applications import _transform_okta_applications
    from cartography.intel.okta.applications import _transform_okta_reply_uris

    applications = asyncio.run(
        _list_applications_page_range(okta_client, start, end)
    )
    logger.info(
        "Processing applications batch [%d:%d): %d applications",
        start,
        end,
        len(applications),
    )
    transformed = _transform_okta_applications(okta_client, applications)
    _load_okta_applications(neo4j_session, transformed, common_job_parameters)
    reply_uris = _transform_okta_reply_uris(applications)
    _load_okta_reply_uris(neo4j_session, reply_uris, common_job_parameters)
    return len(applications)


def _count_real_okta_applications(okta_client: Any) -> int:
    """Returns the CURRENT total number of applications Okta reports for
    this org, via the same listing call the batches themselves use
    (_get_okta_applications, already paginates internally). This is the
    "ground truth" _verify_applications_batch_coverage checks the Neo4j
    node count against before cleanup is allowed to run."""
    import asyncio

    from cartography.intel.okta.applications import _get_okta_applications

    applications = asyncio.run(_get_okta_applications(okta_client))
    return len(applications)


def _count_synced_okta_applications(
    neo4j_session: neo4j.Session, update_tag: int
) -> int:
    """Returns how many OktaApplication nodes in Neo4j are stamped with
    THIS run's update_tag — i.e. how many applications the batches that
    ran so far actually wrote. Deliberately a plain node count, not scoped
    to the OktaOrganization (matching how cartography's own
    OktaApplicationSchema has no org-level sub_resource_relationship
    restricting which org an OktaApplication belongs to in this single-org
    deployment)."""
    result = neo4j_session.run(
        "MATCH (n:OktaApplication) WHERE n.lastupdated = $update_tag "
        "RETURN count(n) AS c",
        update_tag=update_tag,
    )
    return result.single()["c"]


def _verify_applications_batch_coverage(
    okta_client: Any, neo4j_session: neo4j.Session, update_tag: int
) -> None:
    """THE FIX FOR A REAL GAP: the applications batch boundaries
    (applications:0:100, :100:200, etc.) are a FIXED, hand-written list —
    see template.yaml's DefaultSplitsJson. Nothing about that fixed list
    automatically adapts if the org's real application count grows past
    what the list covers (e.g. if applications:300:400 is the last batch
    and the org grows past 400 apps, apps 401+ would be silently never
    synced, with no error, forever, until someone happens to notice the
    count looks wrong — confirmed as a real, not hypothetical, risk by
    direct calculation: Python's list slicing silently returns fewer items
    than requested rather than raising, so a short final batch produces no
    error signal of its own).

    Fixes this by refusing to run cleanup — the step that would otherwise
    quietly treat an incomplete sync as fully done — unless the number of
    OktaApplication nodes actually written under this run's update_tag
    EXACTLY matches what Okta reports as the current total. A mismatch
    means the fixed batch list (as currently configured) does not cover
    every application that exists right now, and raises loudly instead of
    deleting anything or reporting success.

    Called BEFORE any delete runs, specifically so a detected mismatch
    aborts with the already-synced partial data still intact (easier to
    diagnose) rather than after cleanup has already run.
    """
    real_count = _count_real_okta_applications(okta_client)
    synced_count = _count_synced_okta_applications(neo4j_session, update_tag)
    if synced_count != real_count:
        gap = real_count - synced_count
        if gap > 0:
            # The common, expected case: Okta has MORE apps than we
            # synced — almost certainly the fixed batch list's upper bound
            # (e.g. "applications:300:400") is now below the real total,
            # so the apps past that bound were never requested by any
            # batch at all. Tell the reader the exact missing COUNT and
            # the exact RANGE to add, computed directly from the two
            # numbers already in hand — no need to go re-derive this by
            # hand from logs.
            diagnosis = (
                f"Okta has {gap} MORE application(s) than were synced "
                f"({real_count} real vs. {synced_count} synced). This is "
                f"almost certainly a batch-list COVERAGE gap, not a "
                f"transient error: the fixed applications:<n>:<m> batches "
                f"configured for this run (see template.yaml's "
                f"DefaultSplitsJson / /cartography/okta-lambda/"
                f"default-splits) stop at {synced_count}, but "
                f"{real_count} applications exist right now. "
                f"REMEDIATION: add one more batch, "
                f"'applications:{synced_count}:{real_count}' (or "
                f"'applications:{synced_count}:{synced_count + 100}' to "
                f"match the existing 100-per-batch convention and leave "
                f"some headroom), inserted BEFORE 'applications:cleanup' "
                f"in the splits list, then re-run the ENTIRE applications "
                f"sequence from 'applications:0:100' onward with a FRESH "
                f"update_tag (do not reuse {update_tag} — see module "
                f"docstring 'Why a batch failure must FAIL THE WHOLE "
                f"RUN')."
            )
        else:
            # The less likely, but possible, opposite case: synced MORE
            # than Okta currently reports. Could mean apps were deleted in
            # Okta mid-run (a batch synced an app that no longer exists by
            # the time this count ran), or a batch somehow double-counted.
            # Not a batch-coverage-gap fix in this direction — flagged
            # distinctly so a reader doesn't try to "add a batch" for a
            # problem that isn't a coverage gap at all.
            diagnosis = (
                f"Neo4j has {-gap} MORE OktaApplication node(s) stamped "
                f"with this run's update_tag than Okta currently reports "
                f"({synced_count} synced vs. {real_count} real). This is "
                f"NOT the usual 'batch list needs extending' case — likely "
                f"causes: applications were deleted in Okta WHILE this run "
                f"was in progress (a batch synced an app that no longer "
                f"exists by the time this count ran), or a bug double-"
                f"processed a batch. Investigate before re-running; adding "
                f"another applications:<n>:<m> batch will not fix this "
                f"direction of mismatch."
            )
        raise RuntimeError(
            f"applications:cleanup refusing to run (would risk deleting "
            f"or under-counting real data): {diagnosis}"
        )
    logger.info(
        "applications:cleanup coverage check passed: %d/%d applications "
        "synced under update_tag=%d",
        synced_count,
        real_count,
        update_tag,
    )


def _run_applications_cleanup(
    okta_client: Any,
    neo4j_session: neo4j.Session,
    common_job_parameters: dict[str, Any],
) -> None:
    """Runs ONLY the cleanup/delete-stale-data half of
    sync_okta_applications(), via cartography's own private cleanup
    functions. Must only ever be invoked after EVERY applications batch for
    this update_tag has already succeeded — see this module's docstring
    "Why a batch failure must FAIL THE WHOLE RUN, not continue". This
    handler does not itself track "did every batch succeed" across
    invocations; that is the calling state machine's responsibility (by
    construction: a batch that fails makes this handler return
    status=FAILED, and the state machine must treat that as a hard stop
    that never reaches the "applications:cleanup" step).

    ALSO verifies full coverage of the CURRENT real application count
    before allowing cleanup to proceed — see
    _verify_applications_batch_coverage's docstring for the real gap this
    closes (a fixed, hand-written batch list silently not covering org
    growth). A batch-count mismatch raises here and cleanup never runs,
    even if every batch that WAS run reported success individually.
    """
    from cartography.intel.okta.applications import _cleanup_okta_applications
    from cartography.intel.okta.applications import _cleanup_okta_reply_uris

    _verify_applications_batch_coverage(
        okta_client, neo4j_session, common_job_parameters["UPDATE_TAG"]
    )
    _cleanup_okta_applications(neo4j_session, common_job_parameters)
    _cleanup_okta_reply_uris(neo4j_session, common_job_parameters)


def _run_simple_step(
    step: str,
    okta_client: Any,
    neo4j_session: neo4j.Session,
    common_job_parameters: dict[str, Any],
    state: dict[str, Any],
) -> None:
    """Runs one of the KNOWN_SIMPLE_STEPS by calling cartography's real,
    public, UNMODIFIED sync_* function for that step.

    `state` is a per-INVOCATION dict (never persisted across invocations)
    holding values one step returns that a later step in the same
    invocation needs:
      - "user_ids"            (from "users"; used by "factors")
      - "org_properties"      (from "organization"; used by "coverage")
      - "user_roles_synced"   (from "users"; used by "coverage")
      - "group_roles_synced"  (from "groups"; used by "coverage")
      - "log_streams_synced"  (from "log_streams"; used by "coverage")

    sync_okta_users() returns a (user_ids, roles_synced) TUPLE, and
    sync_okta_groups()/sync_okta_organization() return values too. Treating
    the tuple as a bare list breaks "factors": it would pass the whole ID
    list as one user_id, which the Okta SDK rejects.

    Note on `factors` needing user IDs from a prior `users` step: if
    `factors` is requested in a DIFFERENT invocation than `users`, this
    function re-fetches the user ID list itself (a few seconds)
    rather than requiring the caller to pass user IDs through the event.
    `policies` does the same for application IDs, which are never all in
    one invocation's state because applications are batched.
    """
    if step == "organization":
        from cartography.intel.okta.organization import sync_okta_organization

        # Passing okta_client also reads the Admin Console session
        # settings onto the OktaOrganization node (matches
        # start_okta_ingestion()).
        state["org_properties"] = sync_okta_organization(
            neo4j_session, common_job_parameters, okta_client
        )
        return

    if step == "users":
        from cartography.intel.okta.users import sync_okta_users

        user_ids, roles_synced = sync_okta_users(
            okta_client, neo4j_session, common_job_parameters
        )
        state["user_ids"] = user_ids
        state["user_roles_synced"] = roles_synced
        return

    if step == "groups":
        from cartography.intel.okta.groups import sync_okta_groups

        state["group_roles_synced"] = sync_okta_groups(
            okta_client, neo4j_session, common_job_parameters
        )
        return

    if step == "user_types":
        from cartography.intel.okta.users import sync_okta_user_types

        sync_okta_user_types(okta_client, neo4j_session, common_job_parameters)
        return

    if step == "origins":
        from cartography.intel.okta.origins import sync_okta_origins

        sync_okta_origins(okta_client, neo4j_session, common_job_parameters)
        return

    if step == "authenticators":
        from cartography.intel.okta.authenticators import sync_okta_authenticators

        sync_okta_authenticators(okta_client, neo4j_session, common_job_parameters)
        return

    if step == "factors":
        import asyncio

        from cartography.intel.okta.factors import sync_okta_user_factors
        from cartography.intel.okta.users import _get_okta_users

        user_ids = state.get("user_ids")
        if user_ids is None:
            logger.info(
                "'factors' requested without a prior 'users' step in this "
                "invocation; re-fetching the user ID list."
            )
            users = asyncio.run(_get_okta_users(okta_client))
            user_ids = [u.id for u in users]
            state["user_ids"] = user_ids
        sync_okta_user_factors(
            okta_client, neo4j_session, common_job_parameters, user_ids
        )
        return

    if step == "devices":
        # Needs OktaUser nodes (device ownership edges) — runs after "users".
        from cartography.intel.okta.devices import sync_okta_devices

        sync_okta_devices(okta_client, neo4j_session, common_job_parameters)
        return

    if step == "log_streams":
        from cartography.intel.okta.log_streams import sync_okta_log_streams

        state["log_streams_synced"] = sync_okta_log_streams(
            okta_client, neo4j_session, common_job_parameters
        )
        return

    if step == "network_zones":
        # Must run before "api_tokens" and "policies" (both link to zones).
        from cartography.intel.okta.network_zones import sync_okta_network_zones

        sync_okta_network_zones(okta_client, neo4j_session, common_job_parameters)
        return

    if step == "api_tokens":
        # Links to OktaUser and OktaNetworkZone — runs after both.
        from cartography.intel.okta.api_tokens import sync_okta_api_tokens

        sync_okta_api_tokens(okta_client, neo4j_session, common_job_parameters)
        return

    if step == "policies":
        # Links to OktaApplication and OktaNetworkZone — runs after
        # applications:cleanup and network_zones. Upstream passes the app ID
        # set returned by sync_okta_applications(); applications are batched
        # across invocations here, so re-list them (a few seconds).
        import asyncio

        from cartography.intel.okta.applications import _get_okta_applications
        from cartography.intel.okta.policies import sync_okta_policies

        app_ids = {a.id for a in asyncio.run(_get_okta_applications(okta_client))}
        sync_okta_policies(okta_client, neo4j_session, common_job_parameters, app_ids)
        return

    if step == "coverage":
        # Re-writes the OktaOrganization node with flags recording whether
        # the API token could read admin roles / log streams, so "token
        # lacked permission" is distinguishable from "org has none" (matches
        # start_okta_ingestion()'s record_okta_sync_coverage() call). The
        # inputs are return values from earlier steps, so they must have run
        # in THIS invocation — fail loudly rather than write wrong flags.
        from cartography.intel.okta.organization import record_okta_sync_coverage

        required = {
            "org_properties": "organization",
            "user_roles_synced": "users",
            "group_roles_synced": "groups",
            "log_streams_synced": "log_streams",
        }
        missing = [s for key, s in required.items() if key not in state]
        if missing:
            raise RuntimeError(
                f"'coverage' must run in the same invocation as, and after, "
                f"{list(COVERAGE_PREREQUISITES)}; missing in this invocation: "
                f"{missing}. Fix the splits list."
            )
        record_okta_sync_coverage(
            neo4j_session,
            common_job_parameters,
            state["org_properties"],
            {
                "log_streams_synced": state["log_streams_synced"],
                "admin_roles_synced": state["user_roles_synced"]
                and state["group_roles_synced"],
            },
        )
        return

    if step == "awssaml":
        okta_saml_role_regex = os.environ.get("OKTA_SAML_ROLE_REGEX") or None
        if okta_saml_role_regex is None:
            # UPSTREAM CARTOGRAPHY BUG: sync_okta_aws_saml() ->
            # query_for_okta_to_aws_role_mapping() ->
            # transform_okta_group_to_aws_role() -> _parse_okta_group_name()
            # -> _parse_regex() calls `mapping_regex.replace(...)`
            # UNCONDITIONALLY, with no None-guard, even though
            # config.okta_saml_role_regex defaults to None and
            # start_okta_ingestion() passes it through just as
            # unconditionally (cartography/intel/okta/__init__.py:100-102
            # — confirmed by reading the source, not assumed). This bug is
            # NOT specific to this handler's direct-API-call design; the
            # normal CLI path (without --okta-saml-role-regex) would hit
            # the exact same crash (`AttributeError: 'NoneType' object has
            # no attribute 'replace'`) once the graph contains an
            # (:OktaApplication{name:'amazon_aws'})--(:OktaGroup) edge.
            # Okta carries no fork patches (see README.md "Packaging"), so
            # the fix lives here: skip the call entirely when no regex is
            # configured rather than relying on cartography's own (buggy)
            # handling of that case.
            logger.info(
                "Skipping 'awssaml': OKTA_SAML_ROLE_REGEX is not configured "
                "(set it if your Okta groups use the AWS Account Federation "
                "SAML format). "
                "Calling sync_okta_aws_saml() with a None regex would crash "
                "on a real upstream cartography bug if any "
                "(:OktaApplication{name:'amazon_aws'})--(:OktaGroup) edge "
                "exists — confirmed by a real failure, see this branch's "
                "comment for the full writeup."
            )
            return

        from cartography.intel.okta.awssaml import sync_okta_aws_saml

        sync_okta_aws_saml(
            neo4j_session,
            okta_saml_role_regex,
            common_job_parameters["UPDATE_TAG"],
            common_job_parameters["OKTA_ORG_ID"],
        )
        return

    if step == "applications:cleanup":
        _run_applications_cleanup(okta_client, neo4j_session, common_job_parameters)
        return

    if step == "sync_complete":
        # Writes the SAME ModuleSyncMetadata "receipt" node
        # start_okta_ingestion() writes as the very last line of a normal,
        # unsplit CLI sync (cartography/intel/okta/__init__.py) — a single
        # MERGE recording "Okta synced successfully as of this update_tag",
        # used by other tooling to answer "when did Okta last sync?"
        # without inspecting individual Okta node timestamps. A comparison
        # with a plain CLI sync showed this was the ONLY difference between
        # the two paths' graph output — every Okta node/relationship count
        # matched; it's a bookkeeping node, not a data-correctness gap.
        #
        # Deliberately just another named step (not special orchestrator
        # logic) — placed in its OWN split, last, after the split
        # containing "awssaml" in the default splits list (see
        # template.yaml's DefaultSplitsJson). The "only run if everything
        # before it succeeded" guarantee is the same one every other step
        # already gets for free: the orchestrator stops the whole run the
        # moment any earlier split fails, so a later split (this one
        # included) simply never executes — no new orchestrator-specific
        # code needed.
        from cartography.stats import get_stats_client
        from cartography.util import merge_module_sync_metadata

        merge_module_sync_metadata(
            neo4j_session,
            group_type="OktaOrganization",
            group_id=common_job_parameters["OKTA_ORG_ID"],
            synced_type="OktaOrganization",
            update_tag=common_job_parameters["UPDATE_TAG"],
            stat_handler=get_stats_client(__name__),
        )
        return

    raise ValueError(f"Unknown step: {step!r}")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    start = time.time()
    steps: list[str] = event.get("steps") or []
    completed: list[str] = []
    result: dict[str, Any] = {
        "status": "FAILED",
        "completed": completed,
        "remaining": list(steps),
    }

    if not steps:
        result["error"] = "event.steps must be a non-empty list"
        result["durationSeconds"] = round(time.time() - start, 1)
        return result

    unknown = [
        s
        for s in steps
        if s not in KNOWN_SIMPLE_STEPS and _parse_applications_batch_step(s) is None
    ]
    if unknown:
        result["error"] = (
            f"Unknown step(s) {unknown}. Valid: {sorted(KNOWN_SIMPLE_STEPS)} "
            f"or 'applications:<start>:<end>' (e.g. 'applications:0:100')."
        )
        result["durationSeconds"] = round(time.time() - start, 1)
        return result

    # ONE shared update_tag for the entire run — see module docstring "Why
    # one shared update_tag across ALL invocations of one run". Only
    # generated fresh if this is the FIRST invocation of a run (no
    # update_tag provided); every subsequent invocation of the same run
    # MUST pass the same tag back in, or cartography's legacy group
    # cleanup can delete groups written by earlier invocations.
    update_tag = event.get("update_tag")
    if update_tag is None:
        update_tag = int(time.time())
        logger.info("No update_tag provided — starting a NEW run with tag %s", update_tag)
    else:
        update_tag = int(update_tag)
        logger.info("Continuing existing run with shared update_tag %s", update_tag)
    result["updateTag"] = update_tag

    driver = None
    try:
        okta_api_key = _get_okta_api_key()
        okta_client = _build_okta_client(okta_api_key)
        driver = _get_neo4j_driver()

        org_id = os.environ["OKTA_ORG_ID"]
        common_job_parameters = {
            "UPDATE_TAG": update_tag,
            "OKTA_ORG_ID": org_id,
        }

        # Per-invocation values passed between steps — see _run_simple_step.
        state: dict[str, Any] = {}

        with driver.session(
            database=os.environ.get("NEO4J_DATABASE") or None
        ) as neo4j_session:
            for step in steps:
                logger.info("Running step '%s' (update_tag=%s)", step, update_tag)
                batch_range = _parse_applications_batch_step(step)
                if batch_range is not None:
                    batch_start, batch_end = batch_range
                    _run_applications_batch(
                        okta_client,
                        neo4j_session,
                        common_job_parameters,
                        batch_start,
                        batch_end,
                    )
                else:
                    _run_simple_step(
                        step,
                        okta_client,
                        neo4j_session,
                        common_job_parameters,
                        state,
                    )
                completed.append(step)

        # This handler always attempts every step it is given — it does not
        # self-checkpoint on a time budget (see module docstring). A
        # SUCCEEDED result therefore always has an empty "remaining"; the
        # field is kept in the response shape for symmetry with the FAILED
        # case below (and so a caller doesn't need two different response
        # shapes to check against).
        result["remaining"] = []
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 — surface any failure to the caller
        remaining_after_failure = steps[len(completed):]
        logger.exception(
            "Step failed. completed=%s remaining=%s. See module docstring "
            "'Why a batch failure must FAIL THE WHOLE RUN' — the caller "
            "must treat this as a hard stop, not skip to remaining steps "
            "or run applications:cleanup.",
            completed,
            remaining_after_failure,
        )
        result["remaining"] = remaining_after_failure
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)

    return result
