"""
Cartography Cloudflare sync — Lambda handler.

Calls cartography's Python API directly (not the CLI), same approach
../okta-lambda-loader/src/handler.py, ../vercel-lambda-loader/src/handler.py,
and ../lambda-loader/src/handler.py use for Okta, Vercel, and AWS. Cloudflare
is single-tenant like Okta/Vercel (one token, walks every account the token
can see) — there is no per-org-account fan-out the way the AWS loader has.

======================================================================
WHY THIS IS A SINGLE-TASK HANDLER, NOT A STEP/BATCH SPLIT LIKE OKTA/AWS
======================================================================
Okta and AWS both needed a fixed splits list because measured timing
showed a single unsplit sync does NOT fit in Lambda's 900s cap. Cloudflare
does NOT have this problem for a small org. Three weeks of nightly full
syncs for one org (1 account, a handful of zones, ~1,200 DNS records):

    min   ~25s
    max   ~85s   (one outlier day; typical max otherwise ~45s)
    avg   ~35s

Every run used under 10% of Lambda's 900s ceiling.
start_cloudflare_ingestion() is a flat, linear walk — one pass through 9
submodules per account, no per-item loop with real growth risk the way
Okta's `applications` or AWS's `iam`/`inspector` have.

Still, every cartography.intel.cloudflare submodule call is broken out into
its own named STEP (matching the Okta/Vercel step-dispatch shape) so:

  1. A future split, if this org's Cloudflare footprint ever grows enough to
     matter, is a template.yaml config change (regrouping STEP names into
     more than one split) — no handler.py rewrite needed.
  2. A single step can be re-run / tested in isolation
     ({"steps": ["r2buckets"]}), matching ../okta-lambda-loader/README.md's
     "Run manually" per-step debugging ergonomics.

======================================================================
DESIGN: one step per cartography.intel.cloudflare submodule, PLUS the
per-account loop made explicit
======================================================================
Unlike Okta/Vercel (single tenant, no inner loop) and unlike AWS (loops over
ORG accounts, each a separate Lambda invocation), Cloudflare's
start_cloudflare_ingestion() loops over every CLOUDFLARE ACCOUNT the token
can see and, for each one, runs 8 more sub-syncs in a fixed
order. This handler flattens that into named steps exactly matching
__init__.py's call order:

    accounts            -> cartography.intel.cloudflare.accounts.sync()
                            (returns the account list; ALSO the thing every
                            later step's account_id loop iterates over)
    roles               -> roles.sync() per account
    members             -> members.sync() per account
    dnsrecord_migration -> dnsrecords.migrate_account_resource_edges() per
                            account (runs before zones — see __init__.py's
                            own comment: zone cleanup deletes stale zones,
                            which would orphan DNS records only reachable
                            through them)
    zones               -> zones.sync() per account (returns the zone list;
                            needed by dnsrecords/workerroutes/rulesets below)
    dnsrecords          -> dnsrecords.sync() per account, using THIS
                            invocation's zones if the "zones" step already
                            ran, else a fresh (idempotent) zones.get() —
                            see _StepContext, mirrors
                            ../vercel-lambda-loader/src/handler.py's
                            re-fetch-on-demand pattern
    r2buckets           -> r2buckets.sync() per account
    workerscripts       -> workerscripts.sync() per account (runs before
                            workerroutes — see __init__.py's own comment:
                            routes link to the script they invoke)
    workerroutes        -> workerroutes.sync() per account, needs zones
    rulesets            -> rulesets.sync() per account, needs zones

Every step loops over EVERY Cloudflare account the token can see,
not just the first one — this mirrors __init__.py's own outer `for account
in accounts.sync(...)` loop exactly, just with the outer loop and the inner
per-account-call now living in two different places (the "accounts" step
populates ctx.accounts once; every later step iterates ctx.accounts itself).

Every step calls cartography's real, PUBLIC, UNMODIFIED sync() function for
that submodule directly — no patching, no reimplementation, no
underscore-prefixed/private function calls anywhere in this handler (same
lower-risk posture as ../vercel-lambda-loader/src/handler.py; unlike
Okta's `applications` batch split or the AWS loader's `iam`/`inspector`
splits, which do call private functions).

======================================================================
WHY NO "sync_complete" STEP (same reasoning as Vercel)
======================================================================
Confirmed by reading cartography/intel/cloudflare/__init__.py directly:
start_cloudflare_ingestion() does NOT call merge_module_sync_metadata()
anywhere — Cloudflare does not write a ModuleSyncMetadata "receipt" node
today. Same pre-existing gap
../vercel-lambda-loader/src/handler.py's module docstring documents for
Vercel — not introduced here, not silently papered over.

======================================================================
INVOCATION CONTRACT
======================================================================
{
  "steps": ["accounts", "roles", ...],   # optional, see DEFAULT_STEPS
  "update_tag": 1792017089                # optional, see below
}

Returns:
{
  "status": "SUCCEEDED" | "FAILED",
  "completed": [...],
  "remaining": [...],
  "updateTag": 1792017089,
  "durationSeconds": ...,
  "error"?: "..."
}

`update_tag` is optional — omit it to start a brand-new run (a fresh tag is
generated and reported back); pass a prior run's `updateTag` to continue
writing under that same tag (relevant only if this is ever split across
multiple invocations in the future — a single, unsplit invocation of
DEFAULT_STEPS has no cross-invocation tag-sharing concern today, same
reasoning as ../vercel-lambda-loader/src/handler.py's module docstring).
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import boto3
import neo4j
from cloudflare import Cloudflare

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Quiet the very chatty neo4j driver notification logger; cartography's own
# sync logging already covers what we need at INFO.
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

# The fixed step order this handler supports, matching
# cartography.intel.cloudflare.__init__.start_cloudflare_ingestion()'s own
# call order exactly (see that function's source for the per-account loop
# this flattens). A normal, unsplit run requests every step in
# DEFAULT_STEPS, in this order.
DEFAULT_STEPS: list[str] = [
    "accounts",
    "roles",
    "members",
    "dnsrecord_migration",
    "zones",
    "dnsrecords",
    "r2buckets",
    "workerscripts",
    "workerroutes",
    "rulesets",
]

KNOWN_STEPS = frozenset(DEFAULT_STEPS)


def _get_cloudflare_token() -> str:
    """Fetches the Cloudflare API token from SSM at invoke time (with
    decryption) rather than from a Lambda environment variable. Environment
    variables are visible in plaintext to anyone with
    lambda:GetFunctionConfiguration, which would defeat the point of
    storing the token as a SecureString in the first place. Mirrors
    ../vercel-lambda-loader/src/handler.py's _get_vercel_api_token().
    CLOUDFLARE_TOKEN_SSM_PARAM is just the parameter NAME, set at deploy
    time; it is not sensitive."""
    ssm = boto3.client("ssm")
    param_name = os.environ.get(
        "CLOUDFLARE_TOKEN_SSM_PARAM", "/cartography/cloudflare/token"
    )
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


class _StepContext:
    """Carries the small amount of state later steps need from earlier ones
    (the `accounts` and `zones` lists the "accounts"/"zones" steps return),
    exactly mirroring ../vercel-lambda-loader/src/handler.py's _StepContext
    (which threads `projects`/`domains`/`edge_configs` the same way) so
    steps can run out of their "natural" single-call order if ever split
    across invocations later. Re-fetches on demand (a cheap, idempotent
    read) if a later step needs a list that wasn't populated by an earlier
    step in THIS invocation — e.g. "zones" requested alone, without a prior
    "accounts" step in the same invocation.

    Unlike Vercel (one team) or Okta (one org), Cloudflare's accounts list
    is itself per-INVOCATION state every later step loops over — not just a
    single id threaded through. zones, below, is keyed by account_id since
    each Cloudflare account has its own zone list.
    """

    def __init__(self) -> None:
        self.accounts: list[dict[str, Any]] | None = None
        self.zones_by_account: dict[str, list[dict[str, Any]]] = {}

    def get_accounts(self, neo4j_session: neo4j.Session, client: Cloudflare,
                      common_job_parameters: dict[str, Any]) -> list[dict[str, Any]]:
        if self.accounts is None:
            logger.info(
                "Step requested without a prior 'accounts' step in this "
                "invocation; re-running accounts.sync() to resolve the "
                "account list (idempotent — safe to call more than once "
                "per invocation)."
            )
            import cartography.intel.cloudflare.accounts as accounts_mod

            self.accounts = accounts_mod.sync(
                neo4j_session, client, common_job_parameters
            )
        return self.accounts

    def get_zones(
        self,
        neo4j_session: neo4j.Session,
        client: Cloudflare,
        common_job_parameters: dict[str, Any],
        account_id: str,
    ) -> list[dict[str, Any]]:
        if account_id not in self.zones_by_account:
            logger.info(
                "Step requested without a prior 'zones' step in this "
                "invocation for account %s; re-running zones.sync() to "
                "resolve the zone list.",
                account_id,
            )
            import cartography.intel.cloudflare.zones as zones_mod

            self.zones_by_account[account_id] = zones_mod.sync(
                neo4j_session, client, common_job_parameters, account_id=account_id
            )
        return self.zones_by_account[account_id]


def _run_step(
    step: str,
    client: Cloudflare,
    neo4j_session: neo4j.Session,
    common_job_parameters: dict[str, Any],
    ctx: _StepContext,
) -> None:
    """Runs one named step by calling cartography's real, public, UNMODIFIED
    cartography.intel.cloudflare.<module>.sync() function directly — no
    patching, no reimplementation. Every step (other than "accounts" itself)
    loops over every Cloudflare account the token can see, mirroring
    __init__.py's own outer `for account in accounts.sync(...)` loop.
    """

    if step == "accounts":
        import cartography.intel.cloudflare.accounts as accounts_mod

        ctx.accounts = accounts_mod.sync(
            neo4j_session, client, common_job_parameters
        )
        return

    accounts = ctx.get_accounts(neo4j_session, client, common_job_parameters)

    if step == "roles":
        import cartography.intel.cloudflare.roles as roles_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            roles_mod.sync(
                neo4j_session, client, account_job_parameters, account_id=account["id"]
            )
        return

    if step == "members":
        import cartography.intel.cloudflare.members as members_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            members_mod.sync(
                neo4j_session, client, account_job_parameters, account_id=account["id"]
            )
        return

    if step == "dnsrecord_migration":
        # Runs before "zones" — see __init__.py's own comment: zone cleanup
        # deletes stale zones, which would orphan DNS records only reachable
        # through them.
        import cartography.intel.cloudflare.dnsrecords as dnsrecords_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            dnsrecords_mod.migrate_account_resource_edges(
                neo4j_session, account_job_parameters
            )
        return

    if step == "zones":
        import cartography.intel.cloudflare.zones as zones_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            ctx.zones_by_account[account["id"]] = zones_mod.sync(
                neo4j_session, client, account_job_parameters, account_id=account["id"]
            )
        return

    if step == "dnsrecords":
        import cartography.intel.cloudflare.dnsrecords as dnsrecords_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            zones = ctx.get_zones(
                neo4j_session, client, account_job_parameters, account["id"]
            )
            dnsrecords_mod.sync(
                neo4j_session,
                client,
                account_job_parameters,
                account_id=account["id"],
                zones=zones,
            )
        return

    if step == "r2buckets":
        import cartography.intel.cloudflare.r2buckets as r2buckets_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            r2buckets_mod.sync(
                neo4j_session, client, account_job_parameters, account_id=account["id"]
            )
        return

    if step == "workerscripts":
        # Runs before "workerroutes" — see __init__.py's own comment: routes
        # link to the script they invoke.
        import cartography.intel.cloudflare.workerscripts as workerscripts_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            workerscripts_mod.sync(
                neo4j_session, client, account_job_parameters, account_id=account["id"]
            )
        return

    if step == "workerroutes":
        import cartography.intel.cloudflare.workerroutes as workerroutes_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            zones = ctx.get_zones(
                neo4j_session, client, account_job_parameters, account["id"]
            )
            workerroutes_mod.sync(
                neo4j_session,
                client,
                account_job_parameters,
                account_id=account["id"],
                zones=zones,
            )
        return

    if step == "rulesets":
        import cartography.intel.cloudflare.rulesets as rulesets_mod

        for account in accounts:
            account_job_parameters = {**common_job_parameters, "account_id": account["id"]}
            zones = ctx.get_zones(
                neo4j_session, client, account_job_parameters, account["id"]
            )
            rulesets_mod.sync(
                neo4j_session,
                client,
                account_job_parameters,
                account_id=account["id"],
                zones=zones,
            )
        return

    raise ValueError(f"Unknown step: {step!r}")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    start = time.time()
    steps: list[str] = event.get("steps") or list(DEFAULT_STEPS)
    completed: list[str] = []
    result: dict[str, Any] = {
        "status": "FAILED",
        "completed": completed,
        "remaining": list(steps),
    }

    unknown = [s for s in steps if s not in KNOWN_STEPS]
    if unknown:
        result["error"] = f"Unknown step(s) {unknown}. Valid: {sorted(KNOWN_STEPS)}."
        result["durationSeconds"] = round(time.time() - start, 1)
        return result

    # One update_tag for the whole invocation. See module docstring for why
    # this is future-proofing rather than a correctness requirement today.
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
        cloudflare_token = _get_cloudflare_token()
        client = Cloudflare(api_token=cloudflare_token)
        driver = _get_neo4j_driver()

        common_job_parameters: dict[str, Any] = {"UPDATE_TAG": update_tag}

        ctx = _StepContext()

        with driver.session(
            database=os.environ.get("NEO4J_DATABASE") or None
        ) as neo4j_session:
            for step in steps:
                logger.info("Running step '%s' (update_tag=%s)", step, update_tag)
                _run_step(step, client, neo4j_session, common_job_parameters, ctx)
                completed.append(step)

        result["remaining"] = []
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 — surface any failure to the caller
        remaining_after_failure = steps[len(completed):]
        logger.exception(
            "Step failed. completed=%s remaining=%s.",
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
