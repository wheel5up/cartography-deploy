"""
Cartography Vercel sync — Lambda handler.

Calls cartography's Python API directly (not the CLI), same approach
../okta-lambda-loader/src/handler.py and ../lambda-loader/src/handler.py use
for Okta and AWS. Vercel is single-tenant like Okta (one team, one API
token) — there is no per-account fan-out.

======================================================================
WHY THIS IS A SINGLE-TASK HANDLER, NOT A STEP/BATCH SPLIT LIKE OKTA
======================================================================
Okta and AWS both need a fixed splits list because a single unsplit sync
doesn't fit in Lambda's 900s cap (Okta's `applications` step alone can
exceed it). For one org of about 70 projects, 170 domains and 40,000
deployments, the full Vercel sync took 270-360s, under half the cap, so the
default splits list has one entry.

The time grows with projects and deployments. Every submodule call is still
its own named STEP (matching Okta's step-dispatch shape), so if growth ever
pushes a run near 900s, splitting is a config-only change to
DefaultSplitsJson in template.yaml, with no handler rewrite.

Where the time goes, roughly: aliases about a third, the per-project loop
(deployments, env vars, project domains, firewall configs and bypass rules)
about half, everything else a few seconds each. The per-project loop has the
most growth risk (it scales with project count AND each project's deployment
volume); if this handler is ever split, that loop is the most likely
candidate to become its own step or batch.

======================================================================
DESIGN: every cartography.intel.vercel submodule is its own named STEP
======================================================================
Unlike the simpler okta-lambda-loader (which only needed an
`applications`-shaped split), there is no single invocation-sized grouping
decision to make here today — the whole sync fits in one call. Still, EVERY
`cartography.intel.vercel.<module>.sync()` call is broken out into its own
named step (not one monolithic `start_vercel_ingestion()` call) so:

  1. A future split (if the growth trend above continues) is a
     template.yaml config change (regrouping STEP names into more than one
     split), not a handler.py rewrite.
  2. A single step can be re-run / tested in isolation
     ({"steps": ["aliases"]}), the same way ../okta-lambda-loader/README.md
     "Run manually" describes for Okta.
  3. Per-project/per-domain/per-edge-config fan-out (phases 5/6/7 of
     start_vercel_ingestion()) is still done as ONE step each
     ("project_resources", "domain_resources", "edge_config_resources") —
     not as N steps per project/domain/config — because the whole
     per-project loop fits comfortably in one invocation; splitting it
     further would only add complexity. If growth ever makes this loop the
     new "applications", revisit with measured timing first.

Every step calls cartography's real, PUBLIC, UNMODIFIED sync() function for
that submodule directly — no patching, no reimplementation. The one
necessary deviation from a byte-for-byte `start_vercel_ingestion()` replay
is that phases 3 (projects), 4 (domains/edge_configs), 6 (per-domain), and 7
(per-edge-config) all return lists later phases need (`projects`, `domains`,
`edge_configs`) — this handler threads those same lists between steps via a
small in-memory cache (`_StepContext`), exactly mirroring how
../okta-lambda-loader/src/handler.py threads `user_ids_cache` between
`users` and `factors` so steps can run out of their "natural" single-call
order if ever split across multiple invocations in the future.

======================================================================
WHY NO "sync_complete" STEP (unlike Okta)
======================================================================
Okta's start_okta_ingestion() calls merge_module_sync_metadata() itself as
its last action, which okta-lambda-loader's handler.py had to replicate as
an explicit "sync_complete" step since cartography's own stage runner
normally does this (see that handler's docstring). Confirmed by reading
cartography/intel/vercel/__init__.py directly: start_vercel_ingestion() does
NOT call merge_module_sync_metadata() anywhere — Vercel does not write a
ModuleSyncMetadata "receipt" node today. This is a pre-existing gap in cartography's own Vercel module
(same for several other TOP_LEVEL_MODULES entries that don't call it either
— not something introduced by this handler, and not something this handler
attempts to silently paper over by inventing a receipt cartography itself
doesn't write). Not filed upstream yet.

======================================================================
INVOCATION CONTRACT
======================================================================
{
  "steps": ["teams", "users", "projects", ...],   # optional, see DEFAULT_STEPS
  "update_tag": 1792017089                          # optional, see below
}

Returns:
{
  "status": "SUCCEEDED" | "FAILED",
  "completed": [...],
  "remaining": [...],     # see Okta handler's identical contract for the
                            # exact FAILED-vs-SUCCEEDED semantics of this field
  "updateTag": 1792017089,
  "durationSeconds": ...,
  "error"?: "..."
}

`update_tag` is optional — omit it to start a brand-new run (a fresh tag is
generated and reported back); pass a prior run's `updateTag` to continue
writing under that same tag (relevant only if this is ever split across
multiple invocations in the future — a single, unsplit invocation of
DEFAULT_STEPS has no cross-invocation tag-sharing concern today, since
every step in one run already shares one Python-process-local `update_tag`
by construction).

Unlike Okta's legacy OktaGroup cleanup gotcha (a real, cross-invocation
data-loss risk that update_tag sharing exists specifically to avoid), no
Vercel submodule's cleanup() call has an equivalent legacy "delete anything
not stamped with the CURRENT call's tag" hazard when run as a single,
unsplit invocation — every step's cleanup already only ever sees the one
tag this invocation generated. The update_tag threading plumbing is kept
here anyway, purely so a FUTURE split (same motivation as "every submodule
is its own named step" above) does not require retrofitting it in under
time pressure.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import boto3
import neo4j
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Quiet the very chatty neo4j driver notification logger; cartography's own
# sync logging already covers what we need at INFO.
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

_TIMEOUT = (60, 60)

# The fixed step order this handler supports, matching
# cartography.intel.vercel.__init__.start_vercel_ingestion()'s own phase
# order exactly (see that function's own "Phase N" comments). A normal,
# unsplit run requests every step in DEFAULT_STEPS, in this order.
#
#   Phase 1: team                       -> "teams"
#   Phase 2: users                      -> "users"
#   Phase 3: projects                   -> "projects"
#   Phase 4: team-level resources       -> "domains", "edge_configs",
#                                           "shared_env_vars", "integrations",
#                                           "access_groups", "webhooks",
#                                           "log_drains",
#                                           "secure_compute_networks",
#                                           "aliases"
#   Phase 5: per-project sub-resources  -> "project_resources" (one step,
#                                           loops every project in-process —
#                                           see module docstring for why this
#                                           is not split further yet)
#   Phase 6: per-domain sub-resources   -> "domain_resources"
#   Phase 7: per-edge-config resources  -> "edge_config_resources"
#   Phase 8: label migration            -> "label_migration"
DEFAULT_STEPS: list[str] = [
    "teams",
    "users",
    "projects",
    "domains",
    "edge_configs",
    "shared_env_vars",
    "integrations",
    "access_groups",
    "webhooks",
    "log_drains",
    "secure_compute_networks",
    "aliases",
    "project_resources",
    "domain_resources",
    "edge_config_resources",
    "label_migration",
]

KNOWN_STEPS = frozenset(DEFAULT_STEPS)


def _get_vercel_api_token() -> str:
    """Fetches the Vercel API token from SSM at invoke time (with
    decryption) rather than from a Lambda environment variable. Environment
    variables are visible in plaintext to anyone with
    lambda:GetFunctionConfiguration, which would defeat the point of
    storing the token as a SecureString in the first place. Mirrors
    okta-lambda-loader/src/handler.py's _get_okta_api_key().
    VERCEL_TOKEN_SSM_PARAM is just the parameter NAME, set at deploy time;
    it is not sensitive."""
    ssm = boto3.client("ssm")
    param_name = os.environ.get(
        "VERCEL_TOKEN_SSM_PARAM", "/cartography/vercel/token"
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


def _build_api_session(vercel_token: str) -> requests.Session:
    """Builds the requests.Session used for every step in this invocation —
    byte-for-byte the same retry policy start_vercel_ingestion() itself
    configures (cartography/intel/vercel/__init__.py), since this handler
    calls the same per-submodule sync() functions that session is normally
    built for, just one phase at a time instead of all in one call."""
    api_session = requests.session()
    retry_policy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    api_session.mount("https://", HTTPAdapter(max_retries=retry_policy))
    api_session.headers.update({"Authorization": f"Bearer {vercel_token}"})
    return api_session


class _StepContext:
    """Carries the small amount of state later steps need from earlier ones
    (the `projects`/`domains`/`edge_configs` lists phases 3/4 return),
    exactly mirroring how okta-lambda-loader/src/handler.py's
    `user_ids_cache` is threaded between `users` and `factors` so steps can
    run out of their "natural" single-call order if ever split across
    invocations later. Re-fetches on demand (a cheap, idempotent GET) if a
    later step needs a list that wasn't populated by an earlier step in
    THIS invocation — e.g. "project_resources" requested alone, without a
    prior "projects" step in the same invocation.
    """

    def __init__(self) -> None:
        self.projects: list[dict[str, Any]] | None = None
        self.domains: list[dict[str, Any]] | None = None
        self.edge_configs: list[dict[str, Any]] | None = None

    def get_projects(
        self,
        api_session: requests.Session,
        common_job_parameters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if self.projects is None:
            logger.info(
                "'%s' requested without a prior 'projects' step in this "
                "invocation; re-fetching the project list.",
                "project_resources/secure_compute_networks",
            )
            import cartography.intel.vercel.projects as projects_mod

            projects = projects_mod.get(
                api_session,
                common_job_parameters["BASE_URL"],
                common_job_parameters["TEAM_ID"],
            )
            projects_mod.transform(projects)
            self.projects = projects
        return self.projects

    def get_domains(
        self,
        api_session: requests.Session,
        common_job_parameters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if self.domains is None:
            logger.info(
                "'domain_resources' requested without a prior 'domains' "
                "step in this invocation; re-fetching the domain list."
            )
            import cartography.intel.vercel.domains as domains_mod

            self.domains = domains_mod.get(
                api_session,
                common_job_parameters["BASE_URL"],
                common_job_parameters["TEAM_ID"],
            )
        return self.domains

    def get_edge_configs(
        self,
        api_session: requests.Session,
        common_job_parameters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if self.edge_configs is None:
            logger.info(
                "'edge_config_resources' requested without a prior "
                "'edge_configs' step in this invocation; re-fetching the "
                "edge config list."
            )
            import cartography.intel.vercel.edgeconfigs as edgeconfigs_mod

            self.edge_configs = edgeconfigs_mod.get(
                api_session,
                common_job_parameters["BASE_URL"],
                common_job_parameters["TEAM_ID"],
            )
        return self.edge_configs


def _run_step(
    step: str,
    api_session: requests.Session,
    neo4j_session: neo4j.Session,
    common_job_parameters: dict[str, Any],
    ctx: _StepContext,
) -> None:
    """Runs one named step by calling cartography's real, public, UNMODIFIED
    cartography.intel.vercel.<module>.sync() function directly — no
    patching, no reimplementation. See module docstring for the full phase
    mapping and why phases 5/6/7 are each a single step rather than one
    step per project/domain/edge-config.
    """

    if step == "teams":
        import cartography.intel.vercel.teams as teams_mod

        teams_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "users":
        import cartography.intel.vercel.users as users_mod

        users_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "projects":
        import cartography.intel.vercel.projects as projects_mod

        ctx.projects = projects_mod.sync(
            neo4j_session, api_session, common_job_parameters
        )
        return

    if step == "domains":
        import cartography.intel.vercel.domains as domains_mod

        ctx.domains = domains_mod.sync(
            neo4j_session, api_session, common_job_parameters
        )
        return

    if step == "edge_configs":
        import cartography.intel.vercel.edgeconfigs as edgeconfigs_mod

        ctx.edge_configs = edgeconfigs_mod.sync(
            neo4j_session, api_session, common_job_parameters
        )
        return

    if step == "shared_env_vars":
        import cartography.intel.vercel.sharedenvironmentvariables as sev_mod

        sev_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "integrations":
        import cartography.intel.vercel.integrations as integrations_mod

        integrations_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "access_groups":
        import cartography.intel.vercel.accessgroups as accessgroups_mod

        accessgroups_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "webhooks":
        import cartography.intel.vercel.webhooks as webhooks_mod

        webhooks_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "log_drains":
        import cartography.intel.vercel.logdrains as logdrains_mod

        logdrains_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "secure_compute_networks":
        import cartography.intel.vercel.securecomputenetworks as scn_mod

        projects = ctx.get_projects(api_session, common_job_parameters)
        scn_mod.sync(neo4j_session, api_session, common_job_parameters, projects)
        return

    if step == "aliases":
        import cartography.intel.vercel.aliases as aliases_mod

        aliases_mod.sync(neo4j_session, api_session, common_job_parameters)
        return

    if step == "project_resources":
        # Phase 5 of start_vercel_ingestion(): one step covering every
        # per-project sub-resource sync, looped in-process across all
        # projects — see module docstring for why this is a single step
        # rather than one step per project.
        import cartography.intel.vercel.deployments as deployments_mod
        import cartography.intel.vercel.environmentvariables as env_vars_mod
        import cartography.intel.vercel.firewallbypassrules as fw_bypass_mod
        import cartography.intel.vercel.firewallconfigs as fw_config_mod
        import cartography.intel.vercel.projectdomains as project_domains_mod

        projects = ctx.get_projects(api_session, common_job_parameters)
        for project in projects:
            project_id = project["id"]
            project_job_parameters = {
                **common_job_parameters,
                "project_id": project_id,
            }
            deployments_mod.sync(
                neo4j_session,
                api_session,
                project_job_parameters,
                project_id=project_id,
                # Deployment protection is configured on the project but
                # decides whether each deployment URL is reachable, so it
                # is handed down rather than re-read — mirrors
                # start_vercel_ingestion()'s own Phase 5 call exactly.
                protection={
                    key: project.get(key)
                    for key in (
                        "sso_protection_deployment_type",
                        "password_protection_deployment_type",
                    )
                },
            )
            env_vars_mod.sync(
                neo4j_session,
                api_session,
                project_job_parameters,
                project_id=project_id,
            )
            project_domains_mod.sync(
                neo4j_session,
                api_session,
                project_job_parameters,
                project_id=project_id,
            )
            fw_config_mod.sync(
                neo4j_session,
                api_session,
                project_job_parameters,
                project_id=project_id,
            )
            fw_bypass_mod.sync(
                neo4j_session,
                api_session,
                project_job_parameters,
                project_id=project_id,
            )
        return

    if step == "domain_resources":
        # Phase 6: one step covering every per-domain sub-resource sync
        # (today: just dnsrecords), looped in-process across all domains.
        import cartography.intel.vercel.dnsrecords as dnsrecords_mod

        domains = ctx.get_domains(api_session, common_job_parameters)
        for domain in domains:
            domain_name = domain["name"]
            domain_job_parameters = {
                **common_job_parameters,
                "domain_name": domain_name,
            }
            dnsrecords_mod.sync(
                neo4j_session,
                api_session,
                domain_job_parameters,
                domain_name=domain_name,
            )
        return

    if step == "edge_config_resources":
        # Phase 7: one step covering every per-edge-config sub-resource
        # sync (today: just edgeconfigtokens), looped in-process across all
        # edge configs.
        import cartography.intel.vercel.edgeconfigtokens as edgeconfigtokens_mod

        edge_configs = ctx.get_edge_configs(api_session, common_job_parameters)
        for ec in edge_configs:
            ec_id = ec["id"]
            ec_job_parameters = {
                **common_job_parameters,
                "edge_config_id": ec_id,
            }
            edgeconfigtokens_mod.sync(
                neo4j_session,
                api_session,
                ec_job_parameters,
                edge_config_id=ec_id,
            )
        return

    if step == "label_migration":
        # Phase 8 of start_vercel_ingestion(): DEPRECATED compatibility
        # migration for the vercel-local :Group label on VercelAccessGroup.
        # Calls cartography's own run_analysis_job() unmodified, same JSON
        # file start_vercel_ingestion() itself reads via importlib.resources
        # — not vendored/copied, read straight from the vendored
        # cartography/ package tree (see Makefile).
        from cartography.util import run_analysis_job

        run_analysis_job(
            "vercel_label_migration.json",
            neo4j_session,
            common_job_parameters,
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

    # One update_tag for the whole invocation. See module docstring "why
    # the update_tag threading plumbing is kept here anyway" for why this
    # is future-proofing rather than a correctness requirement today (a
    # single, unsplit invocation has no cross-invocation tag-sharing
    # concern to protect against, unlike Okta's legacy OktaGroup cleanup).
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
        vercel_token = _get_vercel_api_token()
        api_session = _build_api_session(vercel_token)
        driver = _get_neo4j_driver()

        team_id = os.environ["VERCEL_TEAM_ID"]
        base_url = os.environ.get("VERCEL_BASE_URL", "https://api.vercel.com")
        common_job_parameters = {
            "UPDATE_TAG": update_tag,
            "BASE_URL": base_url,
            "TEAM_ID": team_id,
        }

        ctx = _StepContext()

        with driver.session(
            database=os.environ.get("NEO4J_DATABASE") or None
        ) as neo4j_session:
            for step in steps:
                logger.info("Running step '%s' (update_tag=%s)", step, update_tag)
                _run_step(
                    step, api_session, neo4j_session, common_job_parameters, ctx
                )
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
