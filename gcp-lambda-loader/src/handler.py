"""
Cartography GCP sync - Lambda handler. One invocation = one GCP organization.

AUTH (no keys)
--------------
Workload Identity Federation, set up by ../gcp/ (Terraform). google-auth signs
an AWS STS GetCallerIdentity request with this Lambda's role credentials
(AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN env vars, which Lambda provides),
exchanges it at sts.googleapis.com for a federated token, then impersonates
GCP_SERVICE_ACCOUNT. The pool provider only accepts the
cartography-gcp-lambda-loader-role assumed-role ARN. Nothing secret is stored
anywhere; GCP_WIF_AUDIENCE and GCP_SERVICE_ACCOUNT are identifiers.

If GCP_WIF_AUDIENCE is unset (local runs), Application Default Credentials are
used instead, optionally impersonating GCP_SERVICE_ACCOUNT.

WHY ONE ORG PER INVOCATION
--------------------------
start_gcp_ingestion() walks every org the identity can see. We scope it to one
org by passing every OTHER visible org as gcp_excluded_org_ids. This is safe:
- folder/project cleanup is scoped to ORG_RESOURCE_NAME,
- per-project resource cleanup is scoped to PROJECT_ID,
- GCPOrganization has no sub-resource, so org cleanup builds no queries, and
  excluded orgs are explicitly never pruned (see intel/gcp/__init__.py).
So one org's run cannot delete another org's data, and a failed org leaves its
existing data untouched.

PREFLIGHT GUARD
---------------
The target org must be visible to the identity. If an org-level grant is
removed, search_organizations() silently stops returning it, and the sync
would quietly do nothing. We fail loudly instead.

PROJECT BATCHES AND FINALIZE
---------------------------
An org too big for one 900s invocation is split into project batches
(project_batch_index / project_batch_count, all with one update_tag). A batch
syncs only its projects' resources and defers the org-wide steps; it reports
policyBindingsComplete. Once every batch of the run has succeeded, the
orchestrator invokes {"finalize": true, ...} once, which runs the deferred
inherited org/folder policy-binding cleanup for each batched org (only if every
batch reported policyBindingsComplete) and the global analysis jobs.

A slow project can be isolated: one split with "include_projects": [id] syncs
only that project, and every batch of the org lists it in "exclude_projects"
(dropped before batching). Both kinds are partial runs, so they also defer to
Finalize and require an explicit update_tag.

INVOCATION CONTRACT
  sync:     {"org_id": "<numeric>", "requested_syncs"?: "iam,compute,...",
             "project_batch_index"?: <int>, "project_batch_count"?: <int>,
             "include_projects"?: [<project id>, ...],
             "exclude_projects"?: [<project id>, ...],
             "update_tag"?: <int>}
  finalize: {"finalize": true, "update_tag": <int>,
             "cleanup_org_ids": ["<numeric>", ...],
             "policy_bindings_complete": <bool>}
Returns:
  {"status": "SUCCEEDED"|"FAILED", "orgId", "orgName", "updateTag",
   "requestedSyncs", "projectBatch"?, "policyBindingsComplete"?,
   "includeProjects"?, "excludeProjects"?, "durationSeconds", "error"?}
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
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)
# googleapiclient logs every discovery-doc load at INFO.
logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)

CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform"


def _get_gcp_credentials():
    import google.auth
    from google.auth import aws
    from google.auth import impersonated_credentials
    from google.auth.transport.requests import Request

    sa = os.environ.get("GCP_SERVICE_ACCOUNT", "")
    audience = os.environ.get("GCP_WIF_AUDIENCE", "")

    if audience:
        creds = aws.Credentials.from_info(
            {
                "type": "external_account",
                "audience": audience,
                "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
                "token_url": "https://sts.googleapis.com/v1/token",
                "service_account_impersonation_url": (
                    "https://iamcredentials.googleapis.com/v1/projects/-/"
                    f"serviceAccounts/{sa}:generateAccessToken"
                ),
                "credential_source": {
                    # AWS creds and region come from the Lambda env vars;
                    # these URLs are only a fallback and are never hit here.
                    "environment_id": "aws1",
                    "region_url": "http://169.254.169.254/latest/meta-data/placement/availability-zone",
                    "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials",
                    "regional_cred_verification_url": (
                        "https://sts.{region}.amazonaws.com"
                        "?Action=GetCallerIdentity&Version=2011-06-15"
                    ),
                },
            },
            scopes=[CLOUD_PLATFORM],
        )
    else:
        creds, _ = google.auth.default(scopes=[CLOUD_PLATFORM])
        if sa:
            creds = impersonated_credentials.Credentials(
                source_credentials=creds,
                target_principal=sa,
                target_scopes=[CLOUD_PLATFORM],
            )

    # Fail fast with a clear error if the federation/impersonation is broken,
    # rather than on the first API call deep inside cartography.
    try:
        creds.refresh(Request())
    except Exception as exc:
        raise RuntimeError(
            f"GCP credential exchange failed (WIF audience={audience or 'ADC'}, "
            f"service account={sa or 'none'}): {exc}"
        ) from exc
    return creds


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
    return neo4j.GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=("neo4j", _get_neo4j_password()),
        max_connection_lifetime=int(os.environ.get("NEO4J_MAX_CONNECTION_LIFETIME", "200")),
    )


def _permission_relationships_file() -> str:
    import cartography

    return os.path.join(
        os.path.dirname(cartography.__file__), "data", "gcp_permission_relationships.yaml"
    )


def _optional_int(event: dict[str, Any], key: str) -> int | None:
    value = event.get(key)
    return None if value is None else int(value)


def _optional_ids(event: dict[str, Any], key: str) -> list[str] | None:
    value = event.get(key)
    if not value:
        return None
    if not isinstance(value, list):
        raise ValueError(f'"{key}" must be a list of project IDs')
    return [str(v).strip() for v in value if str(v).strip()] or None


def _finalize(event: dict[str, Any]) -> dict[str, Any]:
    from cartography.config import Config
    from cartography.intel.gcp import finalize_gcp_ingestion

    start = time.time()
    update_tag = event.get("update_tag")
    cleanup_org_ids = [str(o).strip() for o in event.get("cleanup_org_ids") or []]
    complete = bool(event.get("policy_bindings_complete"))
    result: dict[str, Any] = {
        "status": "FAILED",
        "mode": "finalize",
        "updateTag": update_tag,
        "cleanupOrgIds": cleanup_org_ids,
        "policyBindingsComplete": complete,
    }
    driver = None
    try:
        # Finalize deletes what this run's tag did not refresh, so it must use
        # the run's tag, never a fresh one.
        if not update_tag:
            raise ValueError('finalize requires the run\'s "update_tag"')
        config = Config(
            neo4j_uri=os.environ["NEO4J_URI"],
            update_tag=int(update_tag),
            gcp_permission_relationships_file=_permission_relationships_file(),
        )
        logger.info(
            "Finalizing GCP run %s: inherited binding cleanup for %s (complete=%s), "
            "then analysis jobs.",
            update_tag, cleanup_org_ids, complete,
        )
        credentials = _get_gcp_credentials() if cleanup_org_ids and complete else None
        driver = _get_neo4j_driver()
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            finalize_gcp_ingestion(
                session,
                config,
                [f"organizations/{o}" for o in cleanup_org_ids],
                complete,
                credentials=credentials,
            )
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 - surface to the state machine
        logger.exception("GCP finalize failed")
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)
        logger.info("Result: %s", result)
    return result


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    event = event or {}
    if event.get("finalize"):
        return _finalize(event)

    from cartography.config import Config
    from cartography.intel.gcp import start_gcp_ingestion
    from cartography.intel.gcp.crm.orgs import get_gcp_organizations

    start = time.time()
    org_id = str(event.get("org_id") or "").strip()
    requested_syncs = event.get("requested_syncs") or None
    update_tag = int(event.get("update_tag") or time.time())
    result: dict[str, Any] = {
        "status": "FAILED",
        "orgId": org_id,
        "updateTag": update_tag,
        "requestedSyncs": requested_syncs,
    }

    driver = None
    try:
        if not org_id:
            raise ValueError('event must include "org_id"')
        batch_index = _optional_int(event, "project_batch_index")
        batch_count = _optional_int(event, "project_batch_count")
        include_projects = _optional_ids(event, "include_projects")
        exclude_projects = _optional_ids(event, "exclude_projects")
        if include_projects and exclude_projects:
            raise ValueError('"include_projects" and "exclude_projects" are mutually exclusive')
        if batch_count is not None:
            result["projectBatch"] = f"{batch_index}/{batch_count}"
        if include_projects:
            result["includeProjects"] = include_projects
        if exclude_projects:
            result["excludeProjects"] = exclude_projects
        if batch_count is not None or include_projects or exclude_projects:
            # A partial run's shared cleanup is deferred to a finalize call that
            # must reuse this tag, so the caller has to supply it.
            if not event.get("update_tag"):
                raise ValueError("partial (batch/include/exclude) runs require an explicit update_tag")

        credentials = _get_gcp_credentials()

        visible = {o["name"].split("/")[-1]: o for o in get_gcp_organizations(credentials)}
        if org_id not in visible:
            raise RuntimeError(
                f"Org {org_id} is not visible to the GCP identity (visible: "
                f"{sorted(visible)}). Check the org-level grants in ../gcp/. "
                "Refusing to run so existing data is preserved."
            )
        result["orgName"] = visible[org_id].get("displayName")
        excluded = sorted(set(visible) - {org_id})
        logger.info(
            "Syncing GCP org %s (%s); excluding %s; requested_syncs=%s; batch=%s; "
            "include_projects=%s; exclude_projects=%s; update_tag=%d",
            org_id, result["orgName"], excluded, requested_syncs or "ALL",
            result.get("projectBatch", "none"), include_projects, exclude_projects, update_tag,
        )

        config = Config(
            neo4j_uri=os.environ["NEO4J_URI"],
            update_tag=update_tag,
            gcp_requested_syncs=requested_syncs,
            gcp_permission_relationships_file=_permission_relationships_file(),
            gcp_excluded_org_ids=excluded,
            gcp_project_batch_index=batch_index,
            gcp_project_batch_count=batch_count,
            gcp_include_project_ids=include_projects,
            gcp_exclude_project_ids=exclude_projects,
        )

        driver = _get_neo4j_driver()
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            batch_result = start_gcp_ingestion(session, config, credentials=credentials)
        if batch_result is not None:
            result["policyBindingsComplete"] = batch_result.policy_bindings_complete
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 - surface to the state machine
        logger.exception("GCP sync failed for org %s", org_id)
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)
        logger.info("Result: %s", result)
    return result
