"""
Cartography CrowdStrike Falcon sync - Lambda handler.

Calls cartography's Python API directly (not the CLI), same approach as the
other *-lambda-loader stacks. One Falcon API client, one CID, no fan-out.

What cartography.intel.crowdstrike ingests:
  hosts            QueryDevicesByFilterScroll + GetDeviceDetails
                   -> (:CrowdstrikeTenant)<-[:RESOURCE]-(:CrowdstrikeHost)
  vulnerabilities  combinedQueryVulnerabilities (open/reopen + extra filter)
                   -> (:CrowdstrikeSpotlightVulnerability)-[:HAS_VULNERABILITY]->(:CrowdstrikeHost)
                      (:CrowdstrikeFinding:CVE)-[:HAS_CVE]->(:CrowdstrikeSpotlightVulnerability)
CrowdstrikeHost maps into the ontology Device layer by serial_number.

WHY A SPOTLIGHT FILTER
----------------------
Spotlight can report millions of open vulnerabilities on a fleet of under a
thousand hosts, mostly from one stale browser build. Unfiltered, one such
fleet took ~490 API pages (~28 min) before any Neo4j write, over Lambda's
900s cap. CROWDSTRIKE_SPOTLIGHT_FILTER (default CRITICAL/HIGH severity AND
ExPRT HIGH/CRITICAL) brought it to ~66k records, ~14 pages. The fork ANDs it onto the open/reopen status filter.

WHY THIS DOES NOT CALL start_crowdstrike_ingestion()
-----------------------------------------------------
That function always runs cleanup, and cleanup deletes every host and
vulnerability not stamped with this run's update_tag. An empty-but-200
response (wrong API region, a client with no hosts in scope) would wipe the
tenant. So this handler calls the same public functions in the same order
but gates cleanup on minimum counts. Loads always happen.

The Spotlight sync is one paged loop with no resume point, so hosts and vulns
run in the same invocation; at the filtered volume that fits comfortably.

INVOCATION CONTRACT
-------------------
  {"update_tag"?: <int>, "skip_vulnerabilities"?: <bool>, "skip_cleanup"?: <bool>}

Returns:
  {"status": "SUCCEEDED"|"FAILED", "updateTag": ..., "hostCount": ...,
   "vulnerabilityCount": ..., "cveCount": ..., "spotlightFilter": ...,
   "cleanup": "ran"|"skipped", "durationSeconds": ..., "error"?: "..."}
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

_CONFIG_KEYS = ("client-id", "client-secret", "api-url")


def _get_crowdstrike_config() -> dict[str, str]:
    """client-id, client-secret, api-url from SSM at invoke time, in one call.

    Read at run time, not resolved into env vars at deploy time: the stack
    deploys before they exist, a rotation takes effect on the next run, and the
    secret is never visible via lambda:GetFunctionConfiguration.
    Local testing can bypass SSM by setting CROWDSTRIKE_CLIENT_ID,
    CROWDSTRIKE_CLIENT_SECRET and CROWDSTRIKE_API_URL.
    """
    local = {
        "client-id": os.environ.get("CROWDSTRIKE_CLIENT_ID"),
        "client-secret": os.environ.get("CROWDSTRIKE_CLIENT_SECRET"),
        "api-url": os.environ.get("CROWDSTRIKE_API_URL"),
    }
    if all(local.values()):
        return {k: v.strip() for k, v in local.items() if v}

    prefix = os.environ.get("CROWDSTRIKE_SSM_PREFIX", "/cartography/crowdstrike").rstrip("/")
    names = {k: f"{prefix}/{k}" for k in _CONFIG_KEYS}
    response = boto3.client("ssm").get_parameters(
        Names=list(names.values()), WithDecryption=True
    )
    if response["InvalidParameters"]:
        raise RuntimeError(
            f"Missing SSM parameter(s) {sorted(response['InvalidParameters'])}; "
            "see README.md 'Setup'."
        )
    values = {p["Name"]: p["Value"].strip() for p in response["Parameters"]}
    return {k: values[name] for k, name in names.items()}


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


def _count(session: neo4j.Session, label: str, update_tag: int) -> int:
    query = f"MATCH (n:{label}) WHERE n.lastupdated = $tag RETURN count(n) AS n"
    return session.run(query, tag=update_tag).single()["n"]


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    from cartography.intel import crowdstrike
    from cartography.intel.crowdstrike.endpoints import sync_hosts
    from cartography.intel.crowdstrike.label_migrations import (
        migrate_spotlight_vulnerability_label,
    )
    from cartography.intel.crowdstrike.spotlight import build_spotlight_filter
    from cartography.intel.crowdstrike.spotlight import sync_vulnerabilities
    from cartography.intel.crowdstrike.util import get_authorization
    from cartography.util import merge_module_sync_metadata

    start = time.time()
    update_tag = int(event.get("update_tag") or time.time())
    skip_vulns = bool(event.get("skip_vulnerabilities"))
    skip_cleanup = bool(event.get("skip_cleanup"))
    extra_filter = os.environ.get("CROWDSTRIKE_SPOTLIGHT_FILTER") or None
    min_hosts = int(os.environ.get("CROWDSTRIKE_MIN_HOSTS_FOR_CLEANUP", "1"))
    min_vulns = int(os.environ.get("CROWDSTRIKE_MIN_VULNS_FOR_CLEANUP", "1"))

    result: dict[str, Any] = {
        "status": "FAILED",
        "updateTag": update_tag,
        "spotlightFilter": build_spotlight_filter(extra_filter),
    }

    driver = None
    try:
        cfg = _get_crowdstrike_config()
        auth = get_authorization(cfg["client-id"], cfg["client-secret"], cfg["api-url"])
        common_job_parameters = {"UPDATE_TAG": update_tag}

        driver = _get_neo4j_driver()
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            migrate_spotlight_vulnerability_label(session)

            t = time.time()
            sync_hosts(session, update_tag, auth)
            result["hostCount"] = _count(session, "CrowdstrikeHost", update_tag)
            result["hostsSeconds"] = round(time.time() - t, 1)
            logger.info("Loaded %d CrowdStrike hosts", result["hostCount"])

            if skip_vulns:
                result["vulnerabilityCount"] = None
            else:
                t = time.time()
                sync_vulnerabilities(session, update_tag, auth, extra_filter=extra_filter)
                result["vulnerabilityCount"] = _count(
                    session, "CrowdstrikeSpotlightVulnerability", update_tag
                )
                result["cveCount"] = _count(session, "CrowdstrikeFinding", update_tag)
                result["vulnerabilitiesSeconds"] = round(time.time() - t, 1)
                logger.info(
                    "Loaded %d Spotlight vulnerabilities, %d CVEs",
                    result["vulnerabilityCount"], result["cveCount"],
                )

            reasons = []
            if skip_cleanup:
                reasons.append("skip_cleanup requested")
            if skip_vulns:
                reasons.append("vulnerabilities skipped this run")
            if result["hostCount"] < min_hosts:
                reasons.append(f"hostCount {result['hostCount']} < {min_hosts}")
            if not skip_vulns and result["vulnerabilityCount"] < min_vulns:
                reasons.append(f"vulnerabilityCount {result['vulnerabilityCount']} < {min_vulns}")

            if reasons:
                result["cleanup"] = "skipped"
                result["cleanupSkippedBecause"] = reasons
                logger.warning("Skipping CrowdStrike cleanup: %s", "; ".join(reasons))
            else:
                crowdstrike.cleanup(session, common_job_parameters)
                result["cleanup"] = "ran"

            merge_module_sync_metadata(
                session,
                group_type="crowdstrike",
                group_id=cfg["api-url"],
                synced_type="crowdstrike",
                update_tag=update_tag,
                stat_handler=crowdstrike.stat_handler,
            )
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 - surface to the state machine
        logger.exception("CrowdStrike sync failed")
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)
    return result
