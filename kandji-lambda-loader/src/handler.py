"""
Cartography Kandji (Iru) sync - Lambda handler.

Calls cartography's Python API directly (not the CLI), same approach as the
other *-lambda-loader stacks. Kandji is single-tenant: one API token, one
tenant, no fan-out.

What cartography.intel.kandji ingests (cartography fork): users, devices, then apps.
  - Paged GET {base_uri}/api/v1/users (cursor `next`) ->
    (:KandjiTenant)-[:RESOURCE]->(:KandjiUser:UserAccount)
  - Paged GET {base_uri}/api/v1/devices (300 per page) ->
    (:KandjiTenant)-[:RESOURCE]->(:KandjiDevice), plus
    (:KandjiUser)-[:OWNS]->(:KandjiDevice) from each device's assigned user.
  - GET {base_uri}/api/v1/devices/{id}/apps for every device, in
    parallel (8 workers) ->
    (:KandjiApp {id: bundle_id}), (:KandjiAppVersion {id: bundle_id:version}),
    (:KandjiAppVersion)-[:VERSION]->(:KandjiApp),
    (:KandjiDevice)-[:HAS_INSTALLED {path, source, signature, ...}]->(:KandjiAppVersion)
Users load first so the device OWNS edges have users to match; apps load last
so HAS_INSTALLED has devices to match. KandjiDevice is mapped into the
ontology by serial_number; KandjiUser by email, which lets the ontology job
derive (:User)-[:OWNS]->(:Device) from Kandji assignments. Apps are not mapped
into the ontology.

WHY ONE INVOCATION, NO SPLITS
------------------------------
Users and devices are two paged calls. Apps are one call per device, at
roughly 24 req/s with 8 workers (Kandji's limits are 50 req/s and 10,000
req/hour per token), so about 40s per 1,000 devices. Well inside Lambda's
900s cap for a typical fleet; the hourly limit caps a run at 10,000 devices.

WHY THIS DOES NOT CALL users.sync() / devices.sync() / apps.sync()
-------------------------------------------------------------------
Each sync() is get -> transform -> load -> cleanup in one call, and cleanup
deletes every node under the tenant not stamped with this run's update_tag.
If the API returns an empty list while not raising (wrong base URI pointed at
something that answers 200, a token scoped to nothing, a Kandji-side outage
page), sync() would delete every user/device/app in the graph. So this
handler calls the same public get/transform/load/cleanup functions, in the
same order, but refuses each cleanup when that fetch returned fewer items
than KANDJI_MIN_USERS_FOR_CLEANUP / KANDJI_MIN_DEVICES_FOR_CLEANUP /
KANDJI_MIN_APP_INSTALLS_FOR_CLEANUP. Loads still happen; only the deletes are
gated, independently.

All fetches happen before any write. If any raises (apps retries 429/5xx
before giving up), the run fails with the graph untouched and the previous
run's data stays in place.

INVOCATION CONTRACT
-------------------
  {"update_tag"?: <int>}   omit to start a new run with a fresh tag

Returns:
  {"status": "SUCCEEDED"|"FAILED", "updateTag": ...,
   "userCount": ..., "userCleanup": "ran"|"skipped",
   "deviceCount": ..., "cleanup": "ran"|"skipped",
   "appCount": ..., "appInstallCount": ..., "appCleanup": "ran"|"skipped",
   "durationSeconds": ..., "error"?: "..."}
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


def _get_kandji_config() -> tuple[str, str, str]:
    """(base_uri, tenant_id, token) from SSM at invoke time, in one call.

    All three are read at run time, not resolved into env vars at deploy
    time: the stack deploys before they exist, and a change takes effect on
    the next run with no redeploy. The token must never be an env var anyway
    (plaintext to anyone with lambda:GetFunctionConfiguration).
    """
    prefix = os.environ.get("KANDJI_SSM_PREFIX", "/cartography/kandji").rstrip("/")
    names = {k: f"{prefix}/{k}" for k in ("base-uri", "tenant-id", "token")}
    response = boto3.client("ssm").get_parameters(
        Names=list(names.values()), WithDecryption=True
    )
    if response["InvalidParameters"]:
        raise RuntimeError(
            f"Missing SSM parameter(s) {sorted(response['InvalidParameters'])}; "
            "see README.md 'Setup'."
        )
    values = {p["Name"]: p["Value"] for p in response["Parameters"]}
    return (
        values[names["base-uri"]].strip().rstrip("/"),
        values[names["tenant-id"]].strip(),
        values[names["token"]],
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
    return neo4j.GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=("neo4j", _get_neo4j_password()),
        max_connection_lifetime=int(os.environ.get("NEO4J_MAX_CONNECTION_LIFETIME", "200")),
    )


def _gated_cleanup(
    cleanup_fn: Any,
    session: neo4j.Session,
    common_job_parameters: dict[str, Any],
    count: int,
    min_for_cleanup: int,
    kind: str,
    env_var: str,
) -> str:
    if count >= min_for_cleanup:
        cleanup_fn(session, common_job_parameters)
        return "ran"
    logger.warning(
        "Skipping %s cleanup: fetched %d, below %s=%d. Existing nodes are preserved.",
        kind, count, env_var, min_for_cleanup,
    )
    return "skipped"


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    from cartography.intel.kandji import apps
    from cartography.intel.kandji import devices
    from cartography.intel.kandji import users

    start = time.time()
    update_tag = int(event.get("update_tag") or time.time())
    result: dict[str, Any] = {"status": "FAILED", "updateTag": update_tag}

    driver = None
    try:
        base_uri, tenant_id, token = _get_kandji_config()
        min_users = int(os.environ.get("KANDJI_MIN_USERS_FOR_CLEANUP", "1"))
        min_devices = int(os.environ.get("KANDJI_MIN_DEVICES_FOR_CLEANUP", "1"))
        min_installs = int(os.environ.get("KANDJI_MIN_APP_INSTALLS_FOR_CLEANUP", "1"))
        common_job_parameters = {"UPDATE_TAG": update_tag, "TENANT_ID": tenant_id}

        # Fetch everything before writing anything, so an API failure on any
        # step leaves the graph untouched.
        formatted_users = users.transform(
            users.get(kandji_base_uri=base_uri, kandji_token=token)
        )
        result["userCount"] = len(formatted_users)
        logger.info("Fetched %d Kandji users from %s", len(formatted_users), base_uri)

        formatted_devices = devices.transform(
            devices.get(kandji_base_uri=base_uri, kandji_token=token)
        )
        result["deviceCount"] = len(formatted_devices)
        logger.info("Fetched %d Kandji devices from %s", len(formatted_devices), base_uri)

        app_rows, install_rows = apps.transform(
            apps.get(
                kandji_base_uri=base_uri,
                kandji_token=token,
                device_ids=[d["id"] for d in formatted_devices],
            )
        )
        result["appCount"] = len(app_rows)
        result["appInstallCount"] = len(install_rows)
        logger.info(
            "Fetched %d Kandji app installs (%d distinct apps)",
            len(install_rows), len(app_rows),
        )

        driver = _get_neo4j_driver()
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            # Users first: devices link to KandjiUser via OWNS.
            users.load_users(session, common_job_parameters, formatted_users)
            result["userCleanup"] = _gated_cleanup(
                users.cleanup, session, common_job_parameters,
                len(formatted_users), min_users,
                "KandjiUser", "KANDJI_MIN_USERS_FOR_CLEANUP",
            )

            devices.load_devices(session, common_job_parameters, formatted_devices)
            result["cleanup"] = _gated_cleanup(
                devices.cleanup, session, common_job_parameters,
                len(formatted_devices), min_devices,
                "KandjiDevice", "KANDJI_MIN_DEVICES_FOR_CLEANUP",
            )

            # Apps last: HAS_INSTALLED attaches to the devices loaded above.
            apps.load_apps(session, common_job_parameters, app_rows, install_rows)
            result["appCleanup"] = _gated_cleanup(
                apps.cleanup, session, common_job_parameters,
                len(install_rows), min_installs,
                "KandjiApp/KandjiAppVersion", "KANDJI_MIN_APP_INSTALLS_FOR_CLEANUP",
            )
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 - surface to the state machine
        logger.exception("Kandji sync failed")
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)
    return result
