"""
Cartography ontology - Lambda handler.

Calls cartography.intel.ontology.run() UNMODIFIED, the same entry point the
CLI's `--selected-modules ontology` runs. No external API and no credentials:
it reads what the other loaders already wrote to Neo4j and builds the shared
cross-provider layer on top (User, Device, DNS/package/public-IP
normalization, plus the cross-provider linking analysis jobs).

Sources of truth:
  ONTOLOGY_USERS_SOURCE=okta     only OktaUser may create canonical User nodes
  ONTOLOGY_DEVICES_SOURCE=kandji only KandjiDevice may create canonical Device
Every other provider's accounts/devices link to those existing nodes (users
by email, devices by serial) but cannot create new ones.

Cleanup matches upstream, by design: each run deletes canonical nodes no
source produced this run. A failed/empty Okta or Kandji sync would remove
User/Device nodes until the next good sync restores them.

Runs last in the daily stagger (see template.yaml ScheduleDefault) so it reads
that day's data; it is not chained to the other runs.

INVOCATION CONTRACT
-------------------
  {"update_tag"?: <int>}   omit to start a new run with a fresh tag

Returns:
  {"status": "SUCCEEDED"|"FAILED", "updateTag": ..., "durationSeconds": ...,
   "error"?: "..."}
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


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    import cartography.intel.ontology
    from cartography.config import Config

    start = time.time()
    update_tag = int(event.get("update_tag") or time.time())
    result: dict[str, Any] = {"status": "FAILED", "updateTag": update_tag}

    driver = None
    try:
        neo4j_uri = os.environ["NEO4J_URI"]
        driver = neo4j.GraphDatabase.driver(
            neo4j_uri,
            auth=("neo4j", _get_neo4j_password()),
            max_connection_lifetime=int(os.environ.get("NEO4J_MAX_CONNECTION_LIFETIME", "200")),
        )
        config = Config(
            neo4j_uri=neo4j_uri,
            update_tag=update_tag,
            ontology_users_source=os.environ.get("ONTOLOGY_USERS_SOURCE") or None,
            ontology_devices_source=os.environ.get("ONTOLOGY_DEVICES_SOURCE") or None,
        )
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            cartography.intel.ontology.run(session, config)
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 - surface to the state machine
        logger.exception("Ontology sync failed")
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)
    return result
