"""
Cartography Anthropic sync - Lambda handler.

Calls cartography.intel.anthropic.start_anthropic_ingestion() UNMODIFIED, the
same entry point the CLI's `--selected-modules anthropic` runs. Only the
Config is built differently: the Admin API key comes from SSM at run time
instead of an environment variable.

What it ingests (unchanged upstream): org users, workspaces (with
member/admin edges) and API-key metadata (never the secret), under one
(:AnthropicOrganization) whose id comes from the API's
anthropic-organization-id response header.

Matches upstream exactly, by design: users, workspaces and
apikeys each run their own cleanup right after their load, with no guard
against an empty fetch or a missing org-id header. See README.md.

One invocation, no splits: three paged calls plus one call per workspace.

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


def _get_admin_key() -> str:
    """Anthropic Admin API key from SSM at invoke time, never a Lambda env var
    (env vars are plaintext to anyone with lambda:GetFunctionConfiguration).
    ANTHROPIC_ADMIN_KEY_SSM_PARAM is the parameter NAME only."""
    response = boto3.client("ssm").get_parameter(
        Name=os.environ.get(
            "ANTHROPIC_ADMIN_KEY_SSM_PARAM", "/cartography/anthropic/admin-key"
        ),
        WithDecryption=True,
    )
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


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    from cartography.config import Config
    from cartography.intel.anthropic import start_anthropic_ingestion

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
            anthropic_apikey=_get_admin_key(),
        )
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            start_anthropic_ingestion(session, config)
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 - surface to the state machine
        logger.exception("Anthropic sync failed")
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)
    return result
