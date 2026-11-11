"""
Cartography Salesforce sync - Lambda handler.

Calls cartography's Python API directly (not the CLI), same approach as the
other *-lambda-loader stacks. Salesforce is single-org: one External Client
App, one integration user, no fan-out.

What cartography.intel.salesforce ingests (cartography fork), in load order:
  - Organization            -> (:SalesforceOrganization)
  - Profile                 -> (:SalesforceOrganization)-[:RESOURCE]->(:SalesforceProfile)
  - UserRole                -> (:SalesforceUserRole), parent links
  - User                    -> (:SalesforceUser:UserAccount)-[:HAS_ROLE]->(:SalesforceProfile),
                               -[:MEMBER_OF]->(:SalesforceUserRole)
  - PermissionSet (+ assignments, standalone sets only)
                            -> (:SalesforcePermissionSet) assigned to SalesforceUser
  - Group (+ members)       -> (:SalesforceGroup), MEMBER_OF from users and nested groups
  - ConnectedApplication (+ OAuthToken)
                            -> (:SalesforceConnectedApp) AUTHORIZED by SalesforceUser

INTERNAL USERS ONLY
-------------------
An org with Experience Cloud sites can have far more community users than
employees. The fork's users.get() is
`SELECT ... FROM User` with no filter, so this handler runs its own queries:
  - User:                    WHERE UserType IN (SALESFORCE_USER_TYPES)
  - PermissionSetAssignment: WHERE Assignee.UserType IN (...)
  - UserRole:                WHERE PortalType = 'None' (drops portal roles)
  - GroupMember / OAuthToken: filtered in Python to internal user ids (and,
    for groups, nested group ids). GroupMember.UserOrGroupId is polymorphic,
    and SOQL can't OR two semi-joins.
SALESFORCE_USER_TYPES defaults to "Standard", which covers every internal
licence (Salesforce, Salesforce Integration, Analytics Cloud Integration, ...).
Add "CsnOnly" to include Chatter Free users.

WHY THIS DOES NOT CALL start_salesforce_ingestion() / <module>.sync()
--------------------------------------------------------------------
Each sync() is get -> transform -> load -> cleanup, and cleanup deletes every
node under the org not stamped with this run's update_tag. An integration
user missing "View All Users" gets a 200 with only the users it can see, and
sync() would then delete the rest. So: fetch everything first (any API error
fails the run with the graph untouched), load in the fork's order, and gate
each cleanup on that fetch's count. Users use SALESFORCE_MIN_USERS_FOR_CLEANUP;
everything else only refuses cleanup on an empty fetch.

Connected apps follow the fork: if ConnectedApplication/OAuthToken are not
readable by the integration user, they are skipped (no load, no cleanup) and
the run still succeeds with connectedApps="unavailable".

AUTH (SSM, read at invoke time, prefix SALESFORCE_SSM_PREFIX)
-------------------------------------------------------------
  client-id                    External Client App consumer key (required)
  login-url                    optional, default https://login.salesforce.com
  username + private-key       JWT bearer flow (preferred)
  client-secret                client credentials flow (alternative)

INVOCATION CONTRACT
-------------------
  {"update_tag"?: <int>}   omit to start a new run with a fresh tag

Returns:
  {"status": "SUCCEEDED"|"FAILED", "updateTag": ..., "orgId": ...,
   "counts": {...}, "cleanup": {<kind>: "ran"|"skipped"},
   "connectedApps": "loaded"|"unavailable",
   "durationSeconds": ..., "error"?: "..."}
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import Any

import boto3
import neo4j

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

DEFAULT_LOGIN_URL = "https://login.salesforce.com"


def _get_salesforce_config() -> dict[str, str]:
    """All auth values from SSM in one call. Missing optional names are fine.

    Read at run time, not resolved into env vars at deploy time: the stack
    deploys before they exist, a change takes effect on the next run, and the
    private key never lands in the function configuration.
    """
    prefix = os.environ.get("SALESFORCE_SSM_PREFIX", "/cartography/salesforce").rstrip("/")
    keys = ("login-url", "client-id", "username", "private-key", "client-secret")
    names = {f"{prefix}/{k}": k for k in keys}
    response = boto3.client("ssm").get_parameters(Names=list(names), WithDecryption=True)
    cfg = {names[p["Name"]]: p["Value"].strip() for p in response["Parameters"]}

    has_jwt = bool(cfg.get("username") and cfg.get("private-key"))
    if not cfg.get("client-id") or not (has_jwt or cfg.get("client-secret")):
        raise RuntimeError(
            f"Salesforce SSM config incomplete under {prefix}: need client-id plus "
            "either username + private-key (JWT bearer) or client-secret; "
            f"found {sorted(cfg)}. See README.md 'Setup'."
        )
    cfg.setdefault("login-url", DEFAULT_LOGIN_URL)
    return cfg


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


def _user_types() -> list[str]:
    raw = os.environ.get("SALESFORCE_USER_TYPES", "Standard")
    types = [t.strip() for t in raw.split(",") if t.strip()]
    for t in types:
        if not t.isalnum():
            raise ValueError(f"Invalid SALESFORCE_USER_TYPES entry: {t!r}")
    return types


def _soql_in(values: list[str]) -> str:
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


def _gated_cleanup(
    cleanup_fn: Callable[[neo4j.Session, dict[str, Any]], None],
    session: neo4j.Session,
    common_job_parameters: dict[str, Any],
    count: int,
    min_for_cleanup: int,
    kind: str,
) -> str:
    if count >= min_for_cleanup:
        cleanup_fn(session, common_job_parameters)
        return "ran"
    logger.warning(
        "Skipping %s cleanup: fetched %d, below minimum %d. Existing nodes are preserved.",
        kind, count, min_for_cleanup,
    )
    return "skipped"


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    import requests

    from cartography.intel.salesforce import connectedapps
    from cartography.intel.salesforce import groups
    from cartography.intel.salesforce import organization
    from cartography.intel.salesforce import permissionsets
    from cartography.intel.salesforce import profiles
    from cartography.intel.salesforce import userroles
    from cartography.intel.salesforce import users
    from cartography.intel.salesforce.util import get_salesforce_client

    start = time.time()
    update_tag = int(event.get("update_tag") or time.time())
    result: dict[str, Any] = {"status": "FAILED", "updateTag": update_tag}
    counts: dict[str, int] = {}
    cleanup: dict[str, str] = {}

    driver = None
    try:
        cfg = _get_salesforce_config()
        min_users = int(os.environ.get("SALESFORCE_MIN_USERS_FOR_CLEANUP", "1"))
        types_in = _soql_in(_user_types())

        client = get_salesforce_client(
            login_url=cfg["login-url"],
            client_id=cfg["client-id"],
            client_secret=cfg.get("client-secret"),
            username=cfg.get("username"),
            private_key=cfg.get("private-key"),
        )

        # ── Fetch everything before writing anything ──────────────────────
        org = organization.transform(organization.get(client))
        result["orgId"] = org["Id"]

        profile_rows = profiles.transform(profiles.get(client))
        role_rows = client.query_all(
            f"SELECT {userroles._FIELDS} FROM UserRole WHERE PortalType = 'None'"
        )
        user_rows = users.transform(
            client.query_all(f"SELECT {users._FIELDS} FROM User WHERE UserType IN {types_in}")
        )
        internal_ids = {u["Id"] for u in user_rows}

        permission_set_rows = permissionsets.transform(
            permissionsets.get_permission_sets(client),
            client.query_all(
                "SELECT Id, AssigneeId, PermissionSetId FROM PermissionSetAssignment "
                f"WHERE PermissionSet.IsOwnedByProfile = false AND Assignee.UserType IN {types_in}"
            ),
        )

        group_list = groups.get_groups(client)
        group_ids = {g["Id"] for g in group_list}
        members = [
            m for m in groups.get_members(client)
            if m["UserOrGroupId"] in internal_ids or m["UserOrGroupId"] in group_ids
        ]
        group_rows = groups.transform(group_list, members)

        try:
            app_rows: list[dict[str, Any]] | None = connectedapps.transform(
                connectedapps.get_apps(client),
                [t for t in connectedapps.get_oauth_tokens(client) if t["UserId"] in internal_ids],
            )
            result["connectedApps"] = "loaded"
        except requests.HTTPError as exc:
            if not connectedapps._is_access_error(exc):
                raise
            logger.warning(
                "ConnectedApplication/OAuthToken not readable by the integration user; "
                "skipping connected apps and preserving previously synced data."
            )
            app_rows = None
            result["connectedApps"] = "unavailable"

        counts.update(
            profiles=len(profile_rows),
            userRoles=len(role_rows),
            users=len(user_rows),
            permissionSets=len(permission_set_rows),
            permissionSetAssignments=sum(len(p["_assignee_ids"]) for p in permission_set_rows),
            groups=len(group_rows),
            groupMembers=len(members),
        )
        if app_rows is not None:
            counts["connectedApps"] = len(app_rows)
        logger.info("Fetched from Salesforce org %s: %s", org["Id"], counts)

        # ── Load in the fork's order, gate each cleanup ───────────────────
        cjp = {"UPDATE_TAG": update_tag, "ORG_ID": org["Id"]}
        driver = _get_neo4j_driver()
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            organization.load_organization(session, org, update_tag)
            organization.cleanup(session, cjp)

            # Profiles and roles before users so HAS_ROLE / MEMBER_OF attach.
            profiles.load_profiles(session, profile_rows, org["Id"], update_tag)
            cleanup["profiles"] = _gated_cleanup(
                profiles.cleanup, session, cjp, len(profile_rows), 1, "SalesforceProfile")

            userroles.load_user_roles(session, role_rows, org["Id"], update_tag)
            cleanup["userRoles"] = _gated_cleanup(
                userroles.cleanup, session, cjp, len(role_rows), 1, "SalesforceUserRole")

            users.load_users(session, user_rows, org["Id"], update_tag)
            cleanup["users"] = _gated_cleanup(
                users.cleanup, session, cjp, len(user_rows), min_users, "SalesforceUser")

            permissionsets.load_permission_sets(session, permission_set_rows, org["Id"], update_tag)
            cleanup["permissionSets"] = _gated_cleanup(
                permissionsets.cleanup, session, cjp, len(permission_set_rows), 1,
                "SalesforcePermissionSet")

            groups.load_groups(session, group_rows, org["Id"], update_tag)
            cleanup["groups"] = _gated_cleanup(
                groups.cleanup, session, cjp, len(group_rows), 1, "SalesforceGroup")

            if app_rows is not None:
                connectedapps.load_connected_apps(session, app_rows, org["Id"], update_tag)
                cleanup["connectedApps"] = _gated_cleanup(
                    connectedapps.cleanup, session, cjp, len(app_rows), 1,
                    "SalesforceConnectedApp")
        result["status"] = "SUCCEEDED"
    except Exception as exc:  # noqa: BLE001 - surface to the state machine
        logger.exception("Salesforce sync failed")
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        if driver is not None:
            driver.close()
        result["counts"] = counts
        result["cleanup"] = cleanup
        result["durationSeconds"] = round(time.time() - start, 1)
    return result
