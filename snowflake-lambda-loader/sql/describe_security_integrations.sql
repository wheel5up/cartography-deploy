-- =============================================================================
-- DESCRIBE_SECURITY_INTEGRATIONS: read-only delegation for the Cartography
-- collector role. See README.md in this directory before running.
--
-- Run as: ACCOUNTADMIN (must own, or inherit ownership of, every security
--         integration; see README "Why ACCOUNTADMIN owns it").
-- Replace CARTOGRAPHY_RO with your collector role.
--
-- The fully qualified name DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS
-- must match DESCRIBE_PROCEDURE in the cartography fork
-- (cartography/intel/snowflake/security_integrations.py). If you change it here,
-- change it there too, or the loader silently falls back to direct DESC.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Dedicated schema. Nothing else lives in it, so CARTOGRAPHY_RO's USAGE on
--    it exposes only this procedure. MANAGED ACCESS means only the schema owner
--    (ACCOUNTADMIN) can grant privileges on objects inside it. The database may
--    already exist; USAGE on a database alone exposes none of its schemas.
CREATE DATABASE IF NOT EXISTS DATA_PLATFORM;

CREATE SCHEMA IF NOT EXISTS DATA_PLATFORM.CARTOGRAPHY
  WITH MANAGED ACCESS
  COMMENT = 'Owner''s-rights procedures callable by CARTOGRAPHY_RO. ACCOUNTADMIN-owned; do not grant CREATE here.';

-- 2. The procedure. Takes no arguments, so the caller cannot influence the SQL.
--    Runs only SHOW SECURITY INTEGRATIONS and DESC SECURITY INTEGRATION.
CREATE OR REPLACE PROCEDURE DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS()
  RETURNS VARIANT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
  EXECUTE AS OWNER
  COMMENT = 'Read-only: returns SHOW + DESC output for every security integration, secrets excluded. Called by CARTOGRAPHY_RO.'
AS
$$
# Property names containing any of these are dropped from the output even
# though DESC is not expected to return secret values. Defence in depth.
_DENY = ("SECRET", "PASSWORD", "PRIVATE")


def _row(r):
    # SHOW/DESC column names come back lowercase and sometimes quoted.
    return {k.strip('"').lower(): v for k, v in r.as_dict().items()}


def _text(v):
    return None if v is None else str(v)


def run(session):
    integrations = []
    for show in sorted(
        (_row(r) for r in session.sql("SHOW SECURITY INTEGRATIONS").collect()),
        key=lambda r: r["name"],
    ):
        name = show["name"]
        entry = {
            "name": name,
            "type": _text(show.get("type")),
            "category": _text(show.get("category")),
            "enabled": _text(show.get("enabled")),
            "comment": _text(show.get("comment")),
            "created_on": _text(show.get("created_on")),
            "properties": None,
            "error": None,
        }
        quoted = '"' + name.replace('"', '""') + '"'
        try:
            props = {}
            for p in (_row(r) for r in session.sql(f"DESC SECURITY INTEGRATION {quoted}").collect()):
                key = str(p.get("property", ""))
                if any(d in key.upper() for d in _DENY):
                    continue
                props[key] = {
                    "type": _text(p.get("property_type")),
                    "value": _text(p.get("property_value")),
                    "default": _text(p.get("property_default")),
                }
            entry["properties"] = props
        except Exception as exc:  # e.g. Snowflake-owned SNOWFLAKE$LOCAL_APPLICATION
            entry["error"] = type(exc).__name__ + ": " + str(exc)[:300]
        integrations.append(entry)
    return {"version": 1, "integrations": integrations}
$$;

-- 3. Grants: the minimum to call it. No CREATE, no MODIFY, no OWNERSHIP.
GRANT USAGE ON DATABASE  DATA_PLATFORM             TO ROLE CARTOGRAPHY_RO;
GRANT USAGE ON SCHEMA    DATA_PLATFORM.CARTOGRAPHY TO ROLE CARTOGRAPHY_RO;
GRANT USAGE ON PROCEDURE DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS() TO ROLE CARTOGRAPHY_RO;

-- =============================================================================
-- Rollback
-- =============================================================================
-- REVOKE USAGE ON PROCEDURE DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS() FROM ROLE CARTOGRAPHY_RO;
-- DROP PROCEDURE DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS();
-- DROP SCHEMA DATA_PLATFORM.CARTOGRAPHY;
-- DROP DATABASE DATA_PLATFORM;  -- only if this script created it
