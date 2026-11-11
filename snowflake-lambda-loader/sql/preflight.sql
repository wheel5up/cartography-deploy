-- =============================================================================
-- PREFLIGHT for describe_security_integrations.sql. Creates NOTHING.
-- Run as ACCOUNTADMIN in a Snowsight worksheet, top to bottom. Each step says
-- what a pass looks like. If all pass, the deploy script will work.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- 1. Target role exists. PASS: one row.
SHOW ROLES LIKE 'CARTOGRAPHY_RO';  -- your collector role

-- 2. Schema name is free. PASS: no row with database_name DATA_PLATFORM, or
--    that schema is yours and holds nothing else.
SHOW SCHEMAS LIKE 'CARTOGRAPHY' IN ACCOUNT;

-- 3. Python + Snowpark available (Anaconda terms accepted).
--    PASS: at least one row. FAIL: zero rows -> ORGADMIN accepts terms in
--    Snowsight under Admin -> Terms.
SELECT package_name, MAX(version) AS latest
FROM SNOWFLAKE.INFORMATION_SCHEMA.PACKAGES
WHERE language = 'python' AND package_name = 'snowflake-snowpark-python'
GROUP BY package_name;

-- 4. Ownership: who owns each security integration.
--    PASS: every row's grantee_name is ACCOUNTADMIN or a role ACCOUNTADMIN
--    inherits (e.g. SYSADMIN, or a role granted to SYSADMIN). Snowflake-owned
--    ones may show no row.
SHOW SECURITY INTEGRATIONS;
SELECT "name" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));  -- the list to check
-- then, for anything not created by ACCOUNTADMIN:
-- SHOW GRANTS ON INTEGRATION <name>;

-- 5. DRY RUN of the exact procedure body as an anonymous procedure.
--    Runs SHOW/DESC as ACCOUNTADMIN and returns the same JSON the real
--    procedure will. Creates no objects.
--    PASS: one VARIANT row; every integration has "properties" filled in and
--    "error": null, except Snowflake-owned SNOWFLAKE$LOCAL_APPLICATION.
--    If EVERY entry has "Insufficient privileges to operate on integration",
--    the worksheet isn't running as ACCOUNTADMIN (check the role picker).
--    If only some do, those integrations are owned by a role ACCOUNTADMIN
--    doesn't inherit; see step 4.
--    The body below MUST stay identical to the deploy script; the unit test
--    (test_describe_security_integrations.py) enforces that.
WITH describe_security_integrations AS PROCEDURE ()
  RETURNS VARIANT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'run'
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
$$
CALL describe_security_integrations();
