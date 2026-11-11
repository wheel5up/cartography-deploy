# DESCRIBE_SECURITY_INTEGRATIONS

A read-only, owner's-rights stored procedure that lets the Cartography
collector role read the configuration of Snowflake security integrations,
without owning them or holding an admin role. Optional: without it, security
integrations aren't loaded, and the rest of the sync is unaffected.

| File | Purpose |
|---|---|
| [`describe_security_integrations.sql`](describe_security_integrations.sql) | Creates the schema, procedure and grants. Rollback at the bottom |
| [`preflight.sql`](preflight.sql) | Checks prerequisites and dry-runs the procedure body. Creates nothing |
| [`test_describe_security_integrations.py`](test_describe_security_integrations.py) | Offline unit test of the procedure body |

Examples use `CARTOGRAPHY_RO` for the collector role; replace it with yours.

## The name must match the fork

The cartography fork calls the procedure by a hardcoded name,
`DESCRIBE_PROCEDURE` in `cartography/intel/snowflake/security_integrations.py`:

```
DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS
```

The SQL here creates exactly that. If you deploy it under another name, change
the constant in the fork too (a fork release and a loader bump). **A mismatch
is silent:** the loader logs one INFO line and falls back to direct `DESC`.

## Why this exists

Security integrations are an account's federated sign-in and token trust
relationships: SAML SSO, SCIM provisioning, OAuth clients and External OAuth
issuers.

Reading their configuration needs `DESC SECURITY INTEGRATION`. For SAML2,
SCIM, Snowflake OAuth, External OAuth and OIDC integrations, Snowflake
requires **OWNERSHIP** for that
([DESCRIBE INTEGRATION, access control](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)).
`USAGE` isn't enough, and there's no read-only privilege for these types.
Without the procedure, every sync logs one warning per integration and loads
none of them:

```
Snowflake security integration <name> is unavailable (DESC INTEGRATION is not permitted) - skipping it and its cleanup.
```

Alternatives considered and rejected:

| Option | Why not |
|---|---|
| `GRANT USAGE ON INTEGRATION` | Doesn't permit `DESC` for these integration types |
| Transfer OWNERSHIP to the collector role | Full control (alter, drop, grant), and takes it away from the current owner |
| Grant `ACCOUNTADMIN` (or the owning role) to the collector role | Full admin for an inventory tool |
| Rebuild configuration from `ACCOUNT_USAGE.QUERY_HISTORY` DDL | Only goes back 365 days, so older integrations are missing, and UI-made changes may not appear |

An owner's-rights procedure is Snowflake's documented way to delegate a narrow
admin task without granting the underlying privilege
([caller's vs owner's rights](https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-rights)).

## What it does

1. Runs `SHOW SECURITY INTEGRATIONS`.
2. For each integration, runs `DESC SECURITY INTEGRATION "<name>"`.
3. Returns one `VARIANT`:

```json
{
  "version": 1,
  "integrations": [
    {
      "name": "EXAMPLE_EXTERNAL_OAUTH",
      "type": "EXTERNAL_OAUTH - OKTA",
      "category": "SECURITY",
      "enabled": "true",
      "comment": null,
      "created_on": "2026-08-06 12:29:34.626000-07:00",
      "properties": {
        "EXTERNAL_OAUTH_ISSUER": {"type": "String", "value": "https://example.okta.com/oauth2/...", "default": ""},
        "EXTERNAL_OAUTH_ANY_ROLE_MODE": {"type": "String", "value": "ENABLE", "default": "DISABLE"}
      },
      "error": null
    }
  ]
}
```

If one integration can't be described (e.g. the Snowflake-owned
`SNOWFLAKE$LOCAL_APPLICATION`), its `properties` is `null`, `error` holds the
reason, and the rest still return.

## Why it's read-only

- **No arguments.** The caller can't change what SQL runs. Integration names
  come from `SHOW` and are quoted before use.
- **Only `SHOW` and `DESC` run.** No `ALTER`, `CREATE`, `DROP` or `GRANT`.
- **The caller can run it but not change it.** Only the owner
  (`ACCOUNTADMIN`) can alter or replace the procedure. The collector role
  gets `USAGE` on the procedure, its schema and its database, nothing else.
- **Isolated schema.** A dedicated **managed access** schema holds nothing
  else, so `USAGE` on it exposes nothing else. With managed access, only the
  schema owner can grant on objects inside it.
- **No secrets returned.** `DESC` doesn't return OAuth client secrets
  (those need `SYSTEM$SHOW_OAUTH_CLIENT_SECRETS`). As a second layer, any
  property whose name contains `SECRET`, `PASSWORD` or `PRIVATE` is dropped.
  SAML certificates are public; cartography stores only their SHA-256
  fingerprint.
- **Picks up new integrations automatically.** It runs `SHOW` on every call,
  so there are no per-integration grants to maintain.

## Why ACCOUNTADMIN owns it

An owner's-rights procedure runs with its owner role's privileges, including
privileges inherited through the role hierarchy. The owner must therefore own,
or inherit ownership of, every security integration.

Integrations are usually created as `ACCOUNTADMIN`. One created by another
role is still covered if `ACCOUNTADMIN` inherits that role, for example
through `SYSADMIN`. `SECURITYADMIN` doesn't inherit `SYSADMIN`, so it's
usually the wrong owner. Check ownership before deploying (`preflight.sql`
step 4):

```sql
SHOW SECURITY INTEGRATIONS;
SHOW GRANTS ON INTEGRATION <name>;   -- for each one not created by ACCOUNTADMIN
```

## Testing before deploy

| Step | Who | What it proves |
|---|---|---|
| 1. `python3 test_describe_security_integrations.py` | Anyone, offline | Output shape, secret filtering, name quoting, one bad integration doesn't fail the rest, only `SHOW`/`DESC` run. Also fails if `preflight.sql`'s copy of the body drifts from the deploy script. No dependencies |
| 2. [`preflight.sql`](preflight.sql) | Admin, as `ACCOUNTADMIN` | Creates nothing. Checks the role exists, the schema name is free, Python/Snowpark is available and who owns each integration, then **dry-runs the exact procedure body** as an anonymous procedure and returns the real JSON |
| 3. [Verify](#verify) | Admin, then the collector role | The deployed procedure works when called as the collector role, and direct access is still denied |

The dry run in step 2 runs as the caller, so it doesn't exercise
`EXECUTE AS OWNER`; step 3 does. Steps 3 and 5 of `preflight.sql` can also be
run as the collector role: every `DESC` fails with
`Insufficient privileges to operate on integration`, which confirms the
per-integration error path.

## Deploy

Run [`describe_security_integrations.sql`](describe_security_integrations.sql)
as `ACCOUNTADMIN` after `preflight.sql` passes.

**Prerequisite:** Python stored procedures use the `snowflake-snowpark-python`
package from Snowflake's Anaconda channel. If the account hasn't accepted the
Anaconda terms, creation fails with a terms-acknowledgement error; an ORGADMIN
accepts them in Snowsight under **Admin → Terms**.

## Verify

As `ACCOUNTADMIN`:

```sql
CALL DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS();
SHOW GRANTS TO ROLE CARTOGRAPHY_RO;   -- expect USAGE on the procedure, schema and database
```

As the collector role:

```sql
-- Works: returns every integration
CALL DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS();

-- Still fails: no direct access was granted
DESC SECURITY INTEGRATION <name>;
ALTER SECURITY INTEGRATION <name> SET COMMENT = 'test';
CREATE OR REPLACE PROCEDURE DATA_PLATFORM.CARTOGRAPHY.DESCRIBE_SECURITY_INTEGRATIONS() RETURNS VARIANT LANGUAGE SQL AS 'SELECT 1';
```

Check the output for:

- one entry per row of `SHOW SECURITY INTEGRATIONS`
- `properties` filled in for everything except Snowflake-owned integrations
- no property names containing `SECRET`, `PASSWORD` or `PRIVATE`

## Rollback

Commented out at the bottom of the SQL file: revoke the grant, then drop the
procedure and schema (and the database, if the script created it).
Cartography falls back to direct `DESC`: one warning per integration, none
loaded.

## How the loader uses it

In the fork's `cartography/intel/snowflake/security_integrations.py`:

- `sync()` calls `CALL <DESCRIBE_PROCEDURE>()` once, converts the result into
  the same per-integration shape `DESC` produces, and runs the unchanged
  transform, load and cleanup.
- Any integration the procedure couldn't describe falls back to direct `DESC`.
- Procedure missing or not granted: one INFO line, then direct `DESC` for
  every integration.
- Procedure errors, returns no rows, or returns a payload that isn't
  `version: 1`: one WARNING, then the same fallback. It never fails the sync.
