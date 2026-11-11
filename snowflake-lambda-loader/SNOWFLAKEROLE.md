# Snowflake collector role: grants and network policy

The Snowflake user, role and grants this loader needs, plus the optional
grants that fill visibility gaps, and the network policy.

The base setup (service user, warehouse, key pair, read-only role with
`IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`, `MONITOR ON ACCOUNT` and
per-database `USAGE`/`REFERENCES` grants) is in the cartography fork's
Snowflake docs: `docs/root/modules/snowflake/config.md`. This file covers what
those docs leave out. Examples use `CARTOGRAPHY_SVC` for the user and
`CARTOGRAPHY_RO` for the role.

## Identity

- **User:** a `TYPE = SERVICE` user (no interactive login, no password).
- **Role:** a custom read-only role, set as the user's **default role**. The
  object API endpoints run as the default role and take no role parameter.
- **Warehouse:** a dedicated `XSMALL` warehouse with `AUTO_SUSPEND`.
- **Auth:** RSA key pair (JWT). The PEM private key and passphrase go into
  SSM under `/cartography/snowflake/` with `scripts/bootstrap-snowflake-ssm.sh`.

## Missing grants are gaps, not failures

Without any of the optional grants below, the sync still runs. The affected
module logs a warning, skips the objects it can't read, and is marked
incomplete, so its cleanup is skipped. Nothing is deleted. You lose
visibility, not data.

## Optional grants

| Grant | Object | Scales to new objects? | Adds |
|---|---|---|---|
| `MONITOR` | `SERVICE`, per database | Yes (`ALL` / `FUTURE`) | Containers, images, compute pools, service endpoint exposure |
| `USAGE` | `INTEGRATION`, one per integration | No | Integration details from `DESC INTEGRATION` |
| `MODIFY PROGRAMMATIC AUTHENTICATION METHODS` | `USER`, one per user | No | Per-token network-policy bypass. **Not recommended** |

### `MONITOR` on services

Snowpark Container Services objects, including the service behind every
Streamlit-in-Snowflake app, only have `MONITOR` and `OPERATE` privileges.
`MONITOR` is read-only (status, containers, endpoints, roles). Don't grant
`OPERATE`, which can suspend, resume and modify services.

Without it:

```
003001: insufficient privileges to operate on service '<name>'. your primary
role ... must have monitor granted on service <db>.<schema>.<name>.
```

With it, the graph gets container inventory (image, compute pool) and
`SnowflakeServiceEndpoint.is_public` / `ingress_url`: internet exposure of
workloads running inside Snowflake.

```sql
GRANT MONITOR ON ALL SERVICES IN DATABASE <db> TO ROLE CARTOGRAPHY_RO;
GRANT MONITOR ON FUTURE SERVICES IN DATABASE <db> TO ROLE CARTOGRAPHY_RO;
```

Repeat per database. New services are covered automatically.

### `USAGE` on integrations

`SHOW <TYPE> INTEGRATIONS` only returns name, type and basic metadata. The
useful part, for example a storage integration's IAM role ARN, external ID
and allowed locations, needs `DESC INTEGRATION <name>`, which needs `USAGE` on
that integration. This applies to storage, security, API, catalog and
notification integrations.

Without it, one warning per integration:

```
Snowflake storage integration <name> is unavailable (DESC INTEGRATION is not
permitted) - skipping it and its cleanup.
```

```sql
GRANT USAGE ON INTEGRATION <name> TO ROLE CARTOGRAPHY_RO;
```

Integrations are account-level objects, and Snowflake has no `ALL` / `FUTURE`
grant for account-level objects. Run `SHOW INTEGRATIONS` as an admin for the
list, and grant each new integration as it's created.

For security integrations, the stored procedure in [`sql/`](sql/README.md)
returns the `DESC` output without this grant.

### `MODIFY PROGRAMMATIC AUTHENTICATION METHODS` on users: not recommended

This grant lets the role list another user's programmatic access tokens
(`SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER <user>`). It isn't read-only:
the same privilege lets the role create, rotate and delete that user's
tokens. Like integrations, it has no bulk or future form, so it would be one
grant per user.

**You don't need it to see PATs.** The `credentials` step reads
`SNOWFLAKE.ACCOUNT_USAGE.CREDENTIALS`, which only needs
`IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`, and loads `SnowflakeCredential`
nodes for every PAT, key pair, passkey and TOTP: name, user, status,
expiration, last used, created by, and role restriction (in
`additional_details`, e.g. `{"ROLE_RESTRICTION": ["ANALYST"]}`; `{}` means
unrestricted). Token secrets are never exposed either way; Snowflake shows a
token's value once, at creation.

The only thing lost without the grant is each token's network-policy bypass
setting (`mins_to_bypass_network_policy_requirement`). User-level network
policy is still on the user node (`network_policy_name`).

Without the grant, every run logs this warning from the `access_tokens` step.
It's expected:

```
Snowflake programmatic access tokens is unavailable (N of M users could not be
read (MODIFY PROGRAMMATIC AUTHENTICATION METHODS on each user is required to
see that user's tokens)) - skipping it and its cleanup.
```

## Network policy

Restrict the service user to the IP the Lambda connects from. The Lambda runs
in a private subnet, so that's the public IP of the subnet's NAT gateway. A
NAT gateway uses an Elastic IP, so the address stays fixed until the NAT
gateway is replaced.

```sh
aws ec2 describe-nat-gateways --nat-gateway-ids <nat-id> \
  --query 'NatGateways[0].NatGatewayAddresses[0].PublicIp' --output text
```

Attach the policy to the user, not the account, so it only constrains the
collector. Snowflake recommends network rules over the older
`ALLOWED_IP_LIST`:

```sql
USE ROLE SECURITYADMIN;

CREATE NETWORK RULE CARTOGRAPHY_EGRESS
  TYPE = IPV4 MODE = INGRESS
  VALUE_LIST = ('203.0.113.10')
  COMMENT = 'NAT gateway egress IP of the Cartography Lambda loader';

CREATE NETWORK POLICY CARTOGRAPHY_POLICY
  ALLOWED_NETWORK_RULE_LIST = ('CARTOGRAPHY_EGRESS')
  COMMENT = 'Restricts the Cartography service user to its collector';

ALTER USER CARTOGRAPHY_SVC SET NETWORK_POLICY = CARTOGRAPHY_POLICY;
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN USER CARTOGRAPHY_SVC;
```

If you move the loader to another subnet or replace the NAT gateway, update
the rule before the next run, or every connection is refused.
