# Cartography Salesforce Lambda Loader

Syncs a Salesforce org's internal users and their access into Neo4j. One
Lambda (`src/handler.py`), one state machine
(`cartography-salesforce-lambda-sync`, one execution is one invocation), one
daily schedule (`cartography-salesforce-lambda-daily`, 08:30 UTC, **created
DISABLED**).

## What it ingests

`cartography.intel.salesforce` from the pinned cartography fork tag, in this
order:

```
(:SalesforceOrganization)
(:SalesforceOrganization)-[:RESOURCE]->(:SalesforceProfile)
(:SalesforceOrganization)-[:RESOURCE]->(:SalesforceUserRole)
(:SalesforceOrganization)-[:RESOURCE]->(:SalesforceUser:UserAccount)
(:SalesforceUser)-[:HAS_ROLE]->(:SalesforceProfile)
(:SalesforceUser)-[:MEMBER_OF]->(:SalesforceUserRole)
(:SalesforceOrganization)-[:RESOURCE]->(:SalesforcePermissionSet)   # assigned to users
(:SalesforceOrganization)-[:RESOURCE]->(:SalesforceGroup)           # user + nested group members
(:SalesforceOrganization)-[:RESOURCE]->(:SalesforceConnectedApp)    # AUTHORIZED by users (OAuth tokens)
```

Profiles carry `PermissionsModifyAllData`, `ViewAllData`, `ApiEnabled` and
`ManageUsers`; standalone permission sets carry `ModifyAllData`,
`ViewAllData` and `ApiEnabled`. The ontology loader can link `SalesforceUser`
to a person `User` by email.

### Internal users only

An org with Experience Cloud (community) sites can have far more portal users
than employees. The fork's `users.get()` has no filter, so the handler runs
its own queries:

| Object | Filter |
|---|---|
| User | `UserType IN ('Standard')` (`SalesforceUserTypes` parameter) |
| PermissionSetAssignment | `Assignee.UserType IN (...)` |
| UserRole | `PortalType = 'None'` (drops community/portal roles) |
| GroupMember, OAuthToken | kept only for synced users (and nested groups) |

`Standard` covers every internal licence, including Salesforce Integration
users. Groups themselves aren't filtered. If you widen `SalesforceUserTypes`
to include community users, raise `LambdaMemoryMb`.

## Setup (Salesforce)

Needs a Salesforce admin.

1. **Integration user:** licence `Salesforce Integration`, profile
   `Minimum Access - API Only Integrations`, no role.
2. **Permission set** `Cartography API Access`, assigned to that user: API
   Enabled, View Setup and Configuration, View All Users, View Roles and Role
   Hierarchy. Without View All Users the user list is partial (see
   [Cleanup guard](#cleanup-guard)). Connected apps also need access to
   `ConnectedApplication` and `OAuthToken`; without it the run still succeeds
   with `connectedApps: "unavailable"`.
3. **Key pair**, generated locally. Only the certificate goes to Salesforce:
   ```sh
   openssl req -x509 -newkey rsa:2048 -nodes -days 730 \
     -keyout salesforce-cartography.key -out salesforce-cartography.crt \
     -subj "/CN=cartography"
   ```
4. **External Client App** (Salesforce blocks new Connected Apps since
   Spring '26): Distribution State `Local`; Enable OAuth; callback
   `https://localhost/callback` (unused); scopes `api` and
   `refresh_token, offline_access`; **Enable JWT Bearer Flow** with
   `salesforce-cartography.crt`. Policies: Permitted Users = *Admin approved
   users are pre-authorized*, permission set `Cartography API Access`. Copy
   the **Consumer Key**.

## Setup (SSM)

All read at run time, so the stack can be deployed first. Runs fail with
"Salesforce SSM config incomplete" until these exist.

```sh
aws ssm put-parameter --type String \
  --name /cartography/salesforce/login-url --value 'https://<mydomain>.my.salesforce.com'
aws ssm put-parameter --type String \
  --name /cartography/salesforce/client-id --value '<consumer key>'
aws ssm put-parameter --type String \
  --name /cartography/salesforce/username --value '<integration user username>'
aws ssm put-parameter --type SecureString \
  --name /cartography/salesforce/private-key --value file://salesforce-cartography.key
rm salesforce-cartography.key
```

`/cartography/salesforce/client-secret` (client credentials flow) also works
instead of `username` + `private-key`, but JWT bearer is the intended setup.

## Deploy

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build --use-container
sam deploy
```

The log group `/cartography/salesforce-lambda-loader` is in the dashboard's
`LoaderLogGroups`; redeploy `../dashboard/` once to pick it up.

## Validate

```sh
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> --input '{}'
```

The output reports `orgId`, `counts` (profiles, userRoles, users,
permissionSets, permissionSetAssignments, groups, groupMembers,
connectedApps), `cleanup` per kind, `connectedApps` and `durationSeconds`.
`counts.users` should be close to the org's internal user count, active and
inactive. Then:

```cypher
MATCH (u:SalesforceUser) RETURN u.user_type, u.is_active, count(*);

// Who has Modify All Data, via profile or permission set (both are HAS_ROLE)
MATCH (u:SalesforceUser {is_active: true})-[:HAS_ROLE]->(g)
WHERE (g:SalesforceProfile OR g:SalesforcePermissionSet) AND g.permissions_modify_all_data
RETURN u.username, collect(g.name) AS grants ORDER BY u.username;
```

## Cleanup guard

The handler doesn't call `start_salesforce_ingestion()` or any module's
`sync()`, because those always clean up. An integration user missing View All
Users gets a successful but partial user list, and cleanup would then delete
everyone else.

Instead the handler fetches everything first (any API error fails the run
with nothing written), loads in the fork's order, and gates each cleanup:
users on `SalesforceMinUsersForCleanup` (default 1), everything else on a
non-empty fetch. After the first real run, raise the user floor to about 80%
of the user count in `samconfig.toml`:

```toml
"SalesforceMinUsersForCleanup=<n>"
```

## Schedule

Created **DISABLED**. Once a manual run checks out, set
`SalesforceLambdaSchedule.State` to `ENABLED` in `template.yaml` and
redeploy. A CLI change is reverted by the next deploy.

## Notes

- The handler reuses the fork's `users._FIELDS` and `userroles._FIELDS`, so
  its filtered queries return the columns the schemas expect. Re-check them
  when moving `CARTOGRAPHY_GIT_REF` in the `Makefile`.
- The fork's `SalesforceClient` uses API `v60.0`.
