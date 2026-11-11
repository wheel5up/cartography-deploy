# Cartography Kandji (Iru) Lambda Loader

Syncs Kandji users, devices and installed apps into Neo4j. One Lambda
(`src/handler.py`), one state machine (`cartography-kandji-lambda-sync`, one
execution is one invocation), one daily schedule
(`cartography-kandji-lambda-daily`, 08:00 UTC, created `ENABLED`).

## What it ingests

`cartography.intel.kandji` from the pinned cartography fork tag (the fork adds
users and apps on top of upstream). Users load first, so device ownership can
link to them, then devices, then apps.

| Call | Notes |
|---|---|
| `GET {base_uri}/api/v1/users` | Cursor-paginated |
| `GET {base_uri}/api/v1/devices` | Paged |
| `GET {base_uri}/api/v1/devices/{id}/apps` | One per device, 8 in parallel |

```
(:KandjiTenant)-[:RESOURCE]->(:KandjiUser:UserAccount)
(:KandjiTenant)-[:RESOURCE]->(:KandjiDevice)
(:KandjiUser)-[:OWNS]->(:KandjiDevice)            # device's assigned user
(:KandjiDevice)-[:ENROLLED_TO]->(:KandjiTenant)   # deprecated, upstream back-compat
(:KandjiTenant)-[:RESOURCE]->(:KandjiApp {id: bundle_id, name})
(:KandjiTenant)-[:RESOURCE]->(:KandjiAppVersion {id: "bundle_id:version", version})
(:KandjiAppVersion)-[:VERSION]->(:KandjiApp)
(:KandjiDevice)-[:HAS_INSTALLED {path, source, signature,
                                 modification_date, creation_date}]->(:KandjiAppVersion)
```

**`KandjiUser`** carries `email`, `name`, `active`, `archived`, `department`,
`job_title`, `device_count`, `integration_type`, `created_at` and
`updated_at`. The ontology loader can link it to a person `User` by email
and derive `(:User)-[:OWNS]->(:Device)` from the device assignment.

**`KandjiDevice`** carries `device_id`, `device_name`, `serial_number`,
`model`, `os_version`, `platform`, `last_check_in`, `udid`, `asset_tag`,
`is_missing`, `mdm_enabled`, `agent_installed`, `agent_version`,
`blueprint_id`, `blueprint_name`, `first_enrollment`, `last_enrollment` and
`tags`. No compliance results. Its main value is the **serial-number join**
to other device sources, such as `OktaDevice`:

```cypher
// Macs signing into Okta that are not enrolled in Kandji
MATCH (o:OktaDevice {platform: 'MACOS'})
WHERE NOT EXISTS { MATCH (k:KandjiDevice {serial_number: o.serial_number}) }
RETURN o.display_name, o.serial_number
```

**Apps** include Apple built-ins and iPads. Kandji's API limits are 50 req/s
and 10,000 req/hour, and a run makes about one request per device. 429 and
5xx responses are retried with backoff; a device deleted mid-run (404) counts
as no apps. Apps aren't mapped into the ontology `Package` layer (Mac bundles
have no purl).

## Setup (SSM)

Kandji was rebranded **Iru**. Confirm the API URL in the admin console
(Settings → Access → API Token) rather than assuming `api.kandji.io`.

```sh
# Read-only token scoped to Devices and Users (/api/v1/devices, /api/v1/users).
aws ssm put-parameter --type SecureString \
  --name /cartography/kandji/token --value '<token>'

# e.g. https://<subdomain>.api.kandji.io  (EU: https://<subdomain>.api.eu.kandji.io)
aws ssm put-parameter --type String \
  --name /cartography/kandji/base-uri --value 'https://<subdomain>.api.kandji.io'

# Any stable id for the single KandjiTenant node; the subdomain is a good choice.
aws ssm put-parameter --type String \
  --name /cartography/kandji/tenant-id --value '<subdomain>'
```

`base-uri` must include `https://`. cartography uses it verbatim, and a bare
hostname fails with `MissingSchema` before any request is made.

All three are read at run time, so the stack can be deployed first (runs fail
with "Missing SSM parameter(s)" until they exist), and changes take effect on
the next run without a redeploy.

## Deploy

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build --use-container
sam deploy
```

## Validate

```sh
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> --input '{}'
```

The output reports `userCount`, `userCleanup`, `deviceCount`, `cleanup`,
`appCount`, `appInstallCount`, `appCleanup` (each cleanup `ran` or
`skipped`) and `durationSeconds`. Check the counts against the Iru console,
then:

```cypher
MATCH (:KandjiTenant)-[:RESOURCE]->(d:KandjiDevice) RETURN count(d);
MATCH (:KandjiTenant)-[:RESOURCE]->(u:KandjiUser) RETURN count(u);
MATCH (:KandjiUser)-[:OWNS]->(d:KandjiDevice) RETURN count(d);
```

## Cleanup guard

The handler doesn't call `users.sync()`, `devices.sync()` or `apps.sync()`.
Those always run cleanup, so an empty but successful API response (wrong base
URI, a token without the scope, an outage page returning 200) would delete
every `KandjiUser` or `KandjiDevice`.

Instead the handler fetches everything first (any API error fails the run
with nothing written), then loads and gates each cleanup separately. A
cleanup is skipped when its fetch returned fewer rows than
`KandjiMinUsersForCleanup`, `KandjiMinDevicesForCleanup` or
`KandjiMinAppInstallsForCleanup` (install rows). All default to 1, so only an
empty fetch is refused. After a few runs, raise them to a floor below the
real counts, for example 80%, in `samconfig.toml`:

```toml
"KandjiMinUsersForCleanup=<n>",
"KandjiMinDevicesForCleanup=<n>",
"KandjiMinAppInstallsForCleanup=<n>"
```

## Schedule

To pause it, set `KandjiLambdaSchedule.State` to `DISABLED` in
`template.yaml` and redeploy. A CLI change is reverted by the next deploy.

## Example queries

```cypher
// Missing or unmanaged devices and their owners
MATCH (d:KandjiDevice) WHERE d.is_missing OR NOT d.mdm_enabled OR NOT d.agent_installed
OPTIONAL MATCH (u:KandjiUser)-[:OWNS]->(d)
RETURN d.device_name, d.serial_number, d.is_missing, d.mdm_enabled,
       d.agent_installed, d.last_check_in, u.email
ORDER BY d.last_check_in;

// Chrome major version across the fleet, with on-disk bundle age
MATCH (d:KandjiDevice)-[r:HAS_INSTALLED]->(v:KandjiAppVersion)-[:VERSION]->(:KandjiApp {id:'com.google.Chrome'})
RETURN split(v.version,'.')[0] AS major, count(d) AS devices, max(r.modification_date) AS newest_mod
ORDER BY devices DESC;

// Who has a given app installed (via the ontology owner)
MATCH (u:User)-[:OWNS]->(:Device)-[:OBSERVED_AS]->(k:KandjiDevice)
      -[r:HAS_INSTALLED]->(v:KandjiAppVersion)-[:VERSION]->(a:KandjiApp {id:$bundle_id})
RETURN u.email, k.device_name, v.version, r.path;

// Apps with no signature outside /System (user-installed or unsigned)
MATCH (k:KandjiDevice)-[r:HAS_INSTALLED]->(v:KandjiAppVersion)
WHERE coalesce(r.signature,'') = '' AND NOT r.path STARTS WITH '/System'
RETURN v.bundle_id, count(k) AS devices ORDER BY devices DESC;
```

## Notes

- Iru rebrand: the base URI and token UI may move. Nothing in cartography has
  been renamed yet (labels, docs and code still say Kandji).
