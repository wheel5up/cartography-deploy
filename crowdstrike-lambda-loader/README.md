# Cartography CrowdStrike Falcon Lambda Loader

Syncs CrowdStrike Falcon hosts and a filtered set of Spotlight
vulnerabilities into Neo4j. One Lambda (`src/handler.py`), one state machine
(`cartography-crowdstrike-lambda-sync`, one execution is one invocation), one
daily schedule (`cartography-crowdstrike-lambda-daily`, 09:30 UTC, state set
by the `ScheduleState` parameter, default `ENABLED`).

## What it ingests

```
(:CrowdstrikeTenant {id: CID})-[:RESOURCE]->(:CrowdstrikeHost)
(:CrowdstrikeHost)-[:HAS_VULNERABILITY]->(:CrowdstrikeSpotlightVulnerability)
(:CrowdstrikeSpotlightVulnerability)-[:HAS_CVE]->(:CrowdstrikeFinding:CVE)
```

- **CrowdstrikeHost**: hostname, serial_number, mac_address, local and
  external IP, platform/OS/kernel, agent_version, status,
  reduced_functionality_mode, first and last seen, tags. The ontology loader
  can link it to a `Device` by `serial_number`.
- **CrowdstrikeSpotlightVulnerability**: status, timestamps, affected app,
  remediation IDs.
- **CrowdstrikeFinding** (also `CVE`): base_score, base_severity,
  exploitability_score.

Not ingested: detections and alerts, incidents, policies, Falcon users,
Identity, Discover.

## Why the Spotlight filter

Spotlight can report millions of open vulnerabilities on a fleet of under a
thousand Macs, mostly from one browser build a few releases behind on nearly
every machine. Unfiltered, the fetch alone can take far longer than Lambda's
900s cap and adds millions of nodes.

`SpotlightFilter` is FQL that the fork ANDs onto `status:["open","reopen"]`
(`build_spotlight_filter` in `cartography/intel/crowdstrike/spotlight.py`).
The default keeps critical and high CVEs that CrowdStrike's ExPRT rating also
rates high or critical:

```
cve.severity:['CRITICAL','HIGH']+cve.exprt_rating:['HIGH','CRITICAL']
```

On one fleet of about 950 hosts this cut about 2.5 million open
vulnerabilities to about 66,000, and a full run to under a minute. An empty
string collects every open vulnerability.

Cleanup deletes what a run didn't see, so **changing the filter changes what
is kept** on the next run.

The filter needs the cartography fork's `extra_filter` support (the pinned
`CARTOGRAPHY_GIT_REF` in the `Makefile` has it). Upstream hard-codes the
open/reopen filter.

## Setup (SSM)

Create a Falcon API client with read-only scopes **Hosts: Read** and
**Vulnerabilities: Read**. Store it in SSM:

| Parameter | Type | Value |
|---|---|---|
| `/cartography/crowdstrike/client-id` | SecureString | API client ID |
| `/cartography/crowdstrike/client-secret` | SecureString | API client secret |
| `/cartography/crowdstrike/api-url` | String | Your cloud's API URL, e.g. `https://api.us-2.crowdstrike.com` |

If the client is in 1Password, `scripts/bootstrap-crowdstrike-ssm.sh` copies
it (the item's username, credential and hostname fields):

```sh
AWS_REGION=us-east-1 OP_ITEM="op://<vault>/<item>" ./scripts/bootstrap-crowdstrike-ssm.sh
```

Read at run time, so the stack can be deployed first, and a rotation needs no
redeploy.

## Deploy

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build --use-container
sam deploy
```

**Set the cleanup floors for your fleet before the first scheduled run** (see
[Cleanup guard](#cleanup-guard)). The template defaults (750 hosts, 30,000
vulnerabilities) were sized for a fleet of about 950 hosts.

## Validate

```sh
aws stepfunctions start-execution --state-machine-arn <StateMachineArn> --input '{}'
```

Input options: `{"skip_vulnerabilities": true}` (hosts only) and
`{"skip_cleanup": true}` (load without deleting). The output reports
`hostCount`, `vulnerabilityCount`, `cveCount`, per-phase seconds and
`cleanup`.

```cypher
// Active Kandji Macs with no Falcon agent (last_check_in is a string)
MATCH (k:KandjiDevice {platform: 'Mac'})
WHERE datetime(k.last_check_in) > datetime() - duration('P30D')
  AND NOT EXISTS { MATCH (c:CrowdstrikeHost {serial_number: k.serial_number}) }
RETURN k.device_name, k.serial_number, k.last_check_in;

// Hosts with the most exploitable findings
MATCH (h:CrowdstrikeHost)-[:HAS_VULNERABILITY]->(v)
RETURN h.hostname, count(v) AS n ORDER BY n DESC LIMIT 20;

// CVEs seen by both Falcon and AWS Inspector
MATCH (c:CrowdstrikeFinding) MATCH (i:AWSInspectorFinding {cve_id: c.id})
RETURN c.id, count(DISTINCT i) LIMIT 20;
```

## Cleanup guard

The handler doesn't call `start_crowdstrike_ingestion()`, which always runs
cleanup. It calls the same functions in the same order, but skips cleanup
when `hostCount < MinHostsForCleanup`, when
`vulnerabilityCount < MinVulnsForCleanup`, or when vulnerabilities or cleanup
were skipped.

Set both floors to about 80% of what a full run loads. If you tighten
`SpotlightFilter`, lower `MinVulnsForCleanup` too, or every run skips
cleanup. Set them in `samconfig.toml`:

```toml
"MinHostsForCleanup=<n>",
"MinVulnsForCleanup=<n>"
```

Changing a parameter's `Default` in `template.yaml` doesn't change a deployed
stack: CloudFormation keeps the stack's previous values on update.

## Schedule

09:30 UTC, before the ontology loader, so that day's hosts link to `Device`.
To pause it, set `"ScheduleState=DISABLED"` in `samconfig.toml` and redeploy.

## Notes

- Some Macs may report `service_provider: AWS_EC2_V2` with no account or
  instance ID. That looks like a Falcon misclassification, not real EC2.
