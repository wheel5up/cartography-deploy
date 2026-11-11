# Cartography Ontology Lambda Loader

Builds cartography's cross-provider layer on top of what the other loaders
already wrote. One Lambda (`src/handler.py`) calling
`cartography.intel.ontology.run()` unmodified, one state machine
(`cartography-ontology-lambda-sync`), one daily schedule
(`cartography-ontology-lambda-daily`, 10:00 UTC, created `ENABLED`).

No external API and no credentials: it only reads and writes Neo4j.

## What it builds

- **`User`**: one per person, created from the users source, and linked to
  every provider account that matches by email:
  `(:User)-[:HAS_ACCOUNT]->(...)` for GitHub, Anthropic, Vercel, Cloudflare,
  Snowflake, AWS and others.
- **`Device`**: one per machine, created from the devices source, and linked
  to other device records (for example `OktaDevice`) by serial number.
- DNS record, package and public IP normalization, plus cartography's
  cross-provider linking analysis jobs.

## Sources of truth

| Canonical node | Created only from | Set in `template.yaml` |
|---|---|---|
| `User` | `okta` | `ONTOLOGY_USERS_SOURCE` |
| `Device` | `kandji` | `ONTOLOGY_DEVICES_SOURCE` |

Every other provider links to existing nodes but can't create new ones, so
service accounts, bots and personal phones don't become `User` or `Device`
nodes. Change the environment variables in `template.yaml` to use different
sources (any module in cartography's `ONTOLOGY_NODES_MAPPING`), then
redeploy.

## Schedule

Runs last, because it reads that day's data from every loader. Schedule it
after your slowest loader finishes, with some margin. It isn't chained to the
other runs: if one fails or runs late, ontology uses that provider's previous
data.

To pause it, set `OntologyLambdaSchedule.State` to `DISABLED` in
`template.yaml` and redeploy. A CLI change is reverted by the next deploy.

## Cleanup (same as upstream)

Each run deletes canonical nodes that no source produced in this run. If the
users or devices source sync failed and left no nodes, the next ontology run
removes the `User` or `Device` nodes; the next good sync restores them.

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

```cypher
MATCH (u:User) RETURN count(u);
MATCH (d:Device) RETURN count(d);
MATCH (u:User)-[:HAS_ACCOUNT]->(a) RETURN labels(a)[0] AS account, count(*) ORDER BY 2 DESC;
```

Expect gaps in the `HAS_ACCOUNT` counts. Only accounts with a matching email
link: in AWS that's usually only SSO users, and Kubernetes service accounts
never link.

## TODO

- **Move the sources of truth to SSM.** Changing them now needs a redeploy.
  Read `/cartography/ontology/users-source` and
  `/cartography/ontology/devices-source` at run time instead. Because cleanup
  matches upstream, a bad value deletes or replaces every `User` or `Device`
  node on the next run, so the handler should fail the run on a value that
  isn't in `ONTOLOGY_NODES_MAPPING`, and fall back to the defaults if a
  parameter is missing.
