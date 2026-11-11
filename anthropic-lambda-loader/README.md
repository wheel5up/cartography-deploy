# Cartography Anthropic Lambda Loader

Syncs an Anthropic organization into Neo4j. One Lambda (`src/handler.py`)
calling `start_anthropic_ingestion()` unmodified, one state machine
(`cartography-anthropic-lambda-sync`, one execution is one invocation), one
daily schedule (`cartography-anthropic-lambda-daily`, 09:00 UTC, created
`ENABLED`).

## What it ingests

`cartography.intel.anthropic`, unchanged from upstream:

```
(:AnthropicOrganization)-[:RESOURCE]->(:AnthropicUser)        name, email, role, added_at
(:AnthropicOrganization)-[:RESOURCE]->(:AnthropicWorkspace)   name, created_at, archived_at
(:AnthropicUser)-[:MEMBER_OF|ADMIN_OF]->(:AnthropicWorkspace)
(:AnthropicOrganization)-[:RESOURCE]->(:AnthropicApiKey)      name, status, created_at
(:AnthropicUser)-[:OWNS]->(:AnthropicApiKey)<-[:CONTAINS]-(:AnthropicWorkspace)
```

API keys are metadata only; the Admin API never returns the secret. Calls:
`GET /v1/organizations/users`, `/workspaces` and `/api_keys` (paged), plus
`/workspaces/{id}/members` per workspace.

## Setup (SSM)

Needs an **Admin API key** (`sk-ant-admin...`), created by an org admin in the
Console (Settings → Admin keys). It's more powerful than a regular API key;
treat it as the most sensitive secret any loader here holds.

```sh
aws ssm put-parameter --type SecureString \
  --name /cartography/anthropic/admin-key --value '<sk-ant-admin...>'
```

Read at run time, so the stack can be deployed first, and a rotation takes
effect on the next run without a redeploy. No base URL or org ID is
configured: the URL is fixed, and the org ID comes from the API's
`anthropic-organization-id` response header.

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
MATCH (o:AnthropicOrganization)-[:RESOURCE]->(n)
RETURN o.id, labels(n)[0] AS kind, count(*) ORDER BY kind;
```

## Cleanup (same as upstream)

The handler calls upstream unmodified, so cleanup is upstream's: `users`,
`workspaces` and `apikeys` each clean up right after their own load. Unlike
the Kandji and Salesforce loaders, there's no guard against an empty fetch.
If the `anthropic-organization-id` header were ever missing, the org ID would
be `""` and cleanup wouldn't match existing nodes under the real org. This
loader deliberately matches upstream instead of adding a guard.

## Schedule

To pause it, set `AnthropicLambdaSchedule.State` to `DISABLED` in
`template.yaml` and redeploy. A CLI change is reverted by the next deploy.
