# cartography-deploy

AWS SAM stacks that run [Cartography](https://github.com/cartography-cncf/cartography)
on AWS Lambda and Step Functions, loading into a single Neo4j instance on EC2.
Each data source has its own loader stack with its own daily schedule.

The loaders vendor a pinned tag of the fork
[`wheel5up/cartography`](https://github.com/wheel5up/cartography) at build
time (`CARTOGRAPHY_GIT_REF` in each loader's `Makefile`).

## Stacks

Deploy in this order. Each directory has its own `README.md`, except
`gcp/`, which is described in `gcp-lambda-loader/README.md`.

| Order | Directory | What it creates |
|---|---|---|
| 1 | `network/` | Neo4j security group, ID written to SSM `/cartography/sg/neo4j` |
| 2 | `neo4j/` | EC2 instance running Neo4j 5 Community and cloudflared in Docker; private IP written to SSM `/cartography/neo4j/private-ip`. Create the SSM SecureString `/cartography/neo4j/password` first |
| 3 | `stackset/` | `cartography-read-only` role in every org account (StackSet, deployed from the management account) |
| 4 | `gcp/` | Terraform (optional, only for the GCP loader): GCP project, read-only service account, and Workload Identity Federation pool trusted by the GCP loader's Lambda role |
| 5 | `*-lambda-loader/` | One loader per data source (table below) |
| 6 | `lambda-orchestrator-schedule/` | Daily trigger for the AWS loader (02:00 UTC) |
| 7 | `dashboard/` | One CloudWatch dashboard for every loader |

Loaders, with their default schedule (UTC):

| Directory | Source | Schedule |
|---|---|---|
| `lambda-loader/` | AWS org (every account via `cartography-read-only`) | 02:00, via `lambda-orchestrator-schedule/` |
| `cloudflare-lambda-loader/` | Cloudflare | 00:00 |
| `gcp-lambda-loader/` | GCP organizations (via `gcp/`) | 01:00 |
| `okta-lambda-loader/` | Okta | 03:00 |
| `github-lambda-loader/` | GitHub | 04:00 |
| `k8s-lambda-loader/` | EKS clusters | 05:00 (disabled by default) |
| `vercel-lambda-loader/` | Vercel | 06:00 (disabled by default) |
| `snowflake-lambda-loader/` | Snowflake | 07:00 |
| `kandji-lambda-loader/` | Kandji (Iru) | 08:00 |
| `salesforce-lambda-loader/` | Salesforce | 08:30 (disabled by default) |
| `anthropic-lambda-loader/` | Anthropic | 09:00 |
| `crowdstrike-lambda-loader/` | CrowdStrike Falcon | 09:30 |
| `ontology-lambda-loader/` | Cartography ontology | 10:00 |

Schedules are seeded into SSM on first deploy; see each loader's README to
change one.

## Configuration

Deployment config is gitignored. Copy each `.example` file and fill it in:

| File | Holds |
|---|---|
| `*/samconfig.toml` | Region, VPC, subnet and stack-specific parameters |
| `lambda-loader/account-splits.json` | Per-account module splits for accounts too slow for one 900s invocation |
| `gcp-lambda-loader/default-splits.json` | GCP org and project-batch splits (pushed to SSM) |
| `gcp/terraform.tfvars`, `gcp/backend.hcl` | GCP project, org IDs, billing account, AWS account ID; Terraform state bucket |

Every stack expects the same VPC and a private subnet with outbound internet
(NAT). Credentials for each source live in SSM under `/cartography/<source>/`;
each loader's README lists them.

## Requirements

- AWS SAM CLI, Python 3.13, `make`
- An AWS Organization, with the loaders deployed in one hub account
- Terraform 1.6+ and `gcloud`, only for `gcp/`
- `sam validate --lint` in a stack directory before deploying

## Deploy a stack

```sh
cd <stack>
cp samconfig.example.toml samconfig.toml   # then edit
sam build
sam deploy
```

Preview changes with `sam deploy --no-execute-changeset`. For `neo4j/`, always
preview: changing the AMI or root volume replaces the instance and deletes the
graph (see `neo4j/README.md`).

## Neo4j access

Neo4j requires a password for the built-in `neo4j` user, stored in the SSM
SecureString `/cartography/neo4j/password` (see `neo4j/README.md` "Auth").
Every loader reads it at invocation time. Bolt (8443) and the browser (7474)
are reachable from the Neo4j security group's `ClientCidr` and through a
Cloudflare Tunnel. Loaders connect to `bolt://<neo4j-private-ip>:8443`.
