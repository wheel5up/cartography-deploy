# Cartography GitHub Lambda Loader

Syncs one or more GitHub orgs into Neo4j, one Lambda invocation per
repository. Upstream `start_github_ingestion()` syncs an org in one pass,
which for a large org takes over an hour (dependency manifests and Actions are
serial per-repo loops), far past Lambda's 900s cap. This loader runs the same
cartography functions per repo, and defers cleanup until the whole run is
known to be complete.

## Architecture

One Lambda (`src/handler.py`), three state machines (`template.yaml`):

| State machine | Does |
|---|---|
| `cartography-github-lambda-sync` | One Lambda invocation per execution. A `FAILED` result becomes a `Fail` state; `RATE_LIMITED` waits and retries (see [Rate limits](#github-rate-limits-wait-in-step-functions-not-in-lambda)) |
| `cartography-github-lambda-org-orchestrator` | One org: `org_core` → GHCR parts (serial) → repo Map (Distributed, serial, one `-sync` execution per repo/part) → `org_finalize` |
| `cartography-github-lambda-orchestrator` | Scheduled: every org in parallel → `unscoped_cleanup` |

Steps:

| Step | Scope | Does |
|---|---|---|
| `org_core` | org | Org-level data and the repo listing; loads repo nodes. No cleanup |
| `ghcr_expand` / `ghcr` | org | GHCR packages, image manifests, tags, attestations, in one or more parts |
| `repo` | repo or repo part | Dependencies, Actions, CODEOWNERS, webhooks, commits. No cleanup |
| `org_finalize` | org | Coverage check, then every org-scoped cleanup, then supply chain |
| `unscoped_cleanup` | all orgs | Upstream's global cleanup (users, repos, dependencies, languages, owners, collaborators) |

Every function called is unmodified cartography code from the pinned fork
tag. Where upstream only has an org-wide loop, the handler calls the same
per-repo functions that loop calls, in the same order, without the cleanup.
The private functions it uses are listed in the `src/handler.py` docstring;
an upstream rename fails at import.

### Cleanup safety

Cleanup deletes whatever the run didn't touch, so it only runs after a
complete run under one shared `update_tag`:

- Each repo writes its own cleanup-safety flags to its repo node in Neo4j
  (`g2_unsafe`, `g2_unsafe_tag`); nothing per-repo passes through Step
  Functions, so the Map result stays small.
- `org_finalize` refuses unless every expected `{repo, part}` ran and none
  failed. Each cleanup is also gated on its module having been readable (an
  API 403 makes that module unsafe, and its cleanup is skipped).
- `unscoped_cleanup` refuses unless every org in
  `/cartography/github-lambda/orgs` finalized successfully under this tag.
- Targeted and core-only runs never call `org_finalize`: they refresh and
  delete nothing.

## Entry points

| Send to | Input | Runs | Cleanup |
|---|---|---|---|
| orchestrator | `{}` | every org, every repo | org + global |
| org-orchestrator | `{"org": "my-org"}` | that org, every repo | org only |
| org-orchestrator | `{"org": "my-org", "repos": ["repo-a"]}` | those repos (all parts) | none |
| org-orchestrator | `{"org": "my-org", "core_only": true}` | users, teams, org data, repo metadata | none |
| org-orchestrator | `{"org": "my-org", "ghcr_only": true}` | every GHCR part, serially, one tag | GHCR only |

**Never start a single `ghcr` part by hand, especially the last one.** The
last part's cleanup deletes every GHCR node not stamped with its tag, so
running it alone under a fresh tag deletes the packages and images the
earlier parts own. `_ghcr()` refuses to run the last part unless every
earlier part's packages are already loaded under the same tag
(`_check_earlier_ghcr_parts_loaded`). Use `ghcr_only` for GHCR repairs.

`stop-execution` on a sync execution doesn't stop a Lambda invocation that's
already running; it finishes, cleanup included.

## Configuration

### GitHub credentials

| SSM parameter | Type | Holds |
|---|---|---|
| `/cartography/github/app/client-id` | String | GitHub App client ID |
| `/cartography/github/app/private-key` | SecureString | GitHub App private key |
| one per org, e.g. `/cartography/github/app/installation-id` | String | The App's installation ID on that org |
| `/cartography/github/domains-pat` | SecureString | Optional classic PAT, see below |
| `/cartography/github/commit-lookback-days` | String | Optional, default 7 |

Install the App separately on each org; each installation has its own ID and
its own rate limit.

**The optional classic PAT** (`read:org` + `read:packages`, from an org
owner) covers data a GitHub App can't read whatever its permissions: the
org's verified `domains`, and GHCR (Apps and fine-grained PATs get 403 on
`/orgs/{org}/packages`). One PAT is shared across every org. If an org
enforces SAML SSO, authorize the PAT for it (Settings → Developer settings →
the token → Configure SSO), or it gets 403 despite its scopes. If the
parameter is missing, domains and GHCR are skipped (no cleanup, no failure);
add it later without redeploying.

### Run config (SSM, editable without a redeploy)

| Parameter | Holds |
|---|---|
| `/cartography/github-lambda/orgs` | `{"<org>": "<installation-id SSM param>"}`. Seeded from `OrgsJson` |
| `/cartography/github-lambda/repo-splits` | Repos too big for one invocation. Seeded from `RepoSplitsJson` (default `{}`) |
| `/cartography/github-lambda/ghcr-splits` | Orgs with too many GHCR packages for one invocation. Optional |
| `/cartography/github-lambda/schedule` | Daily schedule, default `cron(0 4 * * ? *)` |

CloudFormation only seeds these on stack create. Edit them with
`aws ssm put-parameter --overwrite`. The schedule is resolved at deploy time,
so run `sam build && sam deploy` after changing it.

### Repo splits

Only list exceptions. Unlisted repos run every module in one invocation. A
listed repo runs one invocation per part, in order:

```json
{"my-org": {"big-repo": [
  {"modules": ["deps"], "manifests": [0, 40]},
  {"modules": ["deps"], "manifests": [40, null]},
  {"modules": ["codeowners", "webhooks", "commits"]},
  {"modules": ["actions"], "envs": "none"},
  {"modules": ["actions"], "envs": ["", "m"]},
  {"modules": ["actions"], "envs": ["m", null]}
]}}
```

- `manifests: [lo, hi)` are 0-based positions in the repo's dependency-graph
  manifest list (`null` = to the end).
- `envs: "none"` runs workflows, repo secrets/vars and environment nodes;
  `envs: [lo, hi)` runs only per-environment secrets/vars for environment
  names in that range.
- `codeowners` must share a part with an unranged `deps`, or come after every
  `deps` part.

`_validate_parts` rejects anything that would skip a module, run one twice,
or leave gaps or overlaps in ranges. Full rules are in the `Repo splits`
block of `src/handler.py`. Use `scripts/measure.py` (read-only, never touches
Neo4j) to time each repo's dependencies and Actions and pick boundaries.

### GHCR splits

`{"<org>": [[0, 50], [50, 100], [100, null]]}`: ranges of the org's container
packages sorted by name. Unlisted orgs run GHCR in one invocation. Each part
lists every package (cheap) and fetches details only for its slice. Only the
last part (ending at `null`) runs the GHCR cleanups.

- `GITHUB_GHCR_VERSION_CAP` (default 10) limits versions fetched per package,
  newest first. Without it, one CI image with thousands of builds can cost
  more than hundreds of normal packages.
- At a cap of 10, about 50 packages per part runs in 4–7 minutes.
- **Every org with a meaningful number of packages needs an entry.** An
  unlisted org runs all its packages in one invocation, and a `ghcr` timeout
  fails that org, which blocks `unscoped_cleanup` for every org. Count an
  org's packages before adding it:

  ```sh
  curl -s -H "Authorization: Bearer $PAT" \
    "https://api.github.com/orgs/<org>/packages?package_type=container&per_page=100&page=N"
  ```

**Splits absorb growth silently.** Ranges are list-slice indices, so
`[100, null]` grows as the org adds packages (or a repo adds manifests or
environments). Validation only checks that ranges are contiguous, not that
they're still small enough. Nothing warns you until a part hits 900s and
dies with `Sandbox.Timedout`. `ghcr` logs nothing per package until it
finishes, so there's no earlier signal. Re-check sizing periodically.

## GitHub rate limits: wait in Step Functions, not in Lambda

When a token's hourly budget runs out, cartography sleeps in-process until
GitHub's reset (up to about an hour), far past Lambda's 900s cap. A classic
PAT's REST budget (5,000/h) is shared by every org's GHCR parts, so this
happens on large GHCR syncs.

The handler swaps the `time` reference inside `cartography.intel.github.util`
(only that module; every rate-limit sleep there goes through it):

- A sleep that fits in the remaining invocation time, minus
  `GITHUB_RATE_LIMIT_EXIT_MARGIN_S` (default 60), runs normally.
- A longer one raises `GitHubRateLimited`, and the handler returns
  `{"status": "RATE_LIMITED", "resumeAt": "<reset + cartography's buffer>"}`.
- `cartography-github-lambda-sync` waits until `resumeAt` and re-invokes the
  same event, up to 3 times, then fails like any other step.

The re-invoked step starts over, which is safe: steps only MERGE under the
run's tag, and cleanup only deletes what the tag didn't touch. Waiting costs
nothing (Standard workflows bill per transition). A `RATE_LIMITED` result in
a sync execution's history means it happened.

## Deploy

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build --use-container   # needed on non-x86_64 machines (native wheels)
sam deploy
```

The stack creates its own security group and adds one ingress rule (Bolt
8443) to the Neo4j security group. The daily schedule is created `ENABLED`.

## Run manually

```sh
aws stepfunctions start-execution --state-machine-arn <OrchestratorArn> --input '{}'
aws stepfunctions start-execution --state-machine-arn <OrgOrchestratorArn> \
  --input '{"org": "my-org", "repos": ["repo-a"]}'
aws logs tail /cartography/github-lambda-loader --since 5m --follow
```

`scripts/run_local.py` drives the handler locally against real GitHub and
Neo4j (needs `NEO4J_URI` and `AWS_REGION`). It writes to Neo4j, and the
`full` mode cleans up (requires `--i-mean-it`).

## Tests

```sh
<cartography-venv>/bin/python -m pytest tests -q
```

Uses a venv with the pinned cartography fork installed. Covers split
validation and the rate-limit handling.

## Known gaps

### Manifests GitHub's dependency graph never returns

Dependencies come from GitHub's GraphQL `dependencyGraphManifests`, one
manifest per request. For some very large lockfiles (around 1 MB, thousands
of dependencies), GitHub's resolver times out on **every** request (HTTP 200
with `errors: [{"message": "timedout"}]`), even for `dependencies(first: 1)`.
Retrying never helps.

`_get_manifest_range()` retries once, then `_carry_forward()` re-stamps the
manifest's existing dependency data so cleanup doesn't delete it. A manifest
that has **never** been fetched has nothing to carry forward: its node exists,
with no dependencies.

```cypher
// Manifests whose dependencies weren't fetched by the latest run
MATCH (m:GitHubDependencyGraphManifest)
WHERE m.g2_last_fetched IS NULL OR m.g2_last_fetched <> m.lastupdated
OPTIONAL MATCH (m)-[hd:HAS_DEP]->()
RETURN m.repo_url AS repo, m.blob_path AS manifest,
       m.g2_last_fetched AS last_fetched,      // null = never fetched
       m.dependencies_count AS github_says, count(hd) AS in_graph
ORDER BY repo, manifest
```

`last_fetched` null with `in_graph` 0 is a permanent gap.

Possible fix: when a manifest still fails, download the file and parse it.
`cartography.intel.github.lockfiles` already parses `package-lock.json` and
`uv.lock`; `yarn.lock` and `pnpm-lock.yaml` would need new parsers. The
per-repo SBOM endpoint (`/repos/{owner}/{repo}/dependency-graph/sbom`)
works for these repos, but doesn't attribute packages to a manifest, so it
can't build `HAS_DEP`.

### Other

- **Repository custom properties aren't collected** (not in upstream
  cartography either). `GET /orgs/{org}/properties/values` returns every
  repo's values in one paged org-level call.
- **Webhooks:** without the App permission **Webhooks: Read**, every repo
  reports `webhooks` unsafe, and webhook cleanup never runs.
- **Targeted runs don't clean up** (cartography has no per-repo cleanup).
- **Upstream ordering bug:** `repos.load()` loads dependencies before
  manifests, so `HAS_DEP` isn't created on a manifest's first sync. This
  handler loads manifests first.

## Files

```
github-lambda-loader/
├── README.md
├── template.yaml            # Lambda, IAM, SG, three state machines, schedule
├── Makefile                 # trimmed deps + vendored cartography at the pinned fork tag
├── requirements.txt
├── samconfig.example.toml   # copy to samconfig.toml (gitignored)
├── scripts/
│   ├── measure.py           # read-only per-repo timing, to size splits
│   └── run_local.py         # run the handler locally
├── src/handler.py
└── tests/
```
