"""
Cartography GitHub Lambda loader — per-repository design.

Upstream `cartography.intel.github.start_github_ingestion()` syncs an org in one
pass: every sub-module loops over every repo and cleans up at the end. For
a large org that is ~65 min, far over Lambda's 900s cap (dependency manifests and
Actions are serial per-repo loops). This handler splits the same work into steps, each one Lambda invocation driven
by cartography-github-lambda-sync (one execution = one invocation):

  org_core       per org. Org-level data + the repo listing (cheap bulk calls).
                 Loads repo nodes. NO cleanup. Returns the {repo, part} items
                 for the org-orchestrator's repo Map (split config applied).
  ghcr_expand    per org, before the ghcr Map. GITHUB_GHCR_SPLITS_PARAM ->
                 part count (1 if unsplit). SSM-only, no I/O.
  ghcr           per org, after org_core, once per ghcr_expand part. GHCR:
                 packages -> image manifests -> tags -> attestations. Own step
                 (not folded into org_core): get_container_images() logs
                 nothing per-package until it's entirely done, so even alone
                 it can exceed 900s for an org with hundreds of packages
                 (Sandbox.Timedout both folded into org_core AND isolated —
                 see template.yaml).
                 GITHUB_GHCR_SPLITS_PARAM ranges packages [lo, hi), same shape
                 as repo-splits' manifest ranges (sorted by name first: REST
                 listing order is not documented stable). PAT-only
                 (_get_domains_credential); no-ops if no PAT is configured.
                 Cleanup deferred to the part whose range ends at None (the
                 Map is serial, so it runs last); earlier parts load-only —
                 same deferral repos.py's manifest-range deps parts use.
  repo           per repo, or per configured part of a split repo. Dependencies
                 (manifest ranges, skip/retry, carry-forward), Actions,
                 CODEOWNERS, repo webhooks, commits. NO cleanup. Reads its repo
                 row from Neo4j and writes its cleanup-safety flags back there.
  org_finalize   per org, only after the repo Map. Coverage check (expected
                 items vs what ran vs failures), then every org-scoped cleanup
                 gated on the org-level and Neo4j-recorded per-repo flags, then
                 supply_chain.
  unscoped_cleanup  once, after every org finalized. Upstream's global cleanup
                 (users, repos, dependencies, languages, owners, collaborators).
                 Refuses unless every org in /cartography/github-lambda/orgs has
                 a SUCCEEDED org_finalize under this update_tag.
  expand_targets targeted run: repo names -> {repo, part} items. No I/O.

Cleanup safety:
cleanup deletes whatever this run did not touch, so it may only run after a
COMPLETE run under ONE shared update_tag. org_finalize refuses unless every
expected item ran and none failed. Targeted and core-only runs never call
org_finalize; they refresh and delete nothing.

Every function called here is unmodified cartography code from the pinned fork
tag. Where upstream only exposes an org-wide loop, this handler calls the same
per-repo functions that loop calls, in the same order, minus the cleanup.
Private (underscore) functions used: repos._get_repo_dep_manifests,
repos._get_repo_collaborators_for_multiple_repos, repos._merge_repos_with_privileged_details,
repos._repos_need_privileged_details, repos._transform_dependency_manifests,
repos._transform_dependency_graph, repos._transform_requirements_txt,
repos._transform_setup_cfg_requirements. Pinned tag means an upstream rename
fails loudly at import, not silently.

Event contract (all steps):
  {"step": "expand_targets|org_core|ghcr_expand|ghcr|repo|org_finalize|unscoped_cleanup",
   "org": "<org login>",            # not needed for unscoped_cleanup
   "repo": "<repo name>", "part": n,# repo and ghcr steps only
   "repos": [...],                  # expand_targets only
   "update_tag": <int>,             # required except expand_targets: one tag per run
   "items": [...],                  # org_finalize: the {repo, part} items the Map ran
   "failed": [...],                 # org_finalize: the Map's failed "repo:part" list
   "org_safe": {...},               # org_finalize: org_core's orgSafe
   "repos_complete": bool,          # org_finalize: org_core's reposComplete
   "org_results": [...]}            # unscoped_cleanup: every org_finalize result

Per-repo cleanup-safety flags are written to the repo node in Neo4j
(g2_unsafe / g2_unsafe_tag) and read back by org_finalize, so no per-repo
results pass through Step Functions.

Response: {"status": "SUCCEEDED|FAILED", "step", "org", "repo", "updateTag",
           "durationSeconds", "error"?, ...step-specific fields}
"""

import base64
import json
import logging
import os
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from urllib.parse import quote

import boto3
import neo4j
import requests

from cartography.client.core.tx import read_list_of_dicts_tx
from cartography.intel.github import actions
from cartography.intel.github import app_installations
from cartography.intel.github import cleanup_unscoped_github_resources
from cartography.intel.github import codeowners
from cartography.intel.github import commits
from cartography.intel.github import container_image_attestations
from cartography.intel.github import container_image_tags
from cartography.intel.github import container_images
from cartography.intel.github import dependabot_alerts
from cartography.intel.github import external_identities
from cartography.intel.github import organizations
from cartography.intel.github import packages
from cartography.intel.github import personal_access_tokens
from cartography.intel.github import repos
from cartography.intel.github import supply_chain
from cartography.intel.github import teams
from cartography.intel.github import users
from cartography.intel.github import util as github_util
from cartography.intel.github import webhooks
from cartography.intel.github.app_auth import make_credential
from cartography.intel.github.label_migrations import (
    migrate_dependency_graph_manifest_label,
)
from cartography.intel.github.util import call_github_api
from cartography.intel.github.util import call_github_rest_api
from cartography.intel.github.util import get_file_content
from cartography.intel.github.util import github_org_url
from cartography.intel.github.util import rest_api_base_url
from cartography.util import run_analysis_job

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Rate-limit guard
# ---------------------------------------------------------------------------
# cartography.intel.github.util sleeps in-process until the GitHub rate limit
# resets (up to ~1h). Lambda dies at 900s, so a long sleep = Sandbox.Timedout
# and a failed run (a ghcr part has slept 26 min). Every
# sleep in that module goes through its module-level `time` reference, so we
# swap it (that module only) for one that raises when the sleep would outlive
# the invocation. handler() returns RATE_LIMITED + resumeAt, and
# cartography-github-lambda-sync Waits until resumeAt and re-invokes the step.
# Every step is safe to re-run under the same update_tag (MERGE-only, or
# cleanup that only deletes what this tag didn't touch).

# Seconds kept back to unwind, close Neo4j and return the result.
RATE_LIMIT_EXIT_MARGIN_S = int(os.environ.get("GITHUB_RATE_LIMIT_EXIT_MARGIN_S", "60"))


class GitHubRateLimited(BaseException):
    """BaseException so cartography's `except Exception` blocks can't swallow it."""

    def __init__(self, seconds: float, resume_at: datetime) -> None:
        super().__init__(f"rate-limit sleep of {seconds:.0f}s exceeds remaining Lambda time")
        self.seconds = seconds
        self.resume_at = resume_at


class _GuardedTime:
    """Stand-in for the `time` module inside cartography.intel.github.util."""

    def __init__(self) -> None:
        self.context: Any = None  # set per invocation by handler()

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)

    def sleep(self, seconds: float) -> None:
        if self.context is not None:
            left = self.context.get_remaining_time_in_millis() / 1000 - RATE_LIMIT_EXIT_MARGIN_S
            if seconds > left:
                # cartography's sleep already includes its own post-reset buffer.
                resume_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
                logger.warning(
                    "Rate-limit sleep of %.0fs exceeds the %.0fs left in this invocation; "
                    "handing back to Step Functions to wait until %s",
                    seconds, left, resume_at.isoformat(),
                )
                raise GitHubRateLimited(seconds, resume_at)
        time.sleep(seconds)


_guarded_time = _GuardedTime()
github_util.time = _guarded_time  # type: ignore[assignment]

GRAPHQL_URL = os.environ.get("GITHUB_GRAPHQL_URL", "https://api.github.com/graphql")
SSM_PREFIX = os.environ.get("GITHUB_SSM_PREFIX", "/cartography/github/app")
# Configured orgs: SSM JSON {"<org login>": "<installation-id SSM param name>"}.
# The App is installed separately on each org (own installation id, own rate
# limit). Single source of truth for "every org": the top-level orchestrator
# iterates it and unscoped_cleanup requires all of them to have finalized.
ORGS_PARAM = os.environ.get("GITHUB_ORGS_PARAM", "/cartography/github-lambda/orgs")

# Optional single classic PAT (SecureString), same credential for every org.
# The App cannot read org-owner-only data regardless of its granted
# permissions: organizations.get()'s `domains` field, and GHCR (packages.py —
# fine-grained PATs and Apps both 403 on /orgs/{org}/packages; classic PAT
# only). Missing param = both are skipped org-wide, same safe=False no-op as
# a 403 — not a hard failure.
DOMAINS_PAT_PARAM = os.environ.get(
    "GITHUB_DOMAINS_PAT_PARAM", "/cartography/github/domains-pat"
)


def _load_orgs() -> dict[str, str]:
    orgs = json.loads(_ssm_get(ORGS_PARAM))
    if not isinstance(orgs, dict) or not orgs or not all(
        isinstance(k, str) and isinstance(v, str) and v for k, v in orgs.items()
    ):
        raise ValueError(f"{ORGS_PARAM} must be a non-empty object of org -> installation-id param")
    return orgs


def _load_domains_pat() -> str | None:
    """Missing parameter = no PAT configured yet."""
    try:
        return _ssm_get(DOMAINS_PAT_PARAM, decrypt=True)
    except Exception as exc:  # noqa: BLE001
        if type(exc).__name__ == "ParameterNotFound":
            return None
        raise


COMMIT_LOOKBACK_PARAM = os.environ.get(
    "GITHUB_COMMIT_LOOKBACK_PARAM", "/cartography/github/commit-lookback-days"
)
DEFAULT_COMMIT_LOOKBACK_DAYS = 7

# Every per-repo cleanup_safe flag org_finalize aggregates. A repo result
# missing any of these is treated as unsafe for that flag.
REPO_SAFE_FLAGS = ("deps", "codeowners", "webhooks")

# Per-repo modules. An unsplit repo runs all of them in one invocation.
MODULES = ("deps", "codeowners", "actions", "webhooks", "commits")
# Which REPO_SAFE_FLAGS each module produces.
MODULE_FLAGS = {"deps": ("deps",), "codeowners": ("codeowners",), "webhooks": ("webhooks",)}

SPLITS_PARAM = os.environ.get("GITHUB_SPLITS_PARAM", "/cartography/github-lambda/repo-splits")


# ---------------------------------------------------------------------------
# Repo splits (exceptions only)
#
# SSM JSON: {"<org>": {"<repo>": [<part>, ...]}}. Repos NOT listed run as one
# invocation with every module. A listed repo runs one invocation per part, in
# list order (the repo Map is serial):
#   {"modules": [...]}                          subset of MODULES
#   {"modules": ["deps"], "manifests": [lo,hi]} ONLY dependency-graph manifests at
#                                               0-based positions [lo, hi) (hi null
#                                               = to the end). Every deps part must
#                                               be manifest-ranged if any is.
#   {"modules": ["actions"], "envs": "none"}    workflows, repo secrets/vars,
#                                               environment nodes; no per-env calls
#   {"modules": ["actions"], "envs": [lo,hi]}   ONLY per-env secrets/vars for env
#                                               names in [lo, hi) (hi null = end)
# An "actions" or "deps" part without a range does all of that module.
# "codeowners" matches rules against the repo's manifests: in the same part as
# an unranged "deps" it uses that part's manifests; otherwise it must come after
# every deps part and reads the manifests back from Neo4j.
# ---------------------------------------------------------------------------


def _load_splits() -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Missing parameter = no splits. Any other read/parse error fails loudly."""
    try:
        raw = _ssm_get(SPLITS_PARAM)
    except Exception as exc:  # noqa: BLE001
        if type(exc).__name__ == "ParameterNotFound":
            return {}
        raise
    return json.loads(raw) if raw.strip() else {}


def _check_contiguous(repo: str, what: str, ranges: list[tuple[Any, Any]], start: Any) -> None:
    """[lo, hi) ranges must start at `start`, abut exactly, and end at None."""
    ranges = sorted(ranges, key=lambda r: r[0])
    if ranges[0][0] != start:
        raise ValueError(f"{repo}: {what} ranges must start at {start!r}")
    for (_, hi), (nlo, _) in zip(ranges, ranges[1:]):
        if hi is None or hi != nlo:
            raise ValueError(f"{repo}: {what} ranges have a gap/overlap at {hi!r} -> {nlo!r}")
    if ranges[-1][1] is not None:
        raise ValueError(f"{repo}: last {what} range must end at null")


def _validate_parts(repo: str, parts: list[dict[str, Any]]) -> None:
    """
    Raise ValueError if a repo's parts would leave a module unrun, run it twice,
    leave range gaps/overlaps, or order codeowners before its manifests exist.
    Cleanup trusts this, so be strict.
    """
    if not isinstance(parts, list) or not parts:
        raise ValueError(f"{repo}: parts must be a non-empty list")

    whole = {m: 0 for m in MODULES}
    actions_core = 0
    env_ranges: list[tuple[str, str | None]] = []
    manifest_ranges: list[tuple[int, int | None]] = []
    deps_part_idxs: list[int] = []
    codeowners_idx = -1
    codeowners_with_whole_deps = False

    for i, p in enumerate(parts):
        mods = p.get("modules")
        if not mods or not isinstance(mods, list):
            raise ValueError(f"{repo} part {i}: 'modules' must be a non-empty list")
        unknown = set(mods) - set(MODULES)
        if unknown:
            raise ValueError(f"{repo} part {i}: unknown modules {sorted(unknown)}")
        if len(set(mods)) != len(mods):
            raise ValueError(f"{repo} part {i}: duplicate modules")
        envs, mrange = p.get("envs"), p.get("manifests")
        if envs is not None and mrange is not None:
            raise ValueError(f"{repo} part {i}: 'envs' and 'manifests' cannot share a part")

        if envs is not None:
            if mods != ["actions"]:
                raise ValueError(f"{repo} part {i}: 'envs' is only allowed on an actions-only part")
            if envs == "none":
                actions_core += 1
            elif (
                isinstance(envs, list) and len(envs) == 2 and isinstance(envs[0], str)
                and (envs[1] is None or isinstance(envs[1], str))
            ):
                env_ranges.append((envs[0].lower(), envs[1].lower() if envs[1] else None))
            else:
                raise ValueError(f"{repo} part {i}: 'envs' must be \"none\" or [lo, hi|null]")
            continue

        if mrange is not None:
            if mods != ["deps"]:
                raise ValueError(f"{repo} part {i}: 'manifests' is only allowed on a deps-only part")
            if not (
                isinstance(mrange, list) and len(mrange) == 2
                and isinstance(mrange[0], int) and mrange[0] >= 0
                and (mrange[1] is None or (isinstance(mrange[1], int) and mrange[1] > mrange[0]))
            ):
                raise ValueError(f"{repo} part {i}: 'manifests' must be [lo>=0, hi>lo|null]")
            manifest_ranges.append((mrange[0], mrange[1]))
            deps_part_idxs.append(i)
            continue

        for m in mods:
            whole[m] += 1
        if "deps" in mods:
            deps_part_idxs.append(i)
        if "codeowners" in mods:
            codeowners_idx = i
            codeowners_with_whole_deps = "deps" in mods

    for m in ("codeowners", "webhooks", "commits"):
        if whole[m] != 1:
            raise ValueError(f"{repo}: module '{m}' covered {whole[m]} times (must be 1)")

    # deps: exactly one whole part, XOR contiguous manifest ranges.
    if manifest_ranges:
        if whole["deps"]:
            raise ValueError(f"{repo}: deps must be EITHER one whole part OR manifest-range parts")
        _check_contiguous(repo, "manifest", manifest_ranges, 0)
    elif whole["deps"] != 1:
        raise ValueError(f"{repo}: module 'deps' covered {whole['deps']} times (must be 1)")

    # codeowners needs the repo's full manifest list.
    if not codeowners_with_whole_deps and codeowners_idx < max(deps_part_idxs):
        raise ValueError(
            f"{repo}: 'codeowners' must be in the same part as an unranged 'deps', "
            "or come after every deps part (it reads manifests from Neo4j)"
        )

    # actions: exactly one whole part, XOR one envs="none" core + env ranges.
    if whole["actions"]:
        if whole["actions"] != 1 or actions_core or env_ranges:
            raise ValueError(
                f"{repo}: actions must be EITHER one whole part OR one envs=\"none\" "
                "part plus env-range parts"
            )
        return
    if actions_core != 1 or not env_ranges:
        raise ValueError(
            f"{repo}: split actions needs exactly one envs=\"none\" part and >=1 env-range part"
        )
    _check_contiguous(repo, "env", env_ranges, "")


def _validate_splits(splits: dict[str, Any], org: str) -> dict[str, list[dict[str, Any]]]:
    org_splits = splits.get(org) or {}
    if not isinstance(org_splits, dict):
        raise ValueError(f"repo-splits[{org!r}] must be an object of repo -> parts")
    for repo, parts in org_splits.items():
        _validate_parts(repo, parts)
    return {k.lower(): v for k, v in org_splits.items()}


def _part_spec(org_splits: dict[str, list[dict[str, Any]]], repo: str, part: int) -> dict[str, Any]:
    parts = org_splits.get(repo.lower())
    if not parts:
        if part != 0:
            raise ValueError(f"{repo} is not split; part must be 0, got {part}")
        return {"modules": list(MODULES)}
    if not 0 <= part < len(parts):
        raise ValueError(f"{repo} has {len(parts)} parts; got part {part}")
    return parts[part]


def build_items(repo_names: list[str], org_splits: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """
    The repo Map's work list. Same function in org_core (to run) and
    org_finalize (to check coverage), so both agree by construction.
    """
    items: list[dict[str, Any]] = []
    for name in sorted(repo_names):
        n_parts = len(org_splits.get(name.lower()) or [None])
        items.extend({"repo": name, "part": i} for i in range(n_parts))
    return items


def _env_in_range(env_name: str, envs: Any) -> bool:
    lo, hi = envs
    n = env_name.lower()
    return n >= lo.lower() and (hi is None or n < hi.lower())


# ---------------------------------------------------------------------------
# GHCR splits (exceptions only)
#
# SSM JSON: {"<org>": [[lo, hi], [lo, hi], ...]}. Orgs NOT listed run GHCR as
# one invocation (today's behavior). A listed org runs one invocation per
# part, in list order (the ghcr Map is serial). [lo, hi) are 0-based indices
# into the org's packages sorted by name (packages.get_container_packages()
# order is not documented as stable; sorting ourselves removes that
# dependency). hi null = to the end. Ranges must be contiguous, start at 0,
# and the last must end at null — see _check_contiguous (shared with
# repo-splits' manifest/env ranges).
#
# Each part fetches the FULL package list (packages.get_container_packages()
# is the cheap, fast call — under a second for hundreds of packages; it is NOT what times
# out) then slices it before the per-package manifest/config/attestation
# fetches. Only the part covering the END of the range (hi=None) runs the
# GitHubPackage/GitHubContainerImage(Layer)/Tag/Attestation cleanups; earlier
# parts load-only, same convention as deps' manifest-range parts skipping
# manifest cleanup until the whole repo's run is known-complete. Mid-run
# (between parts, same tag) the graph is incomplete; it is only correct after
# every part has run.
#
# RISK — splits silently absorb package growth, they do not alert on it.
# [lo, hi) are plain list-slice indices, not a count the config pins: a range
# like [300, null] means "index 300 to wherever the list ends now", so if the
# org gains packages, that part's real workload grows silently — no
# validation here compares a range's actual size against what it was tuned
# for (_validate_ghcr_ranges only checks contiguity/start/end, not size), and
# nothing surfaces the growth until that part happens to blow 900s and dies
# with Sandbox.Timedout. Re-check sizing periodically, not just when it breaks.
# ---------------------------------------------------------------------------

GHCR_SPLITS_PARAM = os.environ.get(
    "GITHUB_GHCR_SPLITS_PARAM", "/cartography/github-lambda/ghcr-splits"
)

# Cap on versions (= distinct image digests) fetched PER PACKAGE, newest
# first (the versions REST endpoint is sorted newest-first by default,
# verified against a real package — no query param for this; we just stop
# paginating early). Without this, package count is a meaningless proxy for
# fetch cost: on one org, ~330 packages carried ~37,000 versions, but the
# median package had 15 while one CI image package alone had ~8,000 (one CI
# build image per commit, numeric build-id tags, no pruning). A
# package-count split cannot bound per-invocation work when one package can
# be 500x the median; this bounds it, uniformly, regardless of split size.
# At ~0.4s/manifest, uncapped that org was ~4 hours of total
# fetch time; capped at 10 it drops to ~2,500 versions (~93% reduction) and
# fits a modest, package-count-only split comfortably within Lambda's 900s.
# This is also a correctness improvement, not just a perf one: the 106th
# most recent build of a CI image is realistically never referenced by
# anything live, so capping is "clean up the cruft we were never using,"
# not a lossy workaround.
GHCR_VERSION_CAP = int(os.environ.get("GITHUB_GHCR_VERSION_CAP", "10"))


def _load_ghcr_splits() -> dict[str, list[list[Any]]]:
    """Missing parameter = no org has GHCR split; it runs as one invocation."""
    try:
        raw = _ssm_get(GHCR_SPLITS_PARAM)
    except Exception as exc:  # noqa: BLE001
        if type(exc).__name__ == "ParameterNotFound":
            return {}
        raise
    return json.loads(raw) if raw.strip() else {}


def _validate_ghcr_ranges(org: str, ranges: list[list[Any]]) -> list[tuple[int, int | None]]:
    if not isinstance(ranges, list) or not ranges:
        raise ValueError(f"ghcr-splits[{org!r}] must be a non-empty list of [lo, hi] ranges")
    parsed: list[tuple[int, int | None]] = []
    for i, r in enumerate(ranges):
        if not (
            isinstance(r, list) and len(r) == 2
            and isinstance(r[0], int) and r[0] >= 0
            and (r[1] is None or (isinstance(r[1], int) and r[1] > r[0]))
        ):
            raise ValueError(f"ghcr-splits[{org!r}][{i}] must be [lo>=0, hi>lo|null]")
        parsed.append((r[0], r[1]))
    _check_contiguous(org, "ghcr package", parsed, 0)
    return parsed


def _ghcr_range_for_part(org: str, part: int) -> tuple[int, int | None]:
    """part=0 and no split config -> (0, None), i.e. every package."""
    ranges = _load_ghcr_splits().get(org)
    if not ranges:
        if part != 0:
            raise ValueError(f"{org} GHCR is not split; part must be 0, got {part}")
        return (0, None)
    parsed = _validate_ghcr_ranges(org, ranges)
    parsed.sort(key=lambda r: r[0])
    if not 0 <= part < len(parsed):
        raise ValueError(f"{org} GHCR has {len(parsed)} parts; got part {part}")
    return parsed[part]


def _ghcr_part_count(org: str) -> int:
    ranges = _load_ghcr_splits().get(org)
    if not ranges:
        return 1
    return len(_validate_ghcr_ranges(org, ranges))


# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------


def _ssm_get(name: str, decrypt: bool = False) -> str:
    resp = boto3.client("ssm").get_parameter(Name=name, WithDecryption=decrypt)
    return resp["Parameter"]["Value"]


def _get_credential(org: str) -> Any:
    """GitHub App credential for one org. Fetched from SSM at invoke time —
    never via Lambda env vars, which are plaintext to lambda:GetFunctionConfiguration."""
    orgs = _load_orgs()
    if org not in orgs:
        raise ValueError(f"Unknown org {org!r}; configured: {sorted(orgs)}")
    return make_credential(
        {
            "client_id": _ssm_get(f"{SSM_PREFIX}/client-id"),
            "private_key": _ssm_get(f"{SSM_PREFIX}/private-key", decrypt=True),
            "installation_id": _ssm_get(orgs[org]),
            "url": GRAPHQL_URL,
            "name": org,
        }
    )


def _get_domains_credential(org: str) -> Any | None:
    """
    Classic PAT credential (org-owner scoped, `read:org` + `read:packages`) for
    org-owner-only data the App can never read: organizations.get()'s `domains`
    field, and GHCR (packages.py — fine-grained PATs and Apps both 403 on
    /orgs/{org}/packages; classic PAT only). None if no PAT is configured, so
    callers fall back to the App token (same no-op as a 403).
    """
    pat = _load_domains_pat()
    if pat is None:
        return None
    return make_credential({"token": pat, "url": GRAPHQL_URL, "name": org})


def _commit_lookback_days() -> int:
    try:
        return int(_ssm_get(COMMIT_LOOKBACK_PARAM))
    except Exception:  # noqa: BLE001
        logger.warning(
            "Could not read %s; defaulting commit lookback to %d days",
            COMMIT_LOOKBACK_PARAM,
            DEFAULT_COMMIT_LOOKBACK_DAYS,
        )
        return DEFAULT_COMMIT_LOOKBACK_DAYS


def _get_neo4j_password() -> str:
    """Neo4j password from SSM at invoke time, never a Lambda env var.
    NEO4J_PASSWORD_SSM_PARAM is the parameter NAME only."""
    response = boto3.client("ssm").get_parameter(
        Name=os.environ.get(
            "NEO4J_PASSWORD_SSM_PARAM", "/cartography/neo4j/password"
        ),
        WithDecryption=True,
    )
    return response["Parameter"]["Value"]


def _get_neo4j_driver() -> neo4j.Driver:
    return neo4j.GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=("neo4j", _get_neo4j_password()),
        max_connection_lifetime=int(os.environ.get("NEO4J_MAX_CONNECTION_LIFETIME", "200")),
    )


def _common(update_tag: int) -> dict[str, Any]:
    return {"UPDATE_TAG": update_tag}


# ---------------------------------------------------------------------------
# org_core
# ---------------------------------------------------------------------------


def _org_core(session: neo4j.Session, token: Any, org: str, update_tag: int) -> dict[str, Any]:
    """
    Mirrors start_github_ingestion()'s org-level calls plus the non-dependency
    half of repos.sync(), with every cleanup removed (moved to org_finalize).

    Upstream calls that are reproduced here WITHOUT their trailing cleanup:
      organizations.sync      -> cleanup_domains
      external_identities.sync-> GraphJob(GitHubExternalIdentitySchema)
      personal_access_tokens.sync -> cleanup_personal_access_tokens
      app_installations.sync  -> app_installations.cleanup
      dependabot_alerts.sync  -> dependabot_alerts.cleanup
      teams.sync_github_teams -> teams.cleanup
      webhooks.sync (org hooks only here; repo hooks in repo step) -> webhooks.cleanup
      actions.sync (org secrets/variables only here) -> cleanup_org_level
      repos.sync (non-dependency half) -> cleanup_github_branches,
                                          cleanup_branch_protection_rules, cleanup_rulesets
    Returns cleanup_safe flags for org-level data so org_finalize can gate on them.
    """
    common = _common(update_tag)
    org_url = github_org_url(GRAPHQL_URL, org)
    safe: dict[str, bool] = {}
    # Validate the split config first: a bad config fails before any work.
    org_splits = _validate_splits(_load_splits(), org)

    # Classic PAT for org-owner-only data the App can never read (domains,
    # GHCR). None if this org has no PAT configured; callers then fall back to
    # the App token (same no-op as a 403 in that code path).
    domains_token = _get_domains_credential(org)

    # --- organization + users (start_github_ingestion order) ---
    user_data, owners_data, org_data = users.get_organization(token, GRAPHQL_URL, org)
    settings = organizations.get(domains_token or token, GRAPHQL_URL, org)
    organizations.load_organization(
        session, organizations.transform_organization(org_data, settings), update_tag
    )
    if settings["domains"] is not None:
        organizations.load_domains(
            session, organizations.transform_domains(settings["domains"]), org_url, update_tag
        )
        safe["domains"] = True
    else:
        safe["domains"] = False
    users.sync(session, common, user_data, owners_data, org_data)

    # --- external identities (sync() minus its GraphJob cleanup) ---
    safe["external_identities"] = False
    try:
        snapshot = external_identities.get_external_identities(token, GRAPHQL_URL, org)
    except Exception:  # noqa: BLE001 — upstream skips on RequestException/SnapshotError
        logger.warning("Skipping external identities for %s", org, exc_info=True)
        snapshot = None
    if snapshot is not None:
        identities, ei_org_url = snapshot
        external_identities.load(
            session,
            external_identities.GitHubExternalIdentitySchema(),
            external_identities.transform_external_identities(identities, ei_org_url),
            lastupdated=update_tag,
            org_url=ei_org_url,
        )
        safe["external_identities"] = True

    # --- repos: non-dependency half of repos.sync() ---
    repos_json = repos.get(token, GRAPHQL_URL, org)
    repos_complete = all(r is not None for r in repos_json)

    privileged: dict[str, dict[str, Any]] = {}
    safe["rulesets"] = True
    if repos._repos_need_privileged_details(repos_json):
        try:
            privileged = repos.get_repo_privileged_details_by_url(token, GRAPHQL_URL, org)
        except Exception:  # noqa: BLE001 — upstream: RequestException/ValueError
            safe["rulesets"] = False
            logger.warning("Privileged repo details failed for %s", org, exc_info=True)
    repos_json, _, _ = repos._merge_repos_with_privileged_details(repos_json, privileged)

    direct: dict[str, Any] = {}
    outside: dict[str, Any] = {}
    try:
        direct = repos._get_repo_collaborators_for_multiple_repos(
            repos_json, "DIRECT", org, GRAPHQL_URL, token
        )
        outside = repos._get_repo_collaborators_for_multiple_repos(
            repos_json, "OUTSIDE", org, GRAPHQL_URL, token
        )
    except TypeError:
        logger.warning("Unable to list repo collaborators for %s", org, exc_info=True)

    sec = repos.get_repo_security_and_analysis_by_url(token, GRAPHQL_URL, org)
    for r in repos_json:
        if r is not None and r.get("url") in sec:
            r["securityAndAnalysis"] = sec[r["url"]]

    # Strip dependency-graph inputs so transform() produces repo data only;
    # dependencies are produced per repo. requirements/setupCfg are dropped too
    # so python_requirements come solely from the repo step (which decides the
    # fallback once it knows whether the repo has a dependency graph).
    for r in repos_json:
        if r is not None:
            r.pop("dependencyGraphManifests", None)
            r["requirements"] = None
            r["setupCfg"] = None
    repo_data = repos.transform(repos_json, direct, outside)

    owner_org_id = next(
        (r["owner_org_id"] for r in repo_data["repos"] if r.get("owner_org_id")),
        org_url,
    )
    migrate_dependency_graph_manifest_label(session, owner_org_id)
    repos.load(session, common, repo_data)
    migration_params = {**common, "owner_org_id": owner_org_id}
    run_analysis_job(
        "github_branch_protection_rule_resource_edge_migration.json", session, migration_params
    )
    run_analysis_job(
        "github_dependency_manifest_resource_edge_migration.json", session, migration_params
    )

    # --- remaining org-level modules (start_github_ingestion order) ---
    pats = personal_access_tokens.get(token, GRAPHQL_URL, org)
    personal_access_tokens.load_personal_access_tokens(session, pats, org_url, update_tag)
    safe["pats"] = True

    installs = app_installations.get(token, GRAPHQL_URL, org)
    safe["app_installations"] = installs is not None
    if installs is not None:
        app_installations.load_installations(
            session, app_installations.transform(installs, org_url), org_url, update_tag
        )

    org_hooks = webhooks.get_organization_webhooks(token, GRAPHQL_URL, org)
    safe["org_webhooks"] = org_hooks is not None and repos_complete
    webhooks.load_webhooks(
        session, webhooks.transform_webhooks(org_hooks or [], {}, org_url), org_url, update_tag
    )

    dep = dependabot_alerts.get(token, GRAPHQL_URL, org)
    safe["dependabot"] = dep.cleanup_safe
    if dep.cleanup_safe:
        t = dependabot_alerts.transform(dep.alerts)
        dependabot_alerts.load_users(session, t["users"], update_tag)
        dependabot_alerts.load_alerts(session, t["alerts"], update_tag, org_url)

    _sync_teams_no_cleanup(session, token, org, update_tag)

    org_secrets = actions.get_org_secrets(token, GRAPHQL_URL, org)
    if org_secrets:
        actions.load_org_secrets(
            session, actions.transform_org_secrets(org_secrets, org), update_tag, org_url
        )
    org_vars = actions.get_org_variables(token, GRAPHQL_URL, org)
    if org_vars:
        actions.load_org_variables(
            session, actions.transform_org_variables(org_vars, org), update_tag, org_url
        )

    repo_names = sorted(r["name"] for r in repo_data["repos"])
    items = build_items(repo_names, org_splits)
    return {
        "items": items,
        "repoCount": len(repo_names),
        "reposComplete": repos_complete,
        "orgSafe": safe,
    }


def _sync_teams_no_cleanup(session: neo4j.Session, token: Any, org: str, update_tag: int) -> None:
    """teams.sync_github_teams() minus its trailing teams.cleanup()."""
    teams_paginated, org_data = teams.get_teams(org, GRAPHQL_URL, token)
    nodes = teams_paginated.nodes
    processed = teams.transform_teams(
        teams_paginated,
        org_data,
        teams._get_team_repos_for_multiple_teams(nodes, org, GRAPHQL_URL, token),
        teams._get_team_users_for_multiple_teams(nodes, org, GRAPHQL_URL, token),
        teams._get_child_teams_for_multiple_teams(nodes, org, GRAPHQL_URL, token),
    )
    teams.load_team_repos(session, processed, update_tag, org_data["url"])


# ---------------------------------------------------------------------------
# ghcr
# ---------------------------------------------------------------------------


def _ghcr(session: neo4j.Session, org: str, update_tag: int, part: int = 0) -> dict[str, Any]:
    """
    GHCR: packages -> image manifests -> tags -> attestations. Its own step,
    not folded into org_core: get_container_images() fetches every package
    version's manifest+config one at a time with NO per-package progress log
    (a single summary line at the end), so even alone it can exceed 900s for
    an org with hundreds of packages (Sandbox.Timedout with zero log
    output, both folded into org_core AND isolated into its own
    invocation). GITHUB_GHCR_SPLITS_PARAM splits it the
    same way repo-splits ranges dependency manifests: [lo, hi) 0-based
    indices into the org's packages, SORTED BY NAME (the REST listing order
    is not documented as stable, so we impose our own before slicing).

    cleanup_safe means "packages.sync_packages() returned the full list and
    every part's load succeeded". Real cleanup (packages.cleanup_packages()
    and container_images/_tags/_attestations' cleanup_*) only runs on the
    part whose range ends at None (the Map runs serially in range order, so
    that part runs last and by then every other part under this tag has
    loaded); earlier parts load-only — mirrors deps' manifest-range parts
    deferring manifest cleanup. Mid-run (between parts, same tag) the graph
    is incomplete; only correct once every part has run.

    Classic-PAT only (App and fine-grained PATs both 403 on
    /orgs/{org}/packages); no-ops cleanly if no domains_token is configured.
    """
    common = _common(update_tag)
    org_url = f"https://github.com/{org}"
    domains_token = _get_domains_credential(org)
    safe = {"ghcr": False}
    if domains_token is None:
        return {"orgSafe": safe}

    lo, hi = _ghcr_range_for_part(org, part)
    is_last_part = hi is None

    fetch = packages.get_container_packages(domains_token, GRAPHQL_URL, org)
    if not fetch.cleanup_safe:
        return {"orgSafe": safe}
    all_packages = packages.transform_packages(fetch.packages, org)
    all_packages.sort(key=lambda p: p["name"])
    package_slice = all_packages[lo:hi]

    if is_last_part:
        # Before loading anything: the cleanup below deletes every GHCR node
        # not stamped with this tag, so it is only correct if parts 0..N-1
        # already ran under this same tag.
        _check_earlier_ghcr_parts_loaded(
            session, org, org_url, part, [p["html_url"] for p in all_packages[:lo]], update_tag
        )

    packages.load_packages(session, package_slice, org_url, update_tag)
    if is_last_part:
        packages.cleanup_packages(session, {**common, "org_url": org_url})

    images, _lists, tag_rows, observed_and_skipped = _ghcr_images_load_only(
        session, domains_token, org, package_slice, update_tag, is_last_part, common
    )
    container_image_tags.load_container_image_tags(session, tag_rows, org_url, update_tag)
    if is_last_part:
        container_image_tags.cleanup_container_image_tags(
            session, {**common, "org_url": org_url}
        )

    _ghcr_attestations_load_only(
        session, domains_token, org, images, observed_and_skipped, update_tag,
        is_last_part, common,
    )

    safe["ghcr"] = True
    return {"orgSafe": safe}


def _check_earlier_ghcr_parts_loaded(
    session: neo4j.Session,
    org: str,
    org_url: str,
    part: int,
    expected_ids: list[str],
    update_tag: int,
) -> None:
    """
    Guard for the cleanup (last) ghcr part. Refuses unless every package in
    the earlier parts' ranges is already in the graph under this update_tag.

    Without it, running the last part alone (or under a fresh tag, or before
    earlier parts finished) deletes every other part's packages, images, tags,
    layers and attestations. This has happened: an ad hoc last-part run
    deleted every earlier part's packages and images until p0..pN were
    re-run.

    Also refuses if the package list shifted mid-run (a package created or
    deleted between parts), since some package then never ran under this tag.
    Unsplit orgs (single part, expected_ids empty) pass trivially.
    """
    if not expected_ids:
        return
    rows = session.execute_read(
        read_list_of_dicts_tx,
        "MATCH (:GitHubOrganization {id: $org_url})-[:RESOURCE]->(p:GitHubPackage) "
        "WHERE p.lastupdated = $update_tag AND p.id IN $ids RETURN p.id AS id",
        org_url=org_url,
        update_tag=update_tag,
        ids=expected_ids,
    )
    missing = sorted(set(expected_ids) - {r["id"] for r in rows})
    if missing:
        raise RuntimeError(
            f"ghcr part {part} for {org} is the cleanup part, but {len(missing)} of "
            f"{len(expected_ids)} package(s) from parts 0..{part - 1} are not loaded under "
            f"update_tag {update_tag}; refusing to load or clean up. Run every ghcr part in "
            f"order under one tag (org-orchestrator {{\"org\": ..., \"ghcr_only\": true}}). "
            f"First missing: {missing[:5]}"
        )


def _get_package_versions_capped(token: Any, org: str, package_name: str) -> list[dict[str, Any]]:
    """
    The GHCR_VERSION_CAP most recent versions of one package. One request
    (per_page=cap, page 1 — the endpoint is already newest-first, so no
    further pagination is needed, unlike packages.get_package_versions()
    which fetches every page). 404 (package has no versions) -> [].
    """
    base_url = rest_api_base_url(GRAPHQL_URL)
    endpoint = (
        f"/orgs/{quote(org)}/packages/container/{quote(package_name, safe='')}/versions"
        f"?per_page={GHCR_VERSION_CAP}&page=1"
    )
    try:
        result = call_github_rest_api(endpoint, token, base_url)
    except requests.exceptions.HTTPError as err:
        if err.response is not None and err.response.status_code == 404:
            return []
        raise
    return result if isinstance(result, list) else []


def _get_container_images_capped(
    token: Any,
    org: str,
    packages: list[dict[str, Any]],
    skip_digests: set[str] | None = None,
) -> tuple[
    list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]],
    set[str], list[dict[str, str]],
]:
    """
    container_images.get_container_images() with one change: per-package
    versions come from _get_package_versions_capped (newest GHCR_VERSION_CAP)
    instead of packages.get_package_versions (every version ever pushed —
    see GHCR_VERSION_CAP's comment for why that is unbounded and this
    isn't). Everything else — manifest-list expansion via
    container_images._process_manifest, cross-run dedup via skip_digests,
    tag-row shape, return shape — is identical to upstream so the downstream
    transform/load calls need no changes.
    """
    skip_digests = skip_digests or set()
    all_manifests: list[dict[str, Any]] = []
    manifest_lists: list[dict[str, Any]] = []
    tag_rows: list[dict[str, Any]] = []
    config_cache: dict[str, dict[str, Any] | None] = {}
    observed_and_skipped: set[str] = set()
    skipped_package_image_links: set[tuple[str, str]] = set()

    for pkg in packages:
        package_name = pkg["name"]
        package_id = pkg["html_url"]
        package_uri = pkg["uri"]
        repository_name = container_images._ghcr_repository_name(org, package_name)
        seen_digests: set[str] = set()

        versions = _get_package_versions_capped(token, org, package_name)
        for version in versions:
            digest = version.get("name")
            if not digest or not digest.startswith("sha256:"):
                continue
            tag_names = (version.get("metadata", {}).get("container", {}).get("tags") or [])
            updated_at = version.get("updated_at")
            for tag_name in tag_names:
                tag_rows.append(
                    {
                        "name": tag_name,
                        "uri": f"{package_uri}:{tag_name}",
                        "digest": digest,
                        "image_pushed_at": updated_at,
                        "package_id": package_id,
                    }
                )

            if digest in skip_digests:
                observed_and_skipped.add(digest)
                skipped_package_image_links.add((package_id, digest))
                continue

            manifest = container_images._process_manifest(
                token, repository_name, digest, package_id, package_uri,
                config_cache, seen_digests, all_manifests,
            )
            if manifest is None:
                continue

            media_type = manifest.get("mediaType")
            if media_type in container_images.MANIFEST_LIST_MEDIA_TYPES:
                manifest_lists.append(manifest)
                for child in manifest.get("manifests", []) or []:
                    annotations = child.get("annotations") or {}
                    if annotations.get("vnd.docker.reference.type") == "attestation-manifest":
                        continue
                    child_digest = child.get("digest")
                    if not child_digest:
                        continue
                    if child_digest in skip_digests:
                        observed_and_skipped.add(child_digest)
                        skipped_package_image_links.add((package_id, child_digest))
                        continue
                    container_images._process_manifest(
                        token, repository_name, child_digest, package_id, package_uri,
                        config_cache, seen_digests, all_manifests,
                    )

    logger.info(
        "Fetched %d unique GHCR manifests (%d manifest lists, %d tags) across "
        "%d packages (capped at %d versions/package)",
        len(all_manifests), len(manifest_lists), len(tag_rows), len(packages),
        GHCR_VERSION_CAP,
    )
    return (
        all_manifests, manifest_lists, tag_rows, observed_and_skipped,
        [
            {"package_id": package_id, "digest": digest}
            for package_id, digest in sorted(skipped_package_image_links)
        ],
    )


def _ghcr_images_load_only(
    session: neo4j.Session,
    token: Any,
    org: str,
    package_slice: list[dict[str, Any]],
    update_tag: int,
    is_last_part: bool,
    common: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    """container_images.sync_container_images() minus its unconditional
    cleanup — same structure, load calls only, cleanup gated on is_last_part.
    Uses _get_container_images_capped (version-capped) instead of upstream's
    get_container_images (unbounded) — see that function's docstring."""
    org_url = f"https://github.com/{org}"
    skip_digests = container_images.get_complete_layer_digests(
        session, container_images.GHCR_LAYER_GRAPH, None, {"id": org_url}
    )
    (
        raw_manifests, manifest_lists, tag_rows, observed_and_skipped,
        skipped_package_image_links,
    ) = _get_container_images_capped(
        token, org, package_slice, skip_digests=skip_digests
    )
    images = container_images.transform_container_images(raw_manifests)
    layers = container_images.transform_container_image_layers(raw_manifests)
    if layers:
        container_images.load_container_image_layers(session, layers, org_url, update_tag)
    if images:
        container_images.load_container_images(session, images, org_url, update_tag)
    container_images.refresh_layer_closures(
        session, container_images.GHCR_LAYER_GRAPH, observed_and_skipped,
        {"id": org_url}, update_tag,
    )
    container_images.refresh_skipped_package_image_links(
        session, org_url, skipped_package_image_links, update_tag
    )
    if is_last_part:
        cleanup_params = {**common, "org_url": org_url}
        container_images.cleanup_container_image_layers(session, cleanup_params)
        container_images.cleanup_container_images(session, cleanup_params)
    return raw_manifests, manifest_lists, tag_rows, observed_and_skipped


def _ghcr_attestations_load_only(
    session: neo4j.Session,
    token: Any,
    org: str,
    raw_manifests: list[dict[str, Any]],
    additional_observed_digests: set[str],
    update_tag: int,
    is_last_part: bool,
    common: dict[str, Any],
) -> None:
    """container_image_attestations.sync_container_image_attestations() minus
    its unconditional cleanup."""
    org_url = f"https://github.com/{org}"
    digests = list(container_image_attestations._digests_from_manifests(raw_manifests))
    seen = set(digests)
    for d in additional_observed_digests:
        if d not in seen:
            digests.append(d)
            seen.add(d)
    skip = container_image_attestations._digests_already_enriched(session, org_url)

    attestations_by_digest: dict[str, list[dict[str, Any]]] = {}
    observed_and_skipped: set[str] = set()
    for digest in digests:
        if digest in skip:
            observed_and_skipped.add(digest)
            continue
        fetched = container_image_attestations.get_attestations_for_digest(
            token, GRAPHQL_URL, org, digest
        )
        if fetched:
            attestations_by_digest[digest] = fetched

    attestation_rows, provenance_rows = container_image_attestations.transform_attestations(
        org, attestations_by_digest
    )
    if attestation_rows:
        container_image_attestations.load_attestations(
            session, attestation_rows, org_url, update_tag
        )
    if provenance_rows:
        container_image_attestations.load_image_provenance(
            session, provenance_rows, org_url, update_tag
        )
    container_image_attestations._refresh_skipped_attestation_lastupdated(
        session, observed_and_skipped, org_url, update_tag
    )
    if is_last_part:
        container_image_attestations.cleanup_attestations(
            session, {**common, "org_url": org_url}
        )


# ---------------------------------------------------------------------------
# repo
# ---------------------------------------------------------------------------

# Per-repo cleanup-safety flags, kept on the repo node for org_finalize (so no
# per-repo results travel through Step Functions). Custom g2_ properties, only
# meaningful together: g2_unsafe applies to the run identified by g2_unsafe_tag.
# Parts of a split repo run in sequence under one tag, so flags are MERGED for
# the same tag and REPLACED for a newer tag. A repo with no flags for this tag
# was cleanup-safe for every module it ran.
_MARK_UNSAFE_QUERY = """
MATCH (r:GitHubRepository {id: $repo_url})
SET r.g2_unsafe = CASE
        WHEN r.g2_unsafe_tag = $update_tag
        THEN [f IN coalesce(r.g2_unsafe, []) WHERE NOT f IN $flags] + $flags
        ELSE $flags
    END,
    r.g2_unsafe_tag = $update_tag
"""

_ORG_UNSAFE_QUERY = """
MATCH (:GitHubOrganization {id: $org_url})<-[:OWNER]-(r:GitHubRepository)
WHERE r.g2_unsafe_tag = $update_tag
UNWIND r.g2_unsafe AS flag
RETURN DISTINCT flag
"""

# Manifest-ranged parts record the manifest count they listed. Same merge rule
# as g2_unsafe. finalize flags any repo whose parts saw different counts.
_MARK_MANIFEST_TOTAL_QUERY = """
MATCH (r:GitHubRepository {id: $repo_url})
SET r.g2_manifest_totals = CASE
        WHEN r.g2_manifest_totals_tag = $update_tag
        THEN [t IN coalesce(r.g2_manifest_totals, []) WHERE t <> $total] + [$total]
        ELSE [$total]
    END,
    r.g2_manifest_totals_tag = $update_tag
"""

_ORG_MANIFEST_TOTALS_QUERY = """
MATCH (:GitHubOrganization {id: $org_url})<-[:OWNER]-(r:GitHubRepository)
WHERE r.g2_manifest_totals_tag = $update_tag AND size(r.g2_manifest_totals) > 1
RETURN r.name AS repo, r.g2_manifest_totals AS totals
"""

_REPO_ROW_QUERY = """
MATCH (org:GitHubOrganization {id: $org_url})<-[:OWNER]-(r:GitHubRepository {name: $name})
RETURN r.id AS url, r.name AS name, r.fullname AS fullname,
       r.defaultbranch AS defaultbranch, r.owner_org_id AS owner_org_id
"""

_OWNER_TARGETS_QUERY = """
MATCH (org:GitHubOrganization {id: $org_url})
OPTIONAL MATCH (org)<-[:MEMBER_OF|UNAFFILIATED]-(u:GitHubUser)
WITH org, collect(DISTINCT {login: u.login, url: u.url}) AS users
OPTIONAL MATCH (org)-[:RESOURCE]->(t:GitHubTeam)
RETURN users, collect(DISTINCT {name: t.name, url: t.url, org_login: org.username}) AS teams
"""


def _read_repo_row(session: neo4j.Session, org_url: str, name: str) -> dict[str, Any]:
    rows = session.execute_read(read_list_of_dicts_tx, _REPO_ROW_QUERY, org_url=org_url, name=name)
    if not rows:
        raise LookupError(
            f"Repo {name!r} not found under {org_url} in Neo4j. org_core must run "
            "(with this update_tag) before repo steps."
        )
    return rows[0]


def _repo(
    session: neo4j.Session,
    token: Any,
    org: str,
    name: str,
    part: int,
    update_tag: int,
) -> dict[str, Any]:
    """
    One repo (or one configured part of a split repo). Per module, mirrors:
    repos.sync (dependencies only), actions.sync (repo loop body),
    codeowners.sync (per-repo body), webhooks.get (repo hooks),
    commits.process_repo_commits_batch (per-repo body). Loads only; no cleanup.
    Returns cleanup_safe flags ONLY for modules this part ran.
    """
    org_splits = _validate_splits(_load_splits(), org)
    spec = _part_spec(org_splits, name, part)
    mods = set(spec["modules"])

    org_url = github_org_url(GRAPHQL_URL, org)
    row = _read_repo_row(session, org_url, name)
    repo_url = row["url"]
    default_branch = row["defaultbranch"]
    owner_org_id = row["owner_org_id"] or org_url
    safe: dict[str, bool] = {}
    timings: dict[str, float] = {}

    manifests: list[dict[str, Any]] | None = None
    deps_info: dict[str, Any] = {}
    if "deps" in mods:
        t = time.monotonic()
        manifests, safe["deps"], deps_info = _repo_deps(
            session, token, org, name, row, update_tag, spec.get("manifests")
        )
        timings["deps"] = round(time.monotonic() - t, 1)
        if spec.get("manifests") is not None:
            manifests = None  # a range is not the repo's full list
            _write(
                session, _MARK_MANIFEST_TOTAL_QUERY,
                repo_url=repo_url, total=deps_info["manifestTotal"], update_tag=update_tag,
            )

    if "actions" in mods:
        t = time.monotonic()
        _repo_actions(session, token, org, name, org_url, update_tag, spec.get("envs"))
        timings["actions"] = round(time.monotonic() - t, 1)

    if "codeowners" in mods:
        # Same part as an unranged deps -> `manifests` is the full list.
        # Otherwise validation guarantees every deps part ran earlier in this
        # run, and _repo_codeowners reads them from Neo4j (manifests=None).
        t = time.monotonic()
        safe["codeowners"] = _repo_codeowners(
            session, token, name, repo_url, default_branch, owner_org_id, org_url, manifests, update_tag
        )
        timings["codeowners"] = round(time.monotonic() - t, 1)

    if "webhooks" in mods:
        t = time.monotonic()
        hooks = webhooks.get_repository_webhooks(token, GRAPHQL_URL, row["fullname"])
        safe["webhooks"] = hooks is not None
        if hooks is not None:
            webhooks.load_webhooks(
                session, webhooks.transform_webhooks([], {repo_url: hooks}, org_url), org_url, update_tag
            )
        timings["webhooks"] = round(time.monotonic() - t, 1)

    if "commits" in mods:
        t = time.monotonic()
        since = datetime.now(timezone.utc) - timedelta(days=_commit_lookback_days())
        try:
            repo_commits = commits.get_repo_commits(token, GRAPHQL_URL, org, name, since)
            rels = commits.transform_single_repo_commits_to_relationships(name, repo_commits, org)
            commits.load_github_commit_relationships(session, rels, org, update_tag)
        except Exception:  # noqa: BLE001 — upstream logs and continues per repo
            logger.warning("Failed to fetch commits for %s", name, exc_info=True)
        timings["commits"] = round(time.monotonic() - t, 1)

    unsafe = sorted(flag for flag, ok in safe.items() if not ok)
    if unsafe:
        _write(session, _MARK_UNSAFE_QUERY, repo_url=repo_url, flags=unsafe, update_tag=update_tag)
    return {
        "part": part,
        "modules": sorted(mods),
        "repoSafe": safe,
        "unsafe": unsafe,
        "timings": timings,
        **deps_info,
    }


_MANIFEST_PATHS_QUERY = """
query($owner: String!, $repo: String!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    dependencyGraphManifests(first: 100, after: $cursor) {
      pageInfo { endCursor hasNextPage }
      nodes { blobPath }
    }
  }
}
"""


def _position_cursor(pos: int) -> str | None:
    """
    GitHub's dependencyGraphManifests cursors are base64 of the 1-based position
    (e.g. "MQ"="1", "MjUx"="251"), so
    after=_position_cursor(n) returns the manifest at 0-based position n.
    Undocumented; _get_manifest_range() verifies its first result against an
    independent listing and fails loudly if this ever stops holding.
    """
    if pos <= 0:
        return None
    return base64.b64encode(str(pos).encode()).decode().rstrip("=")


def _list_manifest_paths(token: Any, owner: str, repo: str) -> list[str]:
    """
    Every manifest blobPath, in position order, via the 100-per-page listing
    (no dependencies requested, so it is cheap and does not hit the per-manifest
    resolver timeouts). Source of truth for which manifests EXIST: that, not
    "every dependency fetch succeeded", is what makes manifest cleanup safe.
    Raises if the listing itself is incomplete.
    """
    paths: list[str] = []
    cursor: str | None = None
    while True:
        resp = call_github_api(
            _MANIFEST_PATHS_QUERY,
            json.dumps({"owner": owner, "repo": repo, "cursor": cursor}),
            token,
            GRAPHQL_URL,
        )
        conn = ((resp.get("data") or {}).get("repository") or {}).get("dependencyGraphManifests")
        if conn is None or resp.get("errors"):
            raise RuntimeError(f"{repo}: manifest listing failed: {resp.get('errors')}")
        for node in conn.get("nodes") or []:
            if node is None or not node.get("blobPath"):
                raise RuntimeError(f"{repo}: manifest listing returned a null/inaccessible node")
            paths.append(node["blobPath"])
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            return paths
        cursor = page.get("endCursor")


def _fetch_one_manifest(
    token: Any, org: str, repo: str, pos: int, expected_path: str
) -> dict[str, Any] | None:
    """
    One manifest, with ALL its dependency pages, at 0-based position `pos`, via
    upstream's fetch/retry helper (repos._fetch_manifest_page). Returns the
    manifest node, or None if GitHub could not return it completely. Raises if
    the manifest at `pos` is not `expected_path` (cursor format changed, or
    manifests shifted since the listing).
    """
    cursor = _position_cursor(pos)
    try:
        resp = repos._fetch_manifest_page(token, GRAPHQL_URL, org, repo, cursor)
    except requests.exceptions.RequestException:
        # Upstream's helper retries Timeout/HTTPError/ConnectionError/
        # ChunkedEncodingError but not e.g. JSONDecodeError (HTTP 200 with an
        # empty/non-JSON body, seen on a real repo).
        # Treat any request failure as "this manifest failed": skip, retry
        # later, carry forward. Never fail the whole repo step for one manifest.
        logger.warning("%s: manifest %d request failed", repo, pos, exc_info=True)
        return None
    conn = (
        (((resp or {}).get("data") or {}).get("organization") or {}).get("repository") or {}
    ).get("dependencyGraphManifests")
    nodes = (conn or {}).get("nodes") or []
    manifest = nodes[0] if nodes else None
    if manifest is None or resp.get("errors"):
        return None
    if manifest.get("blobPath") != expected_path:
        raise RuntimeError(
            f"{repo}: manifest at position {pos} is {manifest.get('blobPath')!r}, expected "
            f"{expected_path!r}. GitHub's cursor format may have changed, or manifests "
            "shifted mid-run; refusing to load a possibly-wrong manifest."
        )
    deps_data = manifest.get("dependencies") or {}
    all_dep_nodes = list(deps_data.get("nodes") or [])
    deps_page = deps_data.get("pageInfo") or {}
    while deps_page.get("hasNextPage", False):
        try:
            dep_resp = repos._fetch_manifest_page(
                token, GRAPHQL_URL, org, repo, cursor, deps_page.get("endCursor")
            )
        except requests.exceptions.RequestException:
            logger.warning("%s: manifest %d dependency page request failed", repo, pos, exc_info=True)
            return None
        dm = (
            (((dep_resp or {}).get("data") or {}).get("organization") or {}).get("repository") or {}
        ).get("dependencyGraphManifests")
        dnodes = (dm or {}).get("nodes") or []
        if not dnodes or dnodes[0] is None or dep_resp.get("errors"):
            return None  # partial dependency list: treat the manifest as not fetched
        inner = dnodes[0].get("dependencies") or {}
        all_dep_nodes.extend(inner.get("nodes") or [])
        deps_page = inner.get("pageInfo") or {}
    manifest["dependencies"] = {"nodes": all_dep_nodes}
    return manifest


def _get_manifest_range(token: Any, org: str, repo: str, lo: int, hi: int | None) -> dict[str, Any]:
    """
    Manifests at positions [lo, hi). Per manifest: fetch; on failure SKIP and
    continue; after the pass, RETRY each skipped manifest once. Anything still
    failing is returned in failed_paths for the caller to carry forward.
    """
    paths = _list_manifest_paths(token, org, repo)
    total = len(paths)
    stop = total if hi is None else min(hi, total)
    fetched: dict[int, dict[str, Any]] = {}
    skipped: list[int] = []

    for pos in range(lo, stop):
        m = _fetch_one_manifest(token, org, repo, pos, paths[pos])
        if m is None:
            logger.info("%s: manifest %d (%s) failed; will retry", repo, pos, paths[pos])
            skipped.append(pos)
        else:
            fetched[pos] = m

    failed: list[int] = []
    for pos in skipped:
        m = _fetch_one_manifest(token, org, repo, pos, paths[pos])
        if m is None:
            failed.append(pos)
        else:
            fetched[pos] = m

    if failed:
        logger.warning(
            "%s: %d manifest(s) still failing after retry; carrying forward: %s",
            repo, len(failed), [paths[p] for p in failed],
        )
    return {
        "manifests": [fetched[p] for p in sorted(fetched)],
        "failed_paths": [paths[p] for p in failed],
        "total": total,
        "retried": len(skipped),
    }


# Carry-forward: re-stamp a manifest that still EXISTS (it is in this run's
# listing) but whose dependencies could not be fetched this run, plus every
# edge/node that cleanup would otherwise delete because of it:
#   (Repo)-[:HAS_MANIFEST]->(m), (Org)-[:RESOURCE]->(m)      org manifest cleanup
#   (m)-[:HAS_DEP]->(d), d                                   global dependency cleanup
#   (Repo)-[:REQUIRES {manifest_path: m.blob_path}]->(d)     global dependency cleanup
#   (m)-[:MATCHES_CODEOWNER_RULE]->()                        codeowner match cleanup
# Cleanup query shapes verified against cartography.graph.cleanupbuilder output
# for GitHubDependencySchema / DependencyGraphManifestSchema.
# g2_last_fetched is NOT touched: it records the last real fetch.
_CARRY_FORWARD_QUERY = """
UNWIND $manifest_ids AS mid
MATCH (repo:GitHubRepository {id: $repo_url})-[hm:HAS_MANIFEST]->(m:GitHubDependencyGraphManifest {id: mid})
SET m.lastupdated = $update_tag, hm.lastupdated = $update_tag
WITH repo, m
OPTIONAL MATCH (:GitHubOrganization)-[res:RESOURCE]->(m)
SET res.lastupdated = $update_tag
WITH repo, m
OPTIONAL MATCH (m)-[cm:MATCHES_CODEOWNER_RULE]->()
SET cm.lastupdated = $update_tag
WITH repo, m
OPTIONAL MATCH (m)-[hd:HAS_DEP]->(d:GitHubDependency)
SET hd.lastupdated = $update_tag, d.lastupdated = $update_tag
WITH repo, m, d
OPTIONAL MATCH (repo)-[rq:REQUIRES]->(d)
WHERE rq.manifest_path = m.blob_path
SET rq.lastupdated = $update_tag
RETURN count(DISTINCT m) AS carried
"""

# Set ONLY on a real fetch. Custom (non-cartography) property, g2_-prefixed so
# it cannot collide with a future upstream field. Staleness of a manifest =
# lastupdated - g2_last_fetched (seconds since its dependencies were fetched).
_MARK_FETCHED_QUERY = """
UNWIND $manifest_ids AS mid
MATCH (m:GitHubDependencyGraphManifest {id: mid})
SET m.g2_last_fetched = $update_tag
"""


def _write(session: neo4j.Session, query: str, **params: Any) -> list[dict[str, Any]]:
    return session.execute_write(lambda tx: tx.run(query, **params).data())


def _carry_forward(
    session: neo4j.Session, repo_url: str, failed_paths: list[str], update_tag: int
) -> tuple[int, list[str]]:
    """
    Re-stamp carried-forward manifests. Returns (carried, never_synced). A
    failed manifest with no prior copy in the graph cannot be carried forward;
    it is absent until a run fetches it (and there is nothing to wrongly delete).
    """
    if not failed_paths:
        return 0, []
    ids = [f"{repo_url}#{p}" for p in failed_paths]
    rows = _write(session, _CARRY_FORWARD_QUERY, manifest_ids=ids, repo_url=repo_url, update_tag=update_tag)
    carried = rows[0]["carried"] if rows else 0
    existing = session.execute_read(
        read_list_of_dicts_tx,
        "UNWIND $ids AS mid MATCH (m:GitHubDependencyGraphManifest {id: mid}) RETURN m.id AS id",
        ids=ids,
    )
    have = {r["id"] for r in existing}
    never = [p for p, i in zip(failed_paths, ids) if i not in have]
    return carried, never


def _repo_deps(
    session: neo4j.Session,
    token: Any,
    org: str,
    name: str,
    row: dict[str, Any],
    update_tag: int,
    manifest_range: list[Any] | None = None,
) -> tuple[list[dict[str, Any]], bool, dict[str, Any]]:
    """
    repos.sync's dependency half + transform()'s dependency branch for one repo,
    or one manifest-position range of it (unsplit repo = [0, end)).

    cleanup_safe means "every manifest that EXISTS in this range is represented
    under this update_tag": fetched, or carried forward from a previous run. It
    is False only if a failed manifest had no previous copy to carry forward.
    Returns (manifests, cleanup_safe, info).
    """
    repo_url = row["url"]
    owner_org_id = row["owner_org_id"] or github_org_url(GRAPHQL_URL, org)
    lo, hi = manifest_range if manifest_range is not None else (0, None)
    r = _get_manifest_range(token, org, name, lo, hi)
    manifests_raw = r["manifests"]

    dep_manifests = {"nodes": manifests_raw} if manifests_raw else None
    manifests: list[dict[str, Any]] = []
    dependencies: list[dict[str, Any]] = []
    python_reqs: list[dict[str, Any]] = []
    repos._transform_dependency_manifests(dep_manifests, repo_url, manifests, row["defaultbranch"])
    repos._transform_dependency_graph(dep_manifests, repo_url, dependencies)
    # transform()'s fallback: parse requirements.txt / setup.cfg only when the
    # repo has no dependency graph at all (total 0), decided once by the range
    # starting at 0. org_core stripped these fields, so fetch them here
    # (upstream read them from the repo listing at HEAD).
    if r["total"] == 0 and lo == 0:
        base = rest_api_base_url(GRAPHQL_URL)
        owner = row["fullname"].split("/")[0]
        req = get_file_content(token, owner, name, "requirements.txt", base_url=base)
        cfg = get_file_content(token, owner, name, "setup.cfg", base_url=base)
        repos._transform_requirements_txt({"text": req} if req else None, repo_url, python_reqs)
        repos._transform_setup_cfg_requirements({"text": cfg} if cfg else None, repo_url, python_reqs)
    repos.enrich_dependencies_with_lockfile_versions(dependencies, token, GRAPHQL_URL)
    repos.load_python_requirements(session, update_tag, python_reqs)
    # Manifests BEFORE dependencies: HAS_DEP is created from the dependency side
    # (DependencyGraphManifestToDependencyRel) and needs the manifest node to
    # exist. Upstream's repos.load() loads them the other way round, which only
    # works there when the manifest already exists from a previous run.
    repos.load_github_dependency_manifests(session, update_tag, manifests, owner_org_id)
    repos.load_github_dependencies(session, update_tag, dependencies)
    if manifests:
        _write(session, _MARK_FETCHED_QUERY, manifest_ids=[m["id"] for m in manifests], update_tag=update_tag)

    carried, never_synced = _carry_forward(session, repo_url, r["failed_paths"], update_tag)
    info = {
        "manifestTotal": r["total"],
        "manifestRange": [lo, hi],
        "manifestsFetched": len(manifests_raw),
        "manifestsRetried": r["retried"],
        "manifestsCarriedForward": carried,
        "manifestsNeverSynced": len(never_synced),
    }
    if never_synced:
        logger.warning(
            "%s: %d manifest(s) failed with no previous copy to carry forward: %s",
            name, len(never_synced), never_synced,
        )
    return manifests, not never_synced, info


_REPO_MANIFESTS_QUERY = """
MATCH (:GitHubRepository {id: $repo_url})-[:HAS_MANIFEST]->(m:GitHubDependencyGraphManifest)
WHERE m.lastupdated = $update_tag
RETURN m.id AS id, m.repo_url AS repo_url, m.blob_path AS blob_path,
       m.repo_relative_path AS repo_relative_path
"""


def _repo_codeowners(
    session: neo4j.Session,
    token: Any,
    name: str,
    repo_url: str,
    default_branch: str | None,
    owner_org_id: str,
    org_url: str,
    manifests: list[dict[str, Any]] | None,
    update_tag: int,
) -> bool:
    """
    codeowners.sync per-repo body. Returns cleanup_safe. `manifests` None means
    deps ran in earlier parts of this run: read this run's manifests from Neo4j.
    """
    if manifests is None:
        manifests = session.execute_read(
            read_list_of_dicts_tx, _REPO_MANIFESTS_QUERY, repo_url=repo_url, update_tag=update_tag
        )
    targets = session.execute_read(read_list_of_dicts_tx, _OWNER_TARGETS_QUERY, org_url=org_url)
    users_list = [u for u in targets[0]["users"] if u.get("login")] if targets else []
    teams_list = [x for x in targets[0]["teams"] if x.get("name")] if targets else []
    lookups = codeowners.build_codeowner_target_lookups(users_list, teams_list)
    fetch = codeowners.get_effective_codeowners_file(token, GRAPHQL_URL, repo_url, default_branch)
    rules: list[dict[str, Any]] = []
    if fetch.cleanup_safe and fetch.content is not None and fetch.source_path is not None:
        rules = codeowners.parse_codeowners_content(
            fetch.content, repo_url, name, default_branch, fetch.source_path, owner_org_id, lookups
        )
    codeowners.load_codeowner_rules(session, rules, update_tag, owner_org_id)
    matches = codeowners.build_manifest_codeowner_matches(rules, manifests, {repo_url: default_branch})
    codeowners.load_manifest_matches(session, matches, update_tag, owner_org_id)
    return fetch.cleanup_safe


def _repo_actions(
    session: neo4j.Session,
    token: Any,
    org: str,
    name: str,
    org_url: str,
    update_tag: int,
    envs: Any = None,
) -> None:
    """
    Body of actions.sync()'s `for repo_name in repo_names` loop, verbatim order.
      envs None    -> everything (unsplit)
      envs "none"  -> workflows, environment nodes, repo secrets/vars; no per-env calls
      envs [lo,hi] -> ONLY per-env secrets/vars for env names in [lo, hi)
    """
    env_range_only = isinstance(envs, list)

    if not env_range_only:
        workflows = actions.get_repo_workflows(token, GRAPHQL_URL, org, name)
        if workflows:
            enriched: list[dict[str, Any]] = []
            repo_actions: list[dict[str, Any]] = []
            for wf in actions.transform_workflows(workflows, org, name):
                path, wf_id = wf.get("path"), wf.get("id")
                content = (
                    actions.get_workflow_content(token, GRAPHQL_URL, org, name, path) if path else None
                )
                parsed = actions.parse_workflow_yaml(content) if content else None
                enriched.append(actions.enrich_workflow_with_parsed_content(wf, parsed, org, name))
                if parsed and wf_id is not None:
                    repo_actions.extend(actions.transform_actions(parsed, wf_id, org, name))
            actions.load_workflows(session, enriched, update_tag, org_url)
            if repo_actions:
                actions.load_actions(session, repo_actions, update_tag, org_url)

    # The environment list is needed by every mode (env-range parts filter it).
    environments = actions.get_repo_environments(token, GRAPHQL_URL, org, name)

    if not env_range_only:
        if environments:
            actions.load_environments(
                session, actions.transform_environments(environments, org, name), update_tag, org_url
            )
        secrets = actions.get_repo_secrets(token, GRAPHQL_URL, org, name)
        if secrets:
            actions.load_repo_secrets(
                session, actions.transform_repo_secrets(secrets, org, name), update_tag, org_url
            )
        variables = actions.get_repo_variables(token, GRAPHQL_URL, org, name)
        if variables:
            actions.load_repo_variables(
                session, actions.transform_repo_variables(variables, org, name), update_tag, org_url
            )

    if envs == "none":
        return
    for env in environments or []:
        env_name, env_id = env["name"], env["id"]
        if env_range_only and not _env_in_range(env_name, envs):
            continue
        es = actions.get_env_secrets(token, GRAPHQL_URL, org, name, env_name)
        if es:
            actions.load_env_secrets(
                session,
                actions.transform_env_secrets(es, org, name, env_name, env_id),
                update_tag,
                org_url,
            )
        ev = actions.get_env_variables(token, GRAPHQL_URL, org, name, env_name)
        if ev:
            actions.load_env_variables(
                session,
                actions.transform_env_variables(ev, org, name, env_name, env_id),
                update_tag,
                org_url,
            )


# ---------------------------------------------------------------------------
# org_finalize
# ---------------------------------------------------------------------------


def _check_repo_coverage(
    expected_items: list[dict[str, Any]],
    ran_items: list[dict[str, Any]],
    failed: list[Any],
) -> list[str]:
    """
    Coverage check for org_finalize. Returns problems; any problem => no cleanup.

    expected_items: build_items() over GitHub's repo list NOW (+ split config).
    ran_items:      the items the repo Map actually iterated (org_core's list).
    failed:         the Map's per-item failures ("repo:part" strings).

    Per-repo results never travel through Step Functions: success is implied by
    the item not being in `failed`; cleanup-safety flags are read from Neo4j.
    """
    problems = [f"repo step failed: {f}" for f in failed]
    key = lambda i: (i["repo"], int(i["part"]))  # noqa: E731
    expected = {key(i) for i in expected_items}
    ran = [key(i) for i in ran_items]
    if len(ran) != len(set(ran)):
        problems.append("duplicate items in the repo Map")
    for r, p in sorted(expected - set(ran)):
        problems.append(f"not run: {r} part {p} (repo created or split config changed mid-run?)")
    for r, p in sorted(set(ran) - expected):
        problems.append(f"ran but no longer expected: {r} part {p} (repo deleted or config changed mid-run?)")
    return problems


def _org_repo_safety(session: neo4j.Session, org_url: str, update_tag: int) -> dict[str, bool]:
    """AND of every repo's cleanup-safety flags for this run, from Neo4j."""
    rows = session.execute_read(
        read_list_of_dicts_tx, _ORG_UNSAFE_QUERY, org_url=org_url, update_tag=update_tag
    )
    unsafe = {r["flag"] for r in rows}
    totals = session.execute_read(
        read_list_of_dicts_tx, _ORG_MANIFEST_TOTALS_QUERY, org_url=org_url, update_tag=update_tag
    )
    for row in totals:
        # Manifest-ranged repo whose parts saw different manifest counts:
        # positions shifted mid-run, a manifest may have fallen between ranges.
        logger.warning(
            "Manifest count changed mid-run for %s %s; disabling manifest cleanup.",
            row["repo"], row["totals"],
        )
        unsafe.add("deps")
    return {flag: flag not in unsafe for flag in REPO_SAFE_FLAGS}


def _org_finalize(
    session: neo4j.Session,
    token: Any,
    org: str,
    update_tag: int,
    ran_items: list[dict[str, Any]],
    failed: list[Any],
    org_safe: dict[str, bool],
    repos_complete: bool,
) -> dict[str, Any]:
    common = _common(update_tag)
    org_url = github_org_url(GRAPHQL_URL, org)

    # Source of truth for "every repo" is GitHub right now, not org_core's
    # output — a repo created mid-run would otherwise be silently cleaned up.
    current = sorted(r["name"] for r in repos.get(token, GRAPHQL_URL, org) if r is not None)
    org_splits = _validate_splits(_load_splits(), org)
    problems = _check_repo_coverage(build_items(current, org_splits), ran_items, failed)
    if problems:
        raise RuntimeError(
            f"org_finalize refused to clean up {org}: {len(problems)} coverage problem(s): "
            + "; ".join(problems[:20])
        )
    repo_safe = _org_repo_safety(session, org_url, update_tag)

    skipped: list[str] = []

    def gated(name: str, ok: bool, fn: Any) -> None:
        if ok:
            fn()
        else:
            skipped.append(name)
            logger.warning("Skipping %s cleanup for %s (not cleanup-safe)", name, org)

    owner_org_id = org_url

    # org-level (from org_core)
    gated("domains", org_safe.get("domains", False),
          lambda: organizations.cleanup_domains(session, org_url, update_tag))
    gated("external_identities", org_safe.get("external_identities", False),
          lambda: external_identities.GraphJob.from_node_schema(
              external_identities.GitHubExternalIdentitySchema(), {**common, "org_url": org_url}
          ).run(session))
    gated("pats", org_safe.get("pats", False),
          lambda: personal_access_tokens.cleanup_personal_access_tokens(session, org_url, update_tag))
    gated("app_installations", org_safe.get("app_installations", False),
          lambda: app_installations.cleanup(session, org_url, update_tag))
    gated("dependabot", org_safe.get("dependabot", False),
          lambda: dependabot_alerts.cleanup(session, common, org_url))
    teams.cleanup(session, {**common, "org_url": org_url})
    repos.cleanup_github_branches(session, common, owner_org_id)
    repos.cleanup_branch_protection_rules(session, common, owner_org_id)
    gated("rulesets", org_safe.get("rulesets", False),
          lambda: repos.cleanup_rulesets(session, common, owner_org_id))

    # per-repo data, aggregated
    gated("manifests", repo_safe["deps"],
          lambda: repos.cleanup_github_manifests(session, common, owner_org_id))
    gated("codeowner_rules", repo_safe["codeowners"],
          lambda: codeowners.cleanup_codeowner_rules(session, common, owner_org_id))
    gated("codeowner_manifest_matches", repo_safe["codeowners"] and repo_safe["deps"],
          lambda: codeowners.cleanup_manifest_matches(session, common, owner_org_id))
    gated("webhooks", repo_safe["webhooks"] and org_safe.get("org_webhooks", False) and repos_complete,
          lambda: webhooks.cleanup(session, org_url, update_tag))

    # actions: migration + org-level cleanup (covers org, repo, env secrets/vars,
    # workflows, environments, actions)
    acl = {**common, "org_url": org_url}
    run_analysis_job("github_repo_actions_secret_resource_edge_migration.json", session, acl)
    actions.cleanup_org_level(session, acl)

    commits.cleanup_github_commit_relationships(session, org, update_tag)

    # supply_chain: needs repos + workflows; both are in the graph now.
    repo_rows = session.execute_read(
        read_list_of_dicts_tx,
        "MATCH (:GitHubOrganization {id: $org_url})<-[:OWNER]-(r:GitHubRepository) "
        "RETURN r.id AS url, r.name AS name, r.fullname AS fullname",
        org_url=org_url,
    )
    wf_rows = session.execute_read(
        read_list_of_dicts_tx,
        "MATCH (:GitHubOrganization {id: $org_url})<-[:OWNER]-(r:GitHubRepository)"
        "-[:HAS_WORKFLOW]->(w:GitHubWorkflow) RETURN w.repo_url AS repo_url, w.path AS path",
        org_url=org_url,
    )
    if repo_rows:
        supply_chain.sync(
            session, token, GRAPHQL_URL, org, update_tag, common, repo_rows, workflows=wf_rows
        )

    return {"skippedCleanups": skipped, "repoCount": len(current)}


# ---------------------------------------------------------------------------
# unscoped_cleanup guard
# ---------------------------------------------------------------------------


def _check_all_orgs_finalized(org_results: list[dict[str, Any]], update_tag: int) -> None:
    """
    cleanup_unscoped_github_resources() deletes GitHubRepository, GitHubUser,
    GitHubDependency, language, owner, collaborator and python-requirement data
    across ALL orgs that is not stamped with update_tag. Running it after a
    partial run (one org, or a targeted repo subset) deletes every other org's
    data. The orchestrator checks this too; this is the backstop. Requires a
    SUCCEEDED org_finalize result, under this exact tag, for every configured org.
    """
    finalized = {
        r.get("org")
        for r in org_results
        if r.get("step") == "org_finalize"
        and r.get("status") == "SUCCEEDED"
        and r.get("updateTag") == update_tag
    }
    missing = sorted(set(_load_orgs()) - finalized)
    if missing:
        raise RuntimeError(
            f"unscoped_cleanup refused: no SUCCEEDED org_finalize under update_tag "
            f"{update_tag} for org(s) {missing}. Unscoped cleanup is global and would "
            "delete those orgs' repos/users/dependencies."
        )


# ---------------------------------------------------------------------------
# entrypoint
# ---------------------------------------------------------------------------

STEPS = ("expand_targets", "org_core", "ghcr_expand", "ghcr", "repo", "org_finalize", "unscoped_cleanup")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Always returns a result dict (never raises): status SUCCEEDED|FAILED|
    RATE_LIMITED, plus error on failure. cartography-github-lambda-sync turns
    FAILED into a Fail state so the execution is red and the error/cause are
    visible, and turns RATE_LIMITED into a Wait until resumeAt + re-invoke.
    """
    start = time.time()
    _guarded_time.context = context
    step = event.get("step")
    org = event.get("org")
    repo = event.get("repo")
    result: dict[str, Any] = {"status": "FAILED", "step": step, "org": org, "repo": repo}
    if step in ("repo", "ghcr"):
        result["part"] = int(event.get("part", 0))

    driver = None
    try:
        if step not in STEPS:
            raise ValueError(f"event.step must be one of {STEPS}, got {step!r}")
        if step == "expand_targets":
            # Targeted run: names -> (repo, part) items via the same split
            # config the full run uses. No GitHub or Neo4j calls.
            orgs = _load_orgs()
            if org not in orgs:
                raise ValueError(f"Unknown org {org!r}; configured: {sorted(orgs)}")
            names = event.get("repos") or []
            if not names:
                raise ValueError("event.repos must be a non-empty list")
            result["items"] = build_items(names, _validate_splits(_load_splits(), org))
            result["status"] = "SUCCEEDED"
            return result
        if step == "ghcr_expand":
            # How many ghcr parts this org needs, from GITHUB_GHCR_SPLITS_PARAM.
            # SSM-only, no GitHub or Neo4j calls, same as expand_targets.
            result["parts"] = _ghcr_part_count(org)
            result["status"] = "SUCCEEDED"
            return result
        if event.get("update_tag") is None:
            raise ValueError("event.update_tag is required (one shared tag per run)")
        update_tag = int(event["update_tag"])
        result["updateTag"] = update_tag
        if step != "unscoped_cleanup" and not org:
            raise ValueError("event.org is required")
        if step == "repo" and not repo:
            raise ValueError("event.repo is required for step=repo")

        driver = _get_neo4j_driver()
        with driver.session(database=os.environ.get("NEO4J_DATABASE") or None) as session:
            if step == "unscoped_cleanup":
                _check_all_orgs_finalized(event.get("org_results") or [], update_tag)
                cleanup_unscoped_github_resources(session, _common(update_tag))
            elif step == "ghcr":
                # No App token needed: GHCR is PAT-only (_get_domains_credential).
                result.update(_ghcr(session, org, update_tag, int(event.get("part", 0))))
            else:
                token = _get_credential(org)
                if step == "org_core":
                    result.update(_org_core(session, token, org, update_tag))
                elif step == "repo":
                    result.update(
                        _repo(session, token, org, repo, int(event.get("part", 0)), update_tag)
                    )
                elif step == "org_finalize":
                    result.update(
                        _org_finalize(
                            session,
                            token,
                            org,
                            update_tag,
                            event.get("items") or [],
                            event.get("failed") or [],
                            event.get("org_safe") or {},
                            bool(event.get("repos_complete", False)),
                        )
                    )
        result["status"] = "SUCCEEDED"
    except GitHubRateLimited as exc:
        # Not a failure: the sync state machine waits until resumeAt and
        # re-invokes this same event.
        result["status"] = "RATE_LIMITED"
        result["resumeAt"] = exc.resume_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        result["error"] = str(exc)
    except Exception as exc:  # noqa: BLE001 — surface to the state machine
        logger.exception("step=%s org=%s repo=%s failed", step, org, repo)
        result["error"] = f"{type(exc).__name__}: {exc}"[:2000]
    finally:
        _guarded_time.context = None
        if driver is not None:
            driver.close()
        result["durationSeconds"] = round(time.time() - start, 1)
    return result
