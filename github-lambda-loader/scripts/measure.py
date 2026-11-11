#!/usr/bin/env python3
"""
Read-only per-repo timing for the two GitHub sub-syncs too slow for one Lambda
invocation: dependency-graph manifests (repos.py) and Actions (actions.py).

Never touches Neo4j. Calls only cartography's GitHub API fetch functions, in the
same serial order the real sync uses, and times each repo individually. Output
is used to choose pinned repos and alphabetical range boundaries for
dependencies:* and actions:* batches.

Run with the fork's venv so the pinned cartography source is importable:

  PY=<cartography-venv>/bin/python
  $PY scripts/measure.py --org my-org
  $PY scripts/measure.py --org my-org --phase deps
  $PY scripts/measure.py --org my-org --repos repo-a,repo-b

Results append to measure-<org>-<phase>.csv (one row per repo, flushed as it
goes, so a long run that dies midway still leaves usable data).
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import boto3

from cartography.intel.github import actions
from cartography.intel.github import repos
from cartography.intel.github.app_auth import make_credential

REGION = os.environ["AWS_REGION"]
GRAPHQL_URL = "https://api.github.com/graphql"
ORGS_PARAM = "/cartography/github-lambda/orgs"

logger = logging.getLogger("measure")


def _ssm(name: str, decrypt: bool = False) -> str:
    client = boto3.client("ssm", region_name=REGION)
    return client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


def build_credential(org: str) -> Any:
    return make_credential(
        {
            "client_id": _ssm("/cartography/github/app/client-id"),
            "private_key": _ssm("/cartography/github/app/private-key", decrypt=True),
            "installation_id": _ssm(json.loads(_ssm(ORGS_PARAM))[org]),
            "url": GRAPHQL_URL,
            "name": org,
        },
    )


def time_deps(token: Any, org: str, repo: dict[str, Any]) -> dict[str, Any]:
    """Mirror repos.sync()'s per-repo dependency work: manifest fetch, then lockfile enrichment."""
    name = repo["name"]
    t0 = time.monotonic()
    manifests, cleanup_safe = repos._get_repo_dep_manifests(token, GRAPHQL_URL, org, name)
    fetch_s = time.monotonic() - t0

    deps: list[dict[str, Any]] = []
    repos._transform_dependency_graph({"nodes": manifests}, repo["url"], deps)
    t1 = time.monotonic()
    repos.enrich_dependencies_with_lockfile_versions(deps, token, GRAPHQL_URL)
    enrich_s = time.monotonic() - t1

    return {
        "repo": name,
        "manifests": len(manifests),
        "deps": len(deps),
        "cleanup_safe": cleanup_safe,
        "fetch_s": round(fetch_s, 1),
        "enrich_s": round(enrich_s, 1),
        "total_s": round(fetch_s + enrich_s, 1),
    }


def time_actions(token: Any, org: str, repo: dict[str, Any]) -> dict[str, Any]:
    """Mirror actions.sync()'s per-repo API calls (no transforms/loads, which are negligible)."""
    name = repo["name"]
    t0 = time.monotonic()
    workflows = actions.get_repo_workflows(token, GRAPHQL_URL, org, name) or []
    for wf in workflows:
        if wf.get("path"):
            actions.get_workflow_content(token, GRAPHQL_URL, org, name, wf["path"])
    envs = actions.get_repo_environments(token, GRAPHQL_URL, org, name) or []
    actions.get_repo_secrets(token, GRAPHQL_URL, org, name)
    actions.get_repo_variables(token, GRAPHQL_URL, org, name)
    for env in envs:
        actions.get_env_secrets(token, GRAPHQL_URL, org, name, env["name"])
        actions.get_env_variables(token, GRAPHQL_URL, org, name, env["name"])
    return {
        "repo": name,
        "workflows": len(workflows),
        "environments": len(envs),
        "total_s": round(time.monotonic() - t0, 1),
    }


PHASES = {"deps": time_deps, "actions": time_actions}


def run_phase(token: Any, org: str, phase: str, repo_list: list[dict[str, Any]], out_dir: Path) -> None:
    out = out_dir / f"measure-{org}-{phase}.csv"
    fn = PHASES[phase]
    writer = None
    phase_start = time.monotonic()
    with out.open("a", newline="") as fh:
        for i, repo in enumerate(repo_list, 1):
            try:
                row = fn(token, org, repo)
            except Exception as exc:  # keep measuring the rest; record the failure
                row = {"repo": repo["name"], "total_s": -1, "error": repr(exc)[:200]}
            if writer is None:
                fields = list(row.keys()) + (["error"] if "error" not in row else [])
                writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
                if fh.tell() == 0:
                    writer.writeheader()
            writer.writerow(row)
            fh.flush()
            print(f"[{phase}] {i}/{len(repo_list)} {row['repo']}: {row['total_s']}s", flush=True)
    print(f"[{phase}] {org} done in {time.monotonic() - phase_start:.0f}s -> {out}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--org", required=True, help=f"org login listed in {ORGS_PARAM}")
    ap.add_argument("--phase", choices=["deps", "actions", "both"], default="both")
    ap.add_argument("--repos", help="comma-separated repo names to measure (default: all)")
    ap.add_argument("--out-dir", default=str(Path(__file__).resolve().parent.parent / "measurements"))
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    token = build_credential(args.org)

    t0 = time.monotonic()
    repo_list = [r for r in repos.get(token, GRAPHQL_URL, args.org) if r is not None]
    print(f"repos.get: {len(repo_list)} repos in {time.monotonic() - t0:.0f}s", flush=True)

    if args.repos:
        wanted = {r.strip().lower() for r in args.repos.split(",")}
        repo_list = [r for r in repo_list if r["name"].lower() in wanted]
        missing = wanted - {r["name"].lower() for r in repo_list}
        if missing:
            print(f"WARNING: not found in org: {sorted(missing)}", file=sys.stderr)

    phases = ["deps", "actions"] if args.phase == "both" else [args.phase]
    for phase in phases:
        run_phase(token, args.org, phase, repo_list, out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
