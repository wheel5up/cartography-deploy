#!/usr/bin/env python3
"""
Local harness: drives src/handler.handler() against real GitHub + real Neo4j,
the same way the state machine will. Writes to Neo4j (no cleanup unless you
ask for org_finalize / unscoped_cleanup explicitly).

  PY=<cartography-venv>/bin/python
  # org_core only (loads org-level data + repo nodes, no cleanup)
  $PY scripts/run_local.py org_core --org my-org
  # one or more repos (reuse the tag org_core printed)
  $PY scripts/run_local.py repo --org my-org --tag 1792... --repos repo-a,repo-b
  # full org: org_core + every repo + org_finalize  (DOES clean up)
  $PY scripts/run_local.py full --org my-org --i-mean-it

Requires NEO4J_URI (e.g. bolt://<neo4j-private-ip>:8443) and AWS_REGION.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
for _var in ("NEO4J_URI", "AWS_REGION"):
    if not os.environ.get(_var):
        sys.exit(f"set {_var}")
os.environ.setdefault("AWS_DEFAULT_REGION", os.environ["AWS_REGION"])

import handler  # noqa: E402


def call(event: dict) -> dict:
    res = handler.handler(event, None)
    summary = {k: v for k, v in res.items() if k not in ("items",)}
    if "items" in res:
        summary["items"] = f"<{len(res['items'])} items>"
    print(json.dumps(summary, default=str), flush=True)
    return res


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["org_core", "repo", "full", "unscoped_cleanup"])
    ap.add_argument("--org")
    ap.add_argument("--tag", type=int, help="shared update_tag (default: now)")
    ap.add_argument("--repos", help="comma-separated (repo mode)")
    ap.add_argument("--i-mean-it", action="store_true", help="required for modes that clean up")
    args = ap.parse_args()

    tag = args.tag or int(time.time())
    print(f"update_tag={tag}", flush=True)

    if args.mode == "org_core":
        call({"step": "org_core", "org": args.org, "update_tag": tag})
        return 0

    if args.mode == "repo":
        if not args.tag:
            sys.exit("--tag is required for repo mode (use the tag org_core ran with)")
        exp = call({"step": "expand_targets", "org": args.org,
                    "repos": [n.strip() for n in args.repos.split(",")]})
        for it in exp["items"]:
            call({"step": "repo", "org": args.org, "update_tag": tag, **it})
        return 0

    if not args.i_mean_it:
        sys.exit(f"{args.mode} runs cleanup; re-run with --i-mean-it")

    if args.mode == "unscoped_cleanup":
        sys.exit(
            "unscoped_cleanup is global; run it only via the state machine after "
            "every org's org_finalize succeeded under one tag."
        )

    core = call({"step": "org_core", "org": args.org, "update_tag": tag})
    if core["status"] != "SUCCEEDED":
        return 1
    failed = []
    for it in core["items"]:
        r = call({"step": "repo", "org": args.org, "update_tag": tag, **it})
        if r["status"] != "SUCCEEDED":
            failed.append(f"{it['repo']}:{it['part']}")
    fin = call(
        {
            "step": "org_finalize",
            "org": args.org,
            "update_tag": tag,
            "items": core["items"],
            "failed": failed,
            "org_safe": core["orgSafe"],
            "repos_complete": core["reposComplete"],
        }
    )
    return 0 if fin["status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    sys.exit(main())
