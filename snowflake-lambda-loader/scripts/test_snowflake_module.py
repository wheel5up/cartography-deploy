#!/usr/bin/env python3
"""
Local test harness for ../src/handler.py — drives the REAL handler.handler()
Lambda entrypoint directly (not a reimplementation), so what this script
exercises is exactly the same code path the deployed Lambda will run.

run_step()/handler() thread prerequisite outputs internally (via Context —
see handler.py's module docstring), so this script just builds the event
dict Lambda would receive and calls handler.handler() with it, same
contract a real invocation gets. Useful as a local, fast, single-process way to
test a subset of steps without deploying anything or waiting through a full
orchestrator run.

PREREQUISITES
--------------
1. AWS credentials for SSM GetParameter against /cartography/snowflake/*
   (same six params ../scripts/bootstrap-snowflake-ssm.sh wrote). Uses the
   usual AWS_PROFILE / AWS_REGION env vars.

2. Network reachability to Neo4j directly (no SSM tunnel needed if already
   on the right network). Pass --neo4j-uri or set NEO4J_URI.

3. cartography installed in the active Python environment at the fork tag
   the Makefile pins. This script imports
   cartography.intel.snowflake.* (via handler.py), so whatever is
   `pip install`ed is what actually runs.

USAGE
-----
    # Run a handful of steps in cartography's own dependency order
    # (matching ALL_STEPS_IN_ORDER's relative order). handler.handler()
    # re-runs missing prerequisites itself (see handler.py's
    # _resolve_steps_with_prerequisites), except grants_cleanup, which
    # needs grants and account_grants listed explicitly:
    python test_snowflake_module.py databases schemas tables

    # Test the full grants family together (they must stay grouped — see
    # handler.py's run_grants_cleanup docstring):
    python test_snowflake_module.py roles users databases database_roles grants account_grants grants_cleanup

    # Test the cleanup + sync_complete steps against a cheap, already-synced
    # subset:
    python test_snowflake_module.py account cleanup sync_complete

    # Continue a prior run under the same update_tag, optionally passing
    # forward an accumulated incomplete-set (see handler.py's module
    # docstring "COMPLETENESS TRACKING"):
    python test_snowflake_module.py cleanup --update-tag 1792204817 --incomplete tables,views

Every write goes to the REAL Neo4j instance, using a FRESH update_tag
(current unix time) unless --update-tag is passed. This is the real graph;
re-running is safe (cartography sync is idempotent per update_tag) but
updates real LASTUPDATED timestamps.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import handler  # noqa: E402  (path must be adjusted first, see above)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("USAGE")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "steps",
        nargs="+",
        choices=sorted(handler.ALL_STEPS_IN_ORDER),
        help="One or more of: " + ", ".join(handler.ALL_STEPS_IN_ORDER),
    )
    parser.add_argument(
        "--update-tag",
        type=int,
        default=None,
        help="Reuse an existing update_tag instead of minting a fresh one.",
    )
    parser.add_argument(
        "--incomplete",
        default="",
        help="Comma-separated module names to seed as already-incomplete "
        "from earlier invocations (see handler.py's 'incomplete' event key).",
    )
    parser.add_argument(
        "--neo4j-uri",
        default=os.environ.get("NEO4J_URI"),
        required=not os.environ.get("NEO4J_URI"),
    )
    args = parser.parse_args()

    os.environ.setdefault("NEO4J_URI", args.neo4j_uri)

    event = {"steps": args.steps}
    if args.update_tag is not None:
        event["update_tag"] = args.update_tag
    if args.incomplete:
        event["incomplete"] = [s.strip() for s in args.incomplete.split(",") if s.strip()]

    print(f"event: {json.dumps(event)}")
    print(f"neo4j_uri: {args.neo4j_uri}")
    print("-" * 72)

    result = handler.handler(event, None)

    print("-" * 72)
    print(json.dumps(result, indent=2))
    print("-" * 72)
    duration = result.get("durationSeconds", 0)
    print(
        "Lambda 900s budget check: "
        + (
            "WITHIN budget"
            if duration < 900
            else f"EXCEEDS 900s by {duration - 900:.0f}s"
        )
    )
    return 0 if result.get("status") == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
