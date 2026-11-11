"""
The module list, and the rules an account-splits config must follow.

Standard library only: no cartography import, so the lint runs with plain
python3. handler.py imports DEFAULT_SYNCS from here.

account-splits (SSM /cartography/lambda-orchestrator/account-splits) is
{account_id: {"splits": [split, ...]}}. Each split is {"include": [...]}
(run exactly these) or {"exclude": [...]} (run DEFAULT_SYNCS minus these).
The orchestrator runs an account's splits one after another, in order.

    python3 src/splits.py account-splits.json   # exit 1 on any violation
"""

from __future__ import annotations

import json
import sys

# Sync order. Upstream cartography's RESOURCE_FUNCTIONS order, which is how
# upstream encodes module dependencies (resources.py: "Module dependencies are
# enforced by iterating over this dict in order"), with two changes:
#   - resourcegroupstaggingapi is not here: it runs in a separate pass.
#   - permission_relationships is last: it reads every resource type.
DEFAULT_SYNCS: list[str] = [
    "iam", "kms", "s3", "dynamodb", "ec2:instance", "ec2:images",
    "ec2:security_group", "ec2:subnet", "ec2:load_balancer",
    "ec2:load_balancer_v2", "ec2:network_acls", "ec2:network_interface",
    "ec2:tgw", "ec2:tgw_route_table", "ec2:vpc", "ec2:vpc_endpoint",
    "ec2:route_table", "ec2:vpc_peering", "ec2:internet_gateway",
    "ec2:reserved_instances", "ec2:volumes", "ec2:snapshots", "ecr",
    "elasticache", "emr", "lambda_function", "rds", "redshift", "route53",
    "elasticsearch", "cloudformation",
    "apigateway", "apigatewayv2", "secretsmanager",
    "securityhub", "sagemaker", "ses", "sns", "sqs", "ssm", "acm:certificate",
    "inspector", "identitycenter", "cloudtrail", "cloudwatch", "efs", "eks",
    "guardduty", "codebuild", "cognito", "eventbridge", "glue", "ec2:keypair",
    "ecs", "permission_relationships",
]

# Modules valid in an include list but not part of DEFAULT_SYNCS.
EXTRA_MODULES: frozenset[str] = frozenset({
    "iam:service_access",
    "resourcegroupstaggingapi",
})

# Modules that may run after modules that come later in DEFAULT_SYNCS,
# because no other synced module links to the nodes they write. Checked
# against the fork's models at the Makefile's CARTOGRAPHY_GIT_REF (which node schemas have a
# relationship targeting each module's labels):
#   inspector      AWSInspectorFinding/Package: no readers.
#   sagemaker      AWSSageMaker*: no readers.
#   securityhub    AWSSecurityHub: no readers.
#   secretsmanager AWSSecretsManagerSecret: read only by
#                  ecr:pull_through_cache_rules, which isn't synced.
# They must still run before permission_relationships (rule 3).
# To add one: show that nothing in DEFAULT_SYNCS links to its labels.
MAY_RUN_LATE: frozenset[str] = frozenset({
    "inspector",
    "sagemaker",
    "securityhub",
    "secretsmanager",
})

LAST = "permission_relationships"
SSM_MAX_BYTES = 8192  # SSM Advanced tier


def resolve(split: dict) -> list[str]:
    """Modules a split runs, in run order (same logic as the handler)."""
    if split.get("include"):
        return list(dict.fromkeys(split["include"]))
    exclude = set(split.get("exclude", []))
    return [m for m in DEFAULT_SYNCS if m not in exclude]


def validate_account(account_id: str, entry: object) -> list[str]:
    """All rule violations for one account entry."""
    if not isinstance(entry, dict) or not isinstance(entry.get("splits"), list) or not entry["splits"]:
        return [f"{account_id}: needs a non-empty 'splits' list"]
    errors: list[str] = []
    known = set(DEFAULT_SYNCS) | EXTRA_MODULES
    run: list[str] = []
    for i, split in enumerate(entry["splits"]):
        where = f"{account_id} split {i}"
        if not isinstance(split, dict) or set(split) - {"include", "exclude"}:
            errors.append(f"{where}: only 'include' or 'exclude' allowed")
            continue
        if split.get("include") and split.get("exclude"):
            errors.append(f"{where}: has both 'include' and 'exclude'")
            continue
        names = split.get("include") or split.get("exclude") or []
        unknown = [m for m in names if m not in known]
        if unknown:
            errors.append(f"{where}: unknown module(s) {unknown}")
        run += [m for m in resolve(split) if m in DEFAULT_SYNCS]

    # Rule 1: every module runs exactly once.
    counts = {m: run.count(m) for m in DEFAULT_SYNCS}
    missing = [m for m, n in counts.items() if n == 0]
    repeated = [m for m, n in counts.items() if n > 1]
    if missing:
        errors.append(f"{account_id}: never runs {missing}")
    if repeated:
        errors.append(f"{account_id}: runs more than once {repeated}")

    # Rule 2: DEFAULT_SYNCS order, except MAY_RUN_LATE.
    ordered = [m for m in dict.fromkeys(run) if m not in MAY_RUN_LATE]
    for earlier, later in zip(ordered, ordered[1:]):
        if DEFAULT_SYNCS.index(earlier) > DEFAULT_SYNCS.index(later):
            errors.append(
                f"{account_id}: '{earlier}' runs before '{later}', "
                f"but '{later}' comes first in DEFAULT_SYNCS"
            )

    # Rule 3: permission_relationships last.
    if run and run[-1] != LAST and LAST in run:
        after = run[run.index(LAST) + 1:]
        errors.append(f"{account_id}: '{LAST}' must run last; runs before {after}")
    return errors


def validate(config: object, raw: str | None = None) -> list[str]:
    """All rule violations for a whole account-splits config."""
    if not isinstance(config, dict):
        return ["config must be a JSON object keyed by account ID"]
    errors: list[str] = []
    for account_id, entry in config.items():
        if not (isinstance(account_id, str) and account_id.isdigit() and len(account_id) == 12):
            errors.append(f"{account_id!r}: not a 12-digit account ID")
        errors += validate_account(account_id, entry)
    # Rule 4: fits in SSM. Measured on the compact form that gets stored.
    size = len((raw if raw is not None else json.dumps(config, separators=(",", ":"))).encode())
    if size > SSM_MAX_BYTES:
        errors.append(f"config is {size} bytes; SSM limit is {SSM_MAX_BYTES}")
    return errors


def compact(config: dict) -> str:
    """The form to store in SSM: no empty include/exclude keys, no whitespace."""
    out = {
        a: {"splits": [{k: v for k, v in s.items() if v} for s in e["splits"]]}
        for a, e in config.items()
    }
    return json.dumps(out, separators=(",", ":"))


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} ACCOUNT_SPLITS_JSON", file=sys.stderr)
        return 2
    with open(argv[1]) as f:
        config = json.load(f)
    errors = validate(config, compact(config))
    for e in errors:
        print(e)
    size = len(compact(config).encode())
    print(f"{'FAIL' if errors else 'OK'}: {len(config)} accounts, {size}/{SSM_MAX_BYTES} bytes, "
          f"{len(errors)} violation(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
