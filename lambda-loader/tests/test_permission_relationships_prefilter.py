"""
Offline tests for handler._permission_relationships_prefiltered_sync() and
_prune_principals_for_permissions(). No Neo4j, no AWS.

The prefilter must not change output: for every RPR entry, pruned and
unpruned evaluation produce the same edges with the same condition metadata.

  <cartography-venv>/bin/python -m pytest tests -q

Optional real-data check (one account's policies, dumped from the graph):
  PRR_REAL_DUMP=/tmp/dev_dump.json <cartography-venv>/bin/python -m pytest tests -q -k real
Dump format: {"rows": [{"p": principal_arn, "pol": policy_id, "st": [statement, ...]}],
              "targets": {label: [arn, ...]}}
"""

import copy
import inspect
import json
import os
import random
import re
import sys
from pathlib import Path
from unittest import mock

import pytest
import yaml

STACK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(STACK_DIR / "src"))
os.environ.setdefault("NEO4J_URI", "bolt://127.0.0.1:1")

import handler as h  # noqa: E402
from cartography.intel.aws import permission_relationships as pr  # noqa: E402

MAPPING = yaml.safe_load(open(STACK_DIR / "permission_relationships.yaml"))
ACCT = "111111111111"
SSM = [f"arn:aws:ssm:us-east-1:{ACCT}:parameter/{p}" for p in ("app/db", "app/key", "infra/token", "x")]
KMS = [f"arn:aws:kms:us-east-1:{ACCT}:key/k1"]


def compile_principals(raw: dict) -> dict:
    """{principal: {policy: [raw statements]}} -> compiled, as sync() does."""
    return {
        p: {pol: pr.compile_statement(copy.deepcopy(sts)) for pol, sts in pols.items()}
        for p, pols in raw.items()
    }


def edges(principals: dict, arns: list[str], perms: list[str]) -> list[tuple]:
    return sorted(
        (m["principal_arn"], m["resource_arn"], m["has_condition"],
         tuple(m["condition_keys"]), m["conditions"])
        for m in pr.calculate_permission_relationships(principals, arns, perms)
    )


def assert_equivalent(principals: dict, arns: list[str], perms: list[str]) -> None:
    before = copy.deepcopy(list(principals))
    full = edges(principals, arns, perms)
    pruned = edges(h._prune_principals_for_permissions(principals, perms), arns, perms)
    assert pruned == full
    assert list(principals) == before  # input not mutated


def S(effect, action=None, resource=None, notaction=None, notresource=None, condition=None):
    s = {"effect": effect}
    if action is not None:
        s["action"] = action
    if notaction is not None:
        s["notaction"] = notaction
    if resource is not None:
        s["resource"] = resource
    if notresource is not None:
        s["notresource"] = notresource
    if condition is not None:
        s["condition"] = condition
    return s


SSM_READ = next(r for r in MAPPING if r["target_label"] == "AWSSSMParameter" and r["relationship_name"] == "CAN_READ")


# --- hand-built edge cases -------------------------------------------------

CASES = {
    "unrelated principal is dropped, no edges lost": {
        "s3only": {"p": [S("Allow", ["s3:GetObject"], ["*"])]},
        "reader": {"p": [S("Allow", ["ssm:GetParameter"], ["*"])]},
    },
    "deny in another policy still blocks": {
        "r": {
            "allow": [S("Allow", ["ssm:*"], ["*"])],
            "deny": [S("Deny", ["ssm:GetParameter*"], [f"arn:aws:ssm:us-east-1:{ACCT}:parameter/app/*"])],
        },
    },
    "deny-only principal is dropped (nothing to grant)": {
        "d": {"p": [S("Deny", ["ssm:*"], ["*"])]},
    },
    "irrelevant deny does not block": {
        "r": {"p": [S("Allow", ["ssm:GetParameter"], ["*"]), S("Deny", ["s3:*"], ["*"])]},
    },
    "notaction allow grants": {
        "na": {"p": [S("Allow", notaction=["iam:*"], resource=["*"])]},
    },
    "notaction that excludes ssm does not grant": {
        "na": {"p": [S("Allow", notaction=["ssm:*"], resource=["*"])]},
    },
    "notaction deny blocks": {
        "r": {"p": [S("Allow", ["*"], ["*"]), S("Deny", notaction=["s3:*"], resource=["*"])]},
    },
    "statement with no action clause": {
        "weird": {"p": [S("Allow", resource=["*"])]},
    },
    "notresource": {
        "nr": {"p": [S("Allow", ["ssm:GetParameters"], notresource=[f"arn:aws:ssm:us-east-1:{ACCT}:parameter/infra/*"])]},
    },
    "conditional and unconditional paths": {
        "c": {"p": [S("Allow", ["ssm:GetParameter"], ["*"], condition='{"Bool": {"aws:MultiFactorAuthPresent": "true"}}')]},
        "mixed": {
            "a": [S("Allow", ["ssm:GetParameter"], ["*"], condition='{"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}}')],
            "b": [S("Allow", ["ssm:GetParametersByPath"], [f"arn:aws:ssm:us-east-1:{ACCT}:parameter/app/*"])],
        },
    },
    "conditional statement ignored when its action is irrelevant": {
        "c": {"p": [
            S("Allow", ["ssm:GetParameter"], ["*"]),
            S("Allow", ["s3:GetObject"], ["*"], condition='{"Bool": {"aws:SecureTransport": "true"}}'),
        ]},
    },
    "wildcards and case": {
        "w": {"p": [S("Allow", ["SSM:get*"], ["arn:aws:ssm:*:*:parameter/app/*"])]},
        "q": {"p": [S("Allow", ["ssm:GetParamete?"], ["*"])]},
    },
}


@pytest.mark.parametrize("name", sorted(CASES))
def test_equivalent_on_edge_cases(name):
    principals = compile_principals(CASES[name])
    for rpr in MAPPING:
        arns = KMS if rpr["target_label"] == "AWSKMSKey" else SSM
        assert_equivalent(principals, arns, rpr["permissions"])


def test_prune_actually_prunes():
    principals = compile_principals({
        "s3only": {"p": [S("Allow", ["s3:GetObject"], ["*"])]},
        "reader": {"p": [S("Allow", ["ssm:GetParameter"], ["*"]), S("Allow", ["ec2:*"], ["*"])],
                   "other": [S("Allow", ["logs:*"], ["*"])]},
        "denyonly": {"p": [S("Deny", ["ssm:*"], ["*"])]},
    })
    pruned = h._prune_principals_for_permissions(principals, SSM_READ["permissions"])
    assert set(pruned) == {"reader"}
    assert list(pruned["reader"]) == ["p"]
    assert len(pruned["reader"]["p"]) == 1


# --- randomized ------------------------------------------------------------

ACTIONS = ["*", "ssm:*", "ssm:Get*", "ssm:GetParameter", "ssm:GetParameters", "ssm:GetParametersByPath",
           "ssm:GetParameterHistory", "ssm:PutParameter", "ssm:Delete*", "ssm:DescribeParameters",
           "kms:Decrypt", "kms:*", "kms:Describe*", "s3:GetObject", "s3:*", "iam:PassRole", "ec2:Describe*",
           "dynamodb:Query", "secretsmanager:GetSecretValue", "logs:*"]
RESOURCES = ["*", f"arn:aws:ssm:us-east-1:{ACCT}:parameter/app/*", "arn:aws:ssm:*:*:parameter/infra/*",
             SSM[0], SSM[3], KMS[0], "arn:aws:kms:*:*:key/*", "arn:aws:s3:::bucket/*"]
CONDS = [None, None, None, '{"Bool": {"aws:MultiFactorAuthPresent": "true"}}',
         '{"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}}']


def random_statement(rng: random.Random) -> dict:
    kind = rng.random()
    s = {"effect": "Deny" if rng.random() < 0.15 else "Allow"}
    if kind < 0.8:
        s["action"] = rng.sample(ACTIONS, rng.randint(1, 3))
    elif kind < 0.95:
        s["notaction"] = rng.sample(ACTIONS, rng.randint(1, 2))
    if rng.random() < 0.9:
        s["resource"] = rng.sample(RESOURCES, rng.randint(1, 2))
    else:
        s["notresource"] = rng.sample(RESOURCES, 1)
    c = rng.choice(CONDS)
    if c:
        s["condition"] = c
    return s


@pytest.mark.parametrize("seed", range(40))
def test_equivalent_on_random_policies(seed):
    rng = random.Random(seed)
    raw = {
        f"arn:aws:iam::{ACCT}:role/r{i}": {
            f"pol{j}": [random_statement(rng) for _ in range(rng.randint(1, 4))]
            for j in range(rng.randint(1, 3))
        }
        for i in range(25)
    }
    principals = compile_principals(raw)
    for rpr in MAPPING:
        arns = KMS if rpr["target_label"] == "AWSKMSKey" else SSM
        assert_equivalent(principals, arns, rpr["permissions"])


# --- real data (opt-in) ----------------------------------------------------

@pytest.mark.skipif(not os.environ.get("PRR_REAL_DUMP"), reason="set PRR_REAL_DUMP to a graph dump")
def test_equivalent_on_real_dump():
    d = json.load(open(os.environ["PRR_REAL_DUMP"]))
    principals: dict = {}
    for r in d["rows"]:
        principals.setdefault(r["p"], {})[r["pol"]] = pr.compile_statement(r["st"])
    checked = 0
    for rpr in MAPPING:
        arns = d["targets"].get(rpr["target_label"])
        if arns is None:
            continue
        assert_equivalent(principals, arns, rpr["permissions"])
        checked += 1
    assert checked >= 5


# --- wiring and drift ------------------------------------------------------

def test_dispatch_uses_prefiltered_sync():
    with mock.patch("cartography.intel.aws.resources.RESOURCE_FUNCTIONS", {"permission_relationships": pr.sync}):
        fns = h._build_sync_functions()
    assert fns["permission_relationships"] is h._permission_relationships_prefiltered_sync


def test_upstream_sync_shape_unchanged():
    """Fails on a fork-ref bump that changes upstream sync(). Then re-check
    _permission_relationships_prefiltered_sync() against it."""
    src = inspect.getsource(pr.sync)
    calls = re.findall(
        r"\b(get_principals_for_account|parse_permission_relationships_file|is_valid_rpr|"
        r"get_resource_arns|calculate_permission_relationships|load_principal_mappings|cleanup_rpr)\(",
        src,
    )
    assert calls == [
        "get_principals_for_account", "parse_permission_relationships_file", "is_valid_rpr",
        "get_resource_arns", "calculate_permission_relationships", "load_principal_mappings",
        "cleanup_rpr",
    ]
    # Engine still reads only these statement keys (what the prefilter reasons about).
    engine = inspect.getsource(pr.statement_applies_to_permission)
    assert "evaluate_notaction_for_permission" in engine
    assert "evaluate_action_for_permission" in engine


def test_prefiltered_sync_calls_upstream_pieces_in_order():
    calls: list[str] = []
    principals = compile_principals({
        "reader": {"p": [S("Allow", ["ssm:GetParameter"], ["*"])]},
        "s3only": {"p": [S("Allow", ["s3:GetObject"], ["*"])]},
    })
    rpr = {"target_label": "AWSSSMParameter", "permissions": ["ssm:GetParameter"], "relationship_name": "CAN_READ"}
    real_calculate = pr.calculate_permission_relationships  # captured before patching

    def rec(name, ret=None):
        def f(*a, **k):
            calls.append(name)
            if name == "calculate_permission_relationships":
                assert set(a[0]) == {"reader"}  # pruned set passed in
                return real_calculate(*a, **k)
            return ret
        return f

    patches = {
        "get_principals_for_account": rec("get_principals_for_account", principals),
        "parse_permission_relationships_file": rec("parse_permission_relationships_file", [rpr]),
        "get_resource_arns": rec("get_resource_arns", [SSM[0]]),
        "calculate_permission_relationships": rec("calculate_permission_relationships"),
        "load_principal_mappings": rec("load_principal_mappings"),
        "cleanup_rpr": rec("cleanup_rpr"),
    }
    with mock.patch.multiple(pr, **patches):
        h._permission_relationships_prefiltered_sync(
            None, None, [], ACCT, 42, {"permission_relationships_file": "x.yaml"}
        )
    assert calls == [
        "get_principals_for_account", "parse_permission_relationships_file",
        "get_resource_arns", "calculate_permission_relationships",
        "load_principal_mappings", "cleanup_rpr",
    ]
