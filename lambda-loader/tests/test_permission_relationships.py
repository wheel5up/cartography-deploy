"""
Offline tests for lambda-loader/permission_relationships.yaml and the
handler's wiring to it. No network, no Neo4j, no AWS.

Run against the vendored fork (same as github-lambda-loader):

  <cartography-venv>/bin/python -m pytest tests -q

The fork checkout should be at the Makefile's CARTOGRAPHY_GIT_REF. Override
the upstream file location with UPSTREAM_PRR_FILE if needed.
"""

import os
import re
import sys
from pathlib import Path

import pytest
import yaml

STACK_DIR = Path(__file__).resolve().parent.parent
MAPPING_FILE = STACK_DIR / "permission_relationships.yaml"

sys.path.insert(0, str(STACK_DIR / "src"))
os.environ.setdefault("NEO4J_URI", "bolt://127.0.0.1:1")

import handler as h  # noqa: E402
from cartography.intel.aws import permission_relationships as pr  # noqa: E402

UPSTREAM_FILE = Path(
    os.environ.get(
        "UPSTREAM_PRR_FILE",
        Path(pr.__file__).resolve().parents[2] / "data" / "permission_relationships.yaml",
    )
)

ACCOUNT = "111111111111"
PARAM_APP = f"arn:aws:ssm:us-east-1:{ACCOUNT}:parameter/app/db_password"
PARAM_OTHER = f"arn:aws:ssm:us-east-1:{ACCOUNT}:parameter/infra/token"
KEY = f"arn:aws:kms:us-east-1:{ACCOUNT}:key/1111-2222"


def load(path: Path) -> list[dict]:
    with open(path) as f:
        return yaml.safe_load(f)


def key(rpr: dict) -> tuple:
    """Identity of an entry, for the upstream-drift comparison."""
    return (
        rpr["target_label"],
        rpr["relationship_name"],
        tuple(sorted(p.lower() for p in rpr["permissions"])),
        repr(rpr.get("target_precondition")),
    )


def rpr_for(label: str, rel: str) -> dict:
    matches = [
        r for r in load(MAPPING_FILE)
        if r["target_label"] == label and r["relationship_name"] == rel
    ]
    assert len(matches) == 1, (label, rel, matches)
    return matches[0]


def principal(*statements: dict) -> dict:
    """One principal, one policy, compiled the way sync() compiles them."""
    return {"policy": pr.compile_statement(list(statements))}


# --- the file itself -------------------------------------------------------


def test_file_is_valid_for_the_module():
    mapping = load(MAPPING_FILE)
    assert isinstance(mapping, list) and mapping
    for rpr in mapping:
        assert pr.is_valid_rpr(rpr), rpr


def test_no_duplicate_entries():
    keys = [key(r) for r in load(MAPPING_FILE)]
    assert len(keys) == len(set(keys))


def test_every_target_label_is_a_real_node_label():
    """A typo in target_label silently maps nothing (no node matches)."""
    from cartography.models.core.nodes import CartographyNodeSchema

    import cartography.models.aws as aws_models
    import importlib
    import pkgutil

    labels: set[str] = set()
    for mod in pkgutil.walk_packages(aws_models.__path__, aws_models.__name__ + "."):
        module = importlib.import_module(mod.name)
        for obj in vars(module).values():
            if (
                isinstance(obj, type)
                and issubclass(obj, CartographyNodeSchema)
                and obj is not CartographyNodeSchema
            ):
                try:
                    schema = obj()
                except TypeError:
                    continue
                labels.add(schema.label)
                if schema.extra_node_labels:
                    labels.update(
                        lbl if isinstance(lbl, str) else lbl.label
                        for lbl in schema.extra_node_labels.labels
                    )
    missing = {r["target_label"] for r in load(MAPPING_FILE)} - labels
    assert not missing, f"target_label(s) not defined by any AWS node schema: {missing}"


def test_contains_every_upstream_entry():
    """Fails on a CARTOGRAPHY_GIT_REF bump that adds/changes an upstream entry
    we haven't copied into section 1 of our file."""
    assert UPSTREAM_FILE.is_file(), f"upstream file not found: {UPSTREAM_FILE}"
    ours = {key(r) for r in load(MAPPING_FILE)}
    missing = [r for r in load(UPSTREAM_FILE) if key(r) not in ours]
    assert not missing, (
        "Upstream entries missing from lambda-loader/permission_relationships.yaml "
        f"(re-copy section 1 from {UPSTREAM_FILE}): {missing}"
    )


def test_g2_additions_present():
    read = rpr_for("AWSSSMParameter", "CAN_READ")
    assert {p.lower() for p in read["permissions"]} == {
        "ssm:getparameter",
        "ssm:getparameters",
        "ssm:getparametersbypath",
        "ssm:getparameterhistory",
    }
    rpr_for("AWSSSMParameter", "CAN_WRITE")
    assert rpr_for("AWSKMSKey", "CAN_DECRYPT")["permissions"] == ["kms:Decrypt"]


def test_makefile_packages_the_file():
    makefile = (STACK_DIR / "Makefile").read_text()
    assert re.search(
        r"^\s*cp permission_relationships\.yaml \"\$\(ARTIFACTS_DIR\)/\"",
        makefile,
        re.MULTILINE,
    ), "Makefile no longer copies permission_relationships.yaml into the build"


# --- handler wiring --------------------------------------------------------


def test_handler_resolves_repo_file():
    """From the repo, the handler finds lambda-loader/permission_relationships.yaml."""
    if "PERMISSION_RELATIONSHIPS_FILE" in os.environ:
        pytest.skip("overridden by env")
    assert os.path.isabs(h.PERMISSION_RELATIONSHIPS_FILE)
    assert Path(h.PERMISSION_RELATIONSHIPS_FILE) == MAPPING_FILE


def test_handler_prefers_packaged_file(tmp_path, monkeypatch):
    """Lambda layout: file next to handler.py (/var/task) wins."""
    (tmp_path / "src").mkdir()
    packaged = tmp_path / "src" / "permission_relationships.yaml"
    packaged.write_text("[]\n")
    (tmp_path / "permission_relationships.yaml").write_text("[]\n")
    monkeypatch.setattr(h, "__file__", str(tmp_path / "src" / "handler.py"))
    assert h._default_permission_relationships_file() == str(packaged)


def test_handler_default_when_neither_exists(tmp_path, monkeypatch):
    """Points at the packaged location so the error names the Lambda path."""
    monkeypatch.setattr(h, "__file__", str(tmp_path / "handler.py"))
    assert h._default_permission_relationships_file() == str(tmp_path / "permission_relationships.yaml")


def test_check_accepts_our_file():
    h._check_permission_relationships_file(str(MAPPING_FILE))


def test_check_rejects_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        h._check_permission_relationships_file(str(tmp_path / "nope.yaml"))


@pytest.mark.parametrize("body", ["", "[]\n", "{}\n"])
def test_check_rejects_empty_file(tmp_path, body):
    f = tmp_path / "empty.yaml"
    f.write_text(body)
    with pytest.raises(ValueError):
        h._check_permission_relationships_file(str(f))


def test_check_rejects_invalid_entry(tmp_path):
    f = tmp_path / "bad.yaml"
    f.write_text("- target_label: AWSSSMParameter\n  permissions: [ssm:GetParameter]\n")
    with pytest.raises(ValueError):
        h._check_permission_relationships_file(str(f))


def test_upstream_module_reads_our_file():
    """The module's own parser, with the path the handler passes it."""
    assert pr.parse_permission_relationships_file(str(MAPPING_FILE)) == load(MAPPING_FILE)


# --- engine behavior for the additions ------------------------------------


def edges(principals: dict, arns: list[str], rpr: dict) -> set[tuple[str, str]]:
    return {
        (m["principal_arn"], m["resource_arn"])
        for m in pr.calculate_permission_relationships(principals, arns, rpr["permissions"])
    }


def test_ssm_can_read():
    read = rpr_for("AWSSSMParameter", "CAN_READ")
    principals = {
        "scoped": principal({
            "effect": "Allow",
            "action": ["ssm:GetParameter*"],
            "resource": [f"arn:aws:ssm:us-east-1:{ACCOUNT}:parameter/app/*"],
        }),
        # Batch read only: still reads values, must still get an edge.
        "by_path": principal({
            "effect": "Allow",
            "action": ["ssm:GetParametersByPath"],
            "resource": ["*"],
        }),
        "admin": principal({"effect": "Allow", "action": ["*"], "resource": ["*"]}),
        "denied": principal(
            {"effect": "Allow", "action": ["ssm:*"], "resource": ["*"]},
            {
                "effect": "Deny",
                "action": ["ssm:GetParameter*"],
                "resource": [f"arn:aws:ssm:us-east-1:{ACCOUNT}:parameter/app/*"],
            },
        ),
        # Metadata only: must NOT count as reading.
        "describe_only": principal({
            "effect": "Allow",
            "action": ["ssm:DescribeParameters"],
            "resource": ["*"],
        }),
    }
    got = edges(principals, [PARAM_APP, PARAM_OTHER], read)
    assert got == {
        ("scoped", PARAM_APP),
        ("by_path", PARAM_APP),
        ("by_path", PARAM_OTHER),
        ("admin", PARAM_APP),
        ("admin", PARAM_OTHER),
        ("denied", PARAM_OTHER),
    }


def test_ssm_can_write():
    write = rpr_for("AWSSSMParameter", "CAN_WRITE")
    principals = {
        "writer": principal({
            "effect": "Allow",
            "action": ["ssm:PutParameter"],
            "resource": [PARAM_APP],
        }),
        "reader": principal({
            "effect": "Allow",
            "action": ["ssm:GetParameter*"],
            "resource": ["*"],
        }),
    }
    assert edges(principals, [PARAM_APP, PARAM_OTHER], write) == {("writer", PARAM_APP)}


def test_kms_can_decrypt():
    decrypt = rpr_for("AWSKMSKey", "CAN_DECRYPT")
    principals = {
        "decrypter": principal({"effect": "Allow", "action": ["kms:Decrypt"], "resource": [KEY]}),
        "kms_read": principal({
            "effect": "Allow",
            "action": ["kms:Describe*", "kms:Get*", "kms:List*"],
            "resource": ["*"],
        }),
    }
    assert edges(principals, [KEY], decrypt) == {("decrypter", KEY)}


def test_conditional_grant_is_flagged_not_dropped():
    read = rpr_for("AWSSSMParameter", "CAN_READ")
    principals = {
        "vpn_only": principal({
            "effect": "Allow",
            "action": ["ssm:GetParameter"],
            "resource": ["*"],
            "condition": '{"IpAddress": {"aws:SourceIp": "10.0.0.0/8"}}',
        }),
    }
    [m] = pr.calculate_permission_relationships(principals, [PARAM_APP], read["permissions"])
    assert m["has_condition"] is True
    assert m["condition_keys"] == ["aws:SourceIp"]
