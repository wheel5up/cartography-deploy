"""
Tests for src/splits.py (account-splits rules). Standard library only.

  python3 -m pytest tests/test_splits.py -q

test_default_syncs_matches_upstream_order needs cartography importable and is
skipped otherwise.
"""

import json
import sys
from pathlib import Path

import pytest

STACK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(STACK_DIR / "src"))

import splits as s  # noqa: E402

A = "111111111111"
PR = "permission_relationships"


def errs(splits_list):
    return s.validate({A: {"splits": splits_list}})


def test_whole_default_is_valid():
    assert errs([{}]) == []
    assert errs([{"include": [], "exclude": []}]) == []


def test_tail_split_is_valid():
    assert errs([{"exclude": ["inspector", PR]}, {"include": ["inspector"]}, {"include": [PR]}]) == []


def test_tail_split_without_moving_pr_fails():
    """Catch-all runs permission_relationships, then inspector runs after it."""
    e = errs([{"exclude": ["inspector"]}, {"include": ["inspector"]}])
    assert any("must run last" in x for x in e)


def test_contiguous_cut_is_valid():
    d = s.DEFAULT_SYNCS
    assert errs([{"include": d[:20]}, {"include": d[20:]}]) == []


def test_may_run_late_after_catch_all_is_valid():
    late = sorted(s.MAY_RUN_LATE)
    assert errs([{"exclude": late + [PR]}, {"include": late}, {"include": [PR]}]) == []


def test_moving_a_depended_on_module_late_fails():
    e = errs([{"exclude": ["ec2:instance"]}, {"include": ["ec2:instance"]}])
    assert any("'ec2:instance'" in x and "comes first" in x for x in e)


def test_moving_a_module_early_fails():
    e = errs([{"include": ["ecs"]}, {"exclude": ["ecs"]}])
    assert any("comes first" in x for x in e)


def test_permission_relationships_must_be_last():
    e = errs([{"exclude": [PR]}, {"include": [PR]}, {"include": ["inspector"]}])
    # inspector would run twice too; isolate the rule-3 message
    assert any("must run last" in x for x in e)


def test_missing_and_repeated():
    e = errs([{"exclude": ["kms"]}])
    assert any("never runs ['kms']" in x for x in e)
    e = errs([{}, {"include": ["kms"]}])
    assert any("more than once ['kms']" in x for x in e)


def test_unknown_module_and_both_keys():
    assert any("unknown module" in x for x in errs([{"exclude": ["nope"]}, {"include": ["nope"]}]))
    assert any("both 'include' and 'exclude'" in x for x in errs([{"include": ["iam"], "exclude": ["kms"]}]))


def test_extra_modules_allowed_in_include():
    assert errs([{"include": ["iam", "iam:service_access"]}, {"exclude": ["iam"]}]) == []


def test_bad_shapes():
    assert s.validate([]) != []
    assert s.validate({"123": {"splits": [{}]}}) != []
    assert s.validate({A: {"splits": []}}) != []
    assert s.validate({A: {"splits": [{"incl": []}]}}) != []


def test_size_limit():
    big = {f"{i:012d}": {"splits": [{"exclude": ["inspector"] * 1}, {"include": ["inspector"]}]} for i in range(200)}
    assert any("SSM limit" in x for x in s.validate(big))


def test_compact_drops_empty_keys():
    out = json.loads(s.compact({A: {"splits": [{"include": [], "exclude": ["inspector"]}, {"include": ["inspector"], "exclude": []}]}}))
    assert out == {A: {"splits": [{"exclude": ["inspector"]}, {"include": ["inspector"]}]}}


def test_handler_uses_this_module_list():
    src = (STACK_DIR / "src" / "handler.py").read_text()
    assert "from splits import DEFAULT_SYNCS" in src
    assert "DEFAULT_SYNCS: list[str] = [" not in src


def test_makefile_packages_splits():
    assert "src/splits.py" in (STACK_DIR / "Makefile").read_text()


def test_default_syncs_matches_upstream_order():
    """DEFAULT_SYNCS is upstream's order (minus the separate tagging pass, with
    permission_relationships moved last). Fails if an upgrade reorders it."""
    try:
        from cartography.intel.aws.resources import RESOURCE_FUNCTIONS
    except Exception:
        pytest.skip("cartography not importable")
    upstream = [m for m in RESOURCE_FUNCTIONS if m in s.DEFAULT_SYNCS and m != PR]
    ours = [m for m in s.DEFAULT_SYNCS if m != PR]
    assert ours == upstream


@pytest.mark.parametrize("name", ["account-splits.json", "account-splits.example.json"])
def test_config_file_is_valid(name):
    """account-splits.json (what `make splits-put` stores, gitignored) and
    the committed example pass every rule."""
    path = STACK_DIR / name
    if not path.exists():
        pytest.skip(f"{name} not present")
    config = json.loads(path.read_text())
    assert s.validate(config, s.compact(config)) == []
