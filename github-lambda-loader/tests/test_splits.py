"""
Offline tests for repo-split config, work-item building, and org_finalize's
coverage/safe-flag aggregation. No network, no Neo4j.

  <cartography-venv>/bin/python -m pytest tests -q
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
os.environ.setdefault("NEO4J_URI", "bolt://127.0.0.1:1")

import handler as h  # noqa: E402

TAG = 1000

UE_DEPS_SPLIT = [
    {"modules": ["deps"], "manifests": [0, 90]},
    {"modules": ["deps"], "manifests": [90, 180]},
    {"modules": ["deps"], "manifests": [180, None]},
    {"modules": ["codeowners", "webhooks", "commits"]},
    {"modules": ["actions"], "envs": "none"},
    {"modules": ["actions"], "envs": ["", "u"]},
    {"modules": ["actions"], "envs": ["u", "ue3"]},
    {"modules": ["actions"], "envs": ["ue3", None]},
]

UE_SPLIT = [
    {"modules": ["deps", "codeowners", "webhooks", "commits"]},
    {"modules": ["actions"], "envs": "none"},
    {"modules": ["actions"], "envs": ["", "m"]},
    {"modules": ["actions"], "envs": ["m", None]},
]


# ── validation ─────────────────────────────────────────────────────────────


def test_valid_ue_split():
    h._validate_parts("ue", UE_SPLIT)


def test_valid_ue_deps_split():
    h._validate_parts("ue", UE_DEPS_SPLIT)


def test_codeowners_after_whole_deps_part_ok():
    h._validate_parts("r", [
        {"modules": ["deps", "webhooks"]},
        {"modules": ["codeowners", "actions", "commits"]},
    ])


@pytest.mark.parametrize("parts,msg", [
    # codeowners before the last manifest range
    ([UE_DEPS_SPLIT[0], UE_DEPS_SPLIT[3], UE_DEPS_SPLIT[1], UE_DEPS_SPLIT[2], *UE_DEPS_SPLIT[4:]], "come after every deps part"),
    # manifest gap
    ([{"modules": ["deps"], "manifests": [0, 90]}, {"modules": ["deps"], "manifests": [100, None]}, *UE_DEPS_SPLIT[3:]], "gap/overlap"),
    # not starting at 0
    ([{"modules": ["deps"], "manifests": [5, None]}, *UE_DEPS_SPLIT[3:]], "start at 0"),
    # not ending at null
    ([{"modules": ["deps"], "manifests": [0, 90]}, *UE_DEPS_SPLIT[3:]], "end at null"),
    # mixing whole deps + ranges
    ([{"modules": ["deps"], "manifests": [0, None]}, {"modules": ["deps", "codeowners", "webhooks", "commits"]}, *UE_DEPS_SPLIT[4:]], "EITHER one whole part OR manifest-range"),
    # manifests on a non-deps part
    ([{"modules": ["deps", "commits"], "manifests": [0, None]}, *UE_DEPS_SPLIT[3:]], "deps-only part"),
    # bad range values
    ([{"modules": ["deps"], "manifests": [10, 5]}, *UE_DEPS_SPLIT[3:]], "hi>lo"),
])
def test_invalid_manifest_splits(parts, msg):
    with pytest.raises(ValueError, match=msg):
        h._validate_parts("ue", parts)


def test_valid_two_part_split_without_env_ranges():
    h._validate_parts("midgard", [
        {"modules": ["deps", "codeowners"]},
        {"modules": ["actions", "webhooks", "commits"]},
    ])


@pytest.mark.parametrize("parts,msg", [
    ([], "non-empty"),
    ([{"modules": ["deps", "codeowners", "webhooks"]}, {"modules": ["actions"]}], "'commits' covered 0"),
    ([{"modules": list(h.MODULES)}, {"modules": ["commits"]}], "'commits' covered 2"),
    # codeowners BEFORE the deps part it needs
    ([{"modules": ["codeowners", "actions"]}, {"modules": ["deps", "webhooks", "commits"]}], "come after every deps part"),
    ([{"modules": ["deps", "codeowners", "webhooks", "commits", "bogus"]}], "unknown modules"),
    ([{"modules": ["deps", "codeowners", "webhooks", "commits", "actions"], "envs": "none"}], "only allowed on an actions-only"),
    # actions whole AND env ranges
    ([{"modules": ["deps", "codeowners", "webhooks", "commits", "actions"]},
      {"modules": ["actions"], "envs": ["", None]}], "EITHER one whole part"),
    # env ranges without a "none" core part
    ([{"modules": ["deps", "codeowners", "webhooks", "commits"]},
      {"modules": ["actions"], "envs": ["", None]}], "exactly one envs=\"none\""),
    # gap
    ([*UE_SPLIT[:2], {"modules": ["actions"], "envs": ["", "m"]}, {"modules": ["actions"], "envs": ["n", None]}], "gap/overlap"),
    # overlap
    ([*UE_SPLIT[:2], {"modules": ["actions"], "envs": ["", "n"]}, {"modules": ["actions"], "envs": ["m", None]}], "gap/overlap"),
    # doesn't start at ""
    ([*UE_SPLIT[:2], {"modules": ["actions"], "envs": ["a", None]}], "start at"),
    # doesn't end at null
    ([*UE_SPLIT[:2], {"modules": ["actions"], "envs": ["", "m"]}], "end at null"),
])
def test_invalid_splits(parts, msg):
    with pytest.raises(ValueError, match=msg):
        h._validate_parts("r", parts)


# ── items ──────────────────────────────────────────────────────────────────


def test_build_items_unsplit_and_split():
    splits = h._validate_splits({"example-org": {"ue": UE_SPLIT}}, "example-org")
    items = h.build_items(["zeta", "UE", "alpha"], splits)
    assert items == [
        {"repo": "UE", "part": 0}, {"repo": "UE", "part": 1},
        {"repo": "UE", "part": 2}, {"repo": "UE", "part": 3},
        {"repo": "alpha", "part": 0},
        {"repo": "zeta", "part": 0},
    ]


def test_build_items_no_config():
    assert h.build_items(["b", "a"], {}) == [{"repo": "a", "part": 0}, {"repo": "b", "part": 0}]


def test_config_for_missing_repo_is_harmless():
    splits = h._validate_splits({"example-org": {"deleted-repo": UE_SPLIT}}, "example-org")
    assert h.build_items(["a"], splits) == [{"repo": "a", "part": 0}]


def test_part_spec_bounds():
    splits = h._validate_splits({"o": {"ue": UE_SPLIT}}, "o")
    assert h._part_spec(splits, "a", 0)["modules"] == list(h.MODULES)
    with pytest.raises(ValueError):
        h._part_spec(splits, "a", 1)
    with pytest.raises(ValueError):
        h._part_spec(splits, "ue", 4)


def test_env_in_range():
    assert h._env_in_range("Alpha", ["", "m"])
    assert not h._env_in_range("m1", ["", "m"])
    assert h._env_in_range("m1", ["m", None])
    assert h._env_in_range("zzz", ["m", None])


# ── org list ───────────────────────────────────────────────────────────────

ORGS = {"example-org": "/p/example", "other-org": "/p/other"}


@pytest.fixture
def orgs(monkeypatch):
    monkeypatch.setattr(h, "_load_orgs", lambda: dict(ORGS))


def test_load_orgs_validates(monkeypatch):
    for bad in ('[]', '{}', '{"o": ""}', '"x"'):
        monkeypatch.setattr(h, "_ssm_get", lambda name, decrypt=False, v=bad: v)
        with pytest.raises(ValueError):
            h._load_orgs()
    monkeypatch.setattr(h, "_ssm_get", lambda name, decrypt=False: '{"o": "/p"}')
    assert h._load_orgs() == {"o": "/p"}


def test_unscoped_guard(orgs):
    fin = lambda o, **kw: {"step": "org_finalize", "org": o, "status": "SUCCEEDED", "updateTag": TAG, **kw}  # noqa: E731
    h._check_all_orgs_finalized([fin("example-org"), fin("other-org")], TAG)
    for bad in ([], [fin("example-org")], [fin("example-org"), fin("other-org", updateTag=1)],
                [fin("example-org"), fin("other-org", status="FAILED")]):
        with pytest.raises(RuntimeError, match="unscoped_cleanup refused"):
            h._check_all_orgs_finalized(bad, TAG)


def test_expand_targets(monkeypatch, orgs):
    monkeypatch.setattr(h, "_load_splits", lambda: {"example-org": {"ue": UE_SPLIT}})
    r = h.handler({"step": "expand_targets", "org": "example-org", "repos": ["ue", "a"]}, None)
    assert r["status"] == "SUCCEEDED"
    assert r["items"] == [{"repo": "a", "part": 0}] + [{"repo": "ue", "part": i} for i in range(4)]


def test_expand_targets_unknown_org(monkeypatch, orgs):
    r = h.handler({"step": "expand_targets", "org": "nope", "repos": ["a"]}, None)
    assert r["status"] == "FAILED" and "Unknown org" in r["error"]


def test_expand_targets_bad_config_fails(monkeypatch, orgs):
    monkeypatch.setattr(h, "_load_splits", lambda: {"example-org": {"ue": UE_SPLIT[:2]}})
    r = h.handler({"step": "expand_targets", "org": "example-org", "repos": ["ue"]}, None)
    assert r["status"] == "FAILED" and "env-range" in r["error"]


def test_removed_list_orgs_step():
    r = h.handler({"step": "list_orgs"}, None)
    assert r["status"] == "FAILED" and "must be one of" in r["error"]


# ── coverage (expected items vs what ran vs failures) ──────────────────────


def _items(names, splits=None):
    return h.build_items(names, h._validate_splits(splits or {}, "o"))


def test_coverage_ok():
    items = _items(["a", "ue"], {"o": {"ue": UE_DEPS_SPLIT}})
    assert h._check_repo_coverage(items, items, []) == []


def test_coverage_failed_items():
    items = _items(["a", "b"])
    assert h._check_repo_coverage(items, items, ["b:0"]) == ["repo step failed: b:0"]


def test_coverage_repo_created_mid_run():
    assert h._check_repo_coverage(_items(["a", "new"]), _items(["a"]), []) == [
        "not run: new part 0 (repo created or split config changed mid-run?)"
    ]


def test_coverage_repo_deleted_mid_run():
    assert h._check_repo_coverage(_items(["a"]), _items(["a", "gone"]), []) == [
        "ran but no longer expected: gone part 0 (repo deleted or config changed mid-run?)"
    ]


def test_coverage_split_config_changed_mid_run():
    ran = _items(["ue"])
    expected = _items(["ue"], {"o": {"ue": UE_DEPS_SPLIT}})
    problems = h._check_repo_coverage(expected, ran, [])
    assert len(problems) == len(UE_DEPS_SPLIT) - 1 and all(p.startswith("not run: ue part") for p in problems)


def test_coverage_duplicates():
    items = _items(["a"])
    assert "duplicate items in the repo Map" in h._check_repo_coverage(items, items + items, [])


# ── per-repo safety flags via Neo4j ────────────────────────────────────────


class ReadSession:
    def __init__(self, unsafe_flags=(), totals=()):
        self.unsafe = list(unsafe_flags)
        self.totals = list(totals)

    def execute_read(self, fn, query, **params):
        if query is h._ORG_UNSAFE_QUERY:
            return [{"flag": f} for f in self.unsafe]
        if query is h._ORG_MANIFEST_TOTALS_QUERY:
            return self.totals
        raise AssertionError(query)


def test_safety_all_safe():
    assert h._org_repo_safety(ReadSession(), "u", TAG) == {"deps": True, "codeowners": True, "webhooks": True}


def test_safety_flags_from_neo4j():
    assert h._org_repo_safety(ReadSession(["deps", "webhooks"]), "u", TAG) == {
        "deps": False, "codeowners": True, "webhooks": False,
    }


def test_safety_manifest_total_drift_disables_deps():
    s = ReadSession(totals=[{"repo": "ue", "totals": [251, 252]}])
    assert h._org_repo_safety(s, "u", TAG)["deps"] is False


def test_unsafe_merge_query_semantics():
    q = h._MARK_UNSAFE_QUERY
    # same tag: merge (dedup) with existing; newer tag: replace
    assert "r.g2_unsafe_tag = $update_tag" in q and "+ $flags" in q and "ELSE $flags" in q


# ── skip / retry / carry-forward (fake GitHub) ─────────────────────────────


class FakeGitHub:
    """Fakes _list_manifest_paths + _fetch_one_manifest. fail_plan: pos -> # of failures."""

    def __init__(self, n, fail_plan=None):
        self.paths = [f"/o/r/blob/main/m{i}/package.json" for i in range(n)]
        self.fail_plan = dict(fail_plan or {})
        self.calls = []

    def list_paths(self, token, owner, repo):
        return list(self.paths)

    def fetch_one(self, token, org, repo, pos, expected):
        assert expected == self.paths[pos]
        self.calls.append(pos)
        if self.fail_plan.get(pos, 0) > 0:
            self.fail_plan[pos] -= 1
            return None
        return {"blobPath": self.paths[pos], "dependencies": {"nodes": []}}


def _patch(monkeypatch, fake):
    monkeypatch.setattr(h, "_list_manifest_paths", fake.list_paths)
    monkeypatch.setattr(h, "_fetch_one_manifest", fake.fetch_one)


def test_range_all_succeed(monkeypatch):
    fake = FakeGitHub(10); _patch(monkeypatch, fake)
    r = h._get_manifest_range(None, "o", "r", 0, None)
    assert len(r["manifests"]) == 10 and r["failed_paths"] == [] and r["retried"] == 0


def test_skip_continues_past_failure(monkeypatch):
    fake = FakeGitHub(10, {3: 99}); _patch(monkeypatch, fake)
    r = h._get_manifest_range(None, "o", "r", 0, None)
    assert [m["blobPath"] for m in r["manifests"]] == [p for i, p in enumerate(fake.paths) if i != 3]
    assert r["failed_paths"] == [fake.paths[3]]


def test_retry_recovers_transient_failure(monkeypatch):
    fake = FakeGitHub(10, {3: 1, 7: 1}); _patch(monkeypatch, fake)
    r = h._get_manifest_range(None, "o", "r", 0, None)
    assert len(r["manifests"]) == 10 and r["failed_paths"] == [] and r["retried"] == 2
    assert fake.calls == list(range(10)) + [3, 7]


def test_range_bounds(monkeypatch):
    fake = FakeGitHub(10); _patch(monkeypatch, fake)
    r = h._get_manifest_range(None, "o", "r", 4, 7)
    assert [m["blobPath"] for m in r["manifests"]] == fake.paths[4:7]
    assert h._get_manifest_range(None, "o", "r", 20, None)["manifests"] == []


class FakeSession:
    """Records writes; reports which manifest ids 'exist' in the graph."""

    def __init__(self, existing_ids):
        self.existing = set(existing_ids)
        self.writes = []

    def execute_write(self, fn):
        class Tx:
            def __init__(s, outer): s.outer = outer
            def run(s, q, **p):
                s.outer.writes.append((q, p))
                ids = p.get("manifest_ids", [])
                n = len([i for i in ids if i in s.outer.existing])
                class R:
                    def data(self_): return [{"carried": n}]
                return R()
        return fn(Tx(self))

    def execute_read(self, fn, query, **params):
        return [{"id": i} for i in params["ids"] if i in self.existing]


def test_carry_forward_existing_and_never_synced():
    repo = "https://github.com/o/r"
    sess = FakeSession({f"{repo}#/a"})
    carried, never = h._carry_forward(sess, repo, ["/a", "/b"], TAG)
    assert carried == 1 and never == ["/b"]
    q, p = sess.writes[0]
    assert p["update_tag"] == TAG and p["repo_url"] == repo
    assert p["manifest_ids"] == [f"{repo}#/a", f"{repo}#/b"]


def test_carry_forward_query_restamps_everything_cleanup_checks():
    q = h._CARRY_FORWARD_QUERY
    for needle in ("m.lastupdated", "hm.lastupdated", "res.lastupdated", "hd.lastupdated",
                   "d.lastupdated", "rq.lastupdated", "cm.lastupdated", "rq.manifest_path = m.blob_path"):
        assert needle in q, needle
    assert "g2_last_fetched" not in q  # carry-forward must not claim a fetch


def test_carry_forward_noop():
    sess = FakeSession(set())
    assert h._carry_forward(sess, "u", [], TAG) == (0, [])
    assert sess.writes == []


def test_fetch_one_manifest_request_error_is_manifest_failure(monkeypatch):
    import requests

    def boom(*a, **k):
        raise requests.exceptions.JSONDecodeError("Expecting value", "", 0)

    monkeypatch.setattr(h.repos, "_fetch_manifest_page", boom)
    assert h._fetch_one_manifest(None, "o", "r", 3, "/o/r/blob/main/x") is None


def test_fetch_one_manifest_dep_page_request_error(monkeypatch):
    import requests
    calls = {"n": 0}

    def page(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"data": {"organization": {"repository": {"dependencyGraphManifests": {"nodes": [
                {"blobPath": "/o/r/blob/main/x",
                 "dependencies": {"nodes": [{}], "pageInfo": {"hasNextPage": True, "endCursor": "c"}}}
            ]}}}}}
        raise requests.exceptions.ConnectionError("reset")

    monkeypatch.setattr(h.repos, "_fetch_manifest_page", page)
    assert h._fetch_one_manifest(None, "o", "r", 0, "/o/r/blob/main/x") is None
