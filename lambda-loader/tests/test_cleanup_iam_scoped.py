"""
Offline tests for handler._cleanup_iam_scoped(), the account-scoped
replacement for upstream cartography.intel.aws.iam.cleanup_iam(). No Neo4j.
The live health-check query is in README.md "Account-scoped managed-policy
cleanup".

  <cartography-venv>/bin/python -m pytest tests -q
"""

import inspect
import os
import re
import sys
from pathlib import Path
from unittest import mock

STACK_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(STACK_DIR / "src"))
os.environ.setdefault("NEO4J_URI", "bolt://127.0.0.1:1")

import handler as h  # noqa: E402
from cartography.graph.cleanupbuilder import build_cleanup_queries  # noqa: E402
from cartography.intel.aws import iam  # noqa: E402


def upstream_cleanup_sequence() -> list[str]:
    """Schema class names in upstream cleanup_iam(), in call order."""
    src = inspect.getsource(iam.cleanup_iam)
    return re.findall(r"GraphJob\.from_node_schema\(\s*(\w+)\(\)", src)


def test_upstream_sequence_unchanged():
    """Fails on a fork-ref bump that changes upstream cleanup_iam(). Then:
    update _cleanup_iam_scoped() and CLEANUP_IAM_UPSTREAM_SEQUENCE to match,
    or drop the override if upstream scoped the managed-policy job."""
    assert tuple(upstream_cleanup_sequence()) == h.CLEANUP_IAM_UPSTREAM_SEQUENCE


def test_upstream_managed_policy_job_is_still_unscoped():
    """The reason this override exists. If this fails, upstream may have fixed
    it: check, and drop _cleanup_iam_scoped() if so."""
    queries = build_cleanup_queries(iam.AWSManagedPolicySchema())
    assert queries, "no cleanup queries for AWSManagedPolicySchema"
    for q in queries:
        assert "AWS_ID" not in q
        assert "AWSAccount" not in q


def test_scoped_query_is_account_scoped():
    q = " ".join(h.SCOPED_MANAGED_POLICY_EDGE_CLEANUP.split())
    assert q.startswith(
        "MATCH (:AWSAccount {id: $AWS_ID})-[:RESOURCE]->(:AWSPrincipal)-[r:POLICY]->(:AWSManagedPolicy)"
    )
    assert "WHERE r.lastupdated <> $UPDATE_TAG" in q
    assert "WITH r LIMIT $LIMIT_SIZE" in q
    assert q.endswith("DELETE r")
    # Edges only, never the shared policy node.
    assert "DETACH" not in q and "DELETE n" not in q


def test_runs_upstream_jobs_in_order_with_scoped_managed_policy_job():
    """Drive _cleanup_iam_scoped() with GraphJob mocked out; record the jobs."""
    calls: list[str] = []

    class FakeJob:
        def __init__(self, name, statements, short_name=None):
            self.name, self.statements = name, statements

        @classmethod
        def from_node_schema(cls, schema, params, **kw):
            calls.append(type(schema).__name__)
            if type(schema).__name__ == "AWSPolicyStatementSchema":
                assert params["POLICY_ID"] in ("p1", "p2")
            return cls(type(schema).__name__, [])

        def run(self, session):
            if self.name.startswith("Cleanup AWSManagedPolicy"):
                calls.append("SCOPED")
                [stmt] = self.statements
                assert stmt.query == h.SCOPED_MANAGED_POLICY_EDGE_CLEANUP
                assert stmt.parameters["AWS_ID"] == "111111111111"
                assert stmt.parameters["UPDATE_TAG"] == 42
                assert stmt.iterative

    with mock.patch("cartography.graph.job.GraphJob", FakeJob), mock.patch.object(
        iam, "_get_policies_in_current_account", return_value=["p1", "p2"]
    ):
        h._cleanup_iam_scoped(None, {"AWS_ID": "111111111111", "UPDATE_TAG": 42})

    expected = list(h.CLEANUP_IAM_UPSTREAM_SEQUENCE)
    expected = ["AWSPolicyStatementSchema"] * 2 + [
        "SCOPED" if s == "AWSManagedPolicySchema" else s for s in expected[1:]
    ]
    assert calls == expected
    assert "AWSManagedPolicySchema" not in calls


def test_iam_core_sync_uses_scoped_cleanup():
    """Call-level, not text-level: run _iam_core_sync with everything mocked."""
    called: list[str] = []
    names = [
        "get_account_authorization_details", "sync_root_principal", "sync_users",
        "sync_groups", "sync_roles", "sync_assumerole_relationships",
        "sync_user_access_keys", "get_saml_providers", "load_saml_providers",
        "sync_server_certificates", "sync_account_summary",
    ]
    patches = [mock.patch.object(iam, n, mock.MagicMock()) for n in names]
    patches += [
        mock.patch.object(iam, "cleanup_iam", side_effect=lambda *a, **k: called.append("upstream")),
        mock.patch.object(h, "_cleanup_iam_scoped", side_effect=lambda *a, **k: called.append("scoped")),
        mock.patch("cartography.util.merge_module_sync_metadata", mock.MagicMock()),
        mock.patch("cartography.intel.aws.util.arns.get_account_partition", return_value="aws"),
    ]
    for p in patches:
        p.start()
    try:
        session = mock.MagicMock()
        h._iam_core_sync(session, mock.MagicMock(region_name="us-east-1"), ["us-east-1"],
                         "111111111111", 42, {"AWS_ID": "111111111111", "UPDATE_TAG": 42})
    finally:
        for p in patches:
            p.stop()
    assert called == ["scoped"]


def test_permission_relationships_runs_last_by_default():
    assert h.DEFAULT_SYNCS[-1] == "permission_relationships"
    assert h.DEFAULT_SYNCS.count("permission_relationships") == 1
