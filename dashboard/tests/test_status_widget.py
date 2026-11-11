"""Offline tests for the status widget. Run: python3 -m pytest dashboard/tests -q"""

import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import status_widget as sw  # noqa: E402

NOW = datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc)
ARN = "arn:aws:states:us-east-1:111111111111:stateMachine:{}"


def _exec(name, status, hours_ago, minutes=10):
    start = NOW - timedelta(hours=hours_ago)
    e = {
        "executionArn": f"arn:aws:states:us-east-1:111111111111:execution:sm:{name}",
        "name": name,
        "status": status,
        "startDate": start,
    }
    if status != "RUNNING":
        e["stopDate"] = start + timedelta(minutes=minutes)
    return e


MACHINES = [
    "cartography-lambda-sync",
    "cartography-lambda-orchestrator",
    "cartography-lambda-daily-controller",
    "cartography-okta-lambda-sync",
    "cartography-okta-lambda-orchestrator",
    "cartography-kandji-lambda-sync",
    "cartography-newthing-lambda-sync",  # a loader added later: must appear automatically
    "cartography-aws-sync",  # old EC2 path: must NOT appear
    "unrelated-machine",
]

EXECUTIONS = {
    "cartography-lambda-sync": [
        _exec("orch-1-222222222222-4-1", "FAILED", 2),
        _exec("orch-1-333333333333-0-1", "SUCCEEDED", 3),
        _exec("orch-1-444444444444-0-1", "SUCCEEDED", 4),
        _exec("old", "FAILED", 100),  # outside window
    ],
    "cartography-lambda-daily-controller": [_exec("ctl", "SUCCEEDED", 10, minutes=62)],
    "cartography-lambda-orchestrator": [_exec("orch", "SUCCEEDED", 10)],
    "cartography-okta-lambda-sync": [_exec("okta-1", "RUNNING", 0.2)],
    "cartography-okta-lambda-orchestrator": [_exec("o", "RUNNING", 0.3)],
    "cartography-kandji-lambda-sync": [_exec("k", "SUCCEEDED", 80)],  # stale
    "cartography-newthing-lambda-sync": [],
}


class FakePaginator:
    def __init__(self, op):
        self.op = op

    def paginate(self, **kwargs):
        if self.op == "list_state_machines":
            yield {"stateMachines": [{"name": n, "stateMachineArn": ARN.format(n)} for n in MACHINES]}
        else:
            name = kwargs["stateMachineArn"].rsplit(":", 1)[1]
            yield {"executions": EXECUTIONS.get(name, [])}


class FakeSfn:
    def get_paginator(self, op):
        return FakePaginator(op)

    def list_executions(self, stateMachineArn, maxResults):
        name = stateMachineArn.rsplit(":", 1)[1]
        return {"executions": EXECUTIONS.get(name, [])[:maxResults]}


@pytest.fixture
def rendered():
    event = {"widgetContext": {"timeRange": {"start": int((NOW - timedelta(hours=36)).timestamp() * 1000)}}}
    with patch.object(sw, "_sfn", FakeSfn()), patch.object(sw, "datetime") as dt:
        dt.now.return_value = NOW
        dt.fromtimestamp.side_effect = datetime.fromtimestamp
        yield sw.handler(event, None)


def test_discovers_loaders_by_name():
    rows = sw._loader_rows({n: ARN.format(n) for n in MACHINES})
    assert [r["loader"] for r in rows] == ["aws", "kandji", "newthing", "okta"]
    by = {r["loader"]: r for r in rows}
    assert by["aws"]["parent"][0] == "cartography-lambda-daily-controller"
    assert by["okta"]["parent"][0] == "cartography-okta-lambda-orchestrator"
    assert by["kandji"]["parent"] is None


def test_health_uses_child_failures_not_orchestrator(rendered):
    aws_row = rendered.split("<b>aws</b>")[1].split("</tr>")[0]
    assert "FAILED" in aws_row  # a child failed even though the controller SUCCEEDED
    assert "orch-1-222222222222-4-1" in aws_row
    assert "old" not in aws_row  # outside the window


def test_running_stale_and_never(rendered):
    assert "RUNNING" in rendered.split("<b>okta</b>")[1].split("</tr>")[0]
    assert "STALE" in rendered.split("<b>kandji</b>")[1].split("</tr>")[0]
    assert "NEVER RUN" in rendered.split("<b>newthing</b>")[1].split("</tr>")[0]
    assert "cartography-aws-sync" not in rendered


def test_failures_win_over_running():
    saved = list(EXECUTIONS["cartography-lambda-sync"])
    EXECUTIONS["cartography-lambda-sync"].insert(0, _exec("still-going", "RUNNING", 0.1))
    try:
        event = {"widgetContext": {"timeRange": {"start": int((NOW - timedelta(hours=36)).timestamp() * 1000)}}}
        with patch.object(sw, "_sfn", FakeSfn()), patch.object(sw, "datetime") as dt:
            dt.now.return_value = NOW
            dt.fromtimestamp.side_effect = datetime.fromtimestamp
            out = sw.handler(event, None)
    finally:
        EXECUTIONS["cartography-lambda-sync"] = saved
    health = out.split("<b>aws</b>")[1].split("</td>")[1]
    assert "FAILED" in health and "(running)" in health


def test_describe():
    assert "lambda-sync" in sw.handler({"describe": True}, None)


def test_truncation_marks_counts():
    many = [_exec(f"e{i}", "SUCCEEDED", 1) for i in range(5)]
    with patch.object(sw, "MAX_EXECUTIONS", 3), patch.object(sw, "_sfn", FakeSfn()):
        EXECUTIONS["cartography-kandji-lambda-sync"], saved = many, EXECUTIONS["cartography-kandji-lambda-sync"]
        try:
            found, truncated = sw._executions(ARN.format("cartography-kandji-lambda-sync"), NOW - timedelta(hours=36))
        finally:
            EXECUTIONS["cartography-kandji-lambda-sync"] = saved
    assert len(found) == 3 and truncated
