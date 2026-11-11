"""CloudWatch custom widget: last run of every Cartography Lambda sync.

Discovers loaders by name, so a new loader appears with no dashboard change:
every state machine named ``cartography-*lambda-sync`` is one row. When the
same loader also has an orchestrator (``...-lambda-orchestrator``) or the AWS
daily controller, that run is shown too, but the row's health comes from the
sync machine: orchestrators report SUCCEEDED even when accounts fail, while
``*-lambda-sync`` ends in a real Fail state.

For each loader it reports, over the dashboard's time range:
- the last sync execution (status, start, duration)
- succeeded / failed / timed out / aborted counts
- the newest failed execution names (account and split for AWS)

Read-only: ListStateMachines, ListExecutions. No Neo4j, no secrets.
"""

import html
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

_sfn = boto3.client("stepfunctions", config=Config(retries={"max_attempts": 8, "mode": "adaptive"}))

SYNC_PATTERN = re.compile(os.environ.get("SYNC_PATTERN", r"^cartography-(?:(?P<name>[a-z0-9-]+)-)?lambda-sync$"))
DEFAULT_LOOKBACK_HOURS = int(os.environ.get("DEFAULT_LOOKBACK_HOURS", "36"))
# Bound per-machine paging so a loader with thousands of child runs (AWS: one
# per account split) can't make the widget time out. Counts say "N+" when hit.
MAX_EXECUTIONS = int(os.environ.get("MAX_EXECUTIONS_PER_MACHINE", "1000"))
MAX_FAILED_SHOWN = 5

_STATUS_STYLE = {
    "SUCCEEDED": ("#037f0c", "OK"),
    "FAILED": ("#d13212", "FAILED"),
    "TIMED_OUT": ("#d13212", "TIMED OUT"),
    "ABORTED": ("#8d6605", "ABORTED"),
    "RUNNING": ("#0972d3", "RUNNING"),
    "PENDING_REDRIVE": ("#8d6605", "REDRIVE"),
}


def _state_machines():
    machines = {}
    for page in _sfn.get_paginator("list_state_machines").paginate():
        for sm in page["stateMachines"]:
            machines[sm["name"]] = sm["stateMachineArn"]
    return machines


def _loader_rows(machines):
    """One row per *-lambda-sync, with its orchestrator/controller if present."""
    rows = []
    for name, arn in machines.items():
        match = SYNC_PATTERN.match(name)
        if not match:
            continue
        loader = match.group("name") or "aws"
        prefix = name[: -len("lambda-sync")]
        parent = None
        # Prefer the controller (AWS: wraps the orchestrator plus the tagging sweep).
        for candidate in (f"{prefix}lambda-orchestrator", f"{prefix}lambda-daily-controller"):
            if candidate in machines:
                parent = (candidate, machines[candidate])
        rows.append({"loader": loader, "sync": (name, arn), "parent": parent})
    return sorted(rows, key=lambda r: r["loader"])


def _executions(arn, since):
    """Executions started at or after `since`, newest first, capped."""
    found, truncated = [], False
    for page in _sfn.get_paginator("list_executions").paginate(
        stateMachineArn=arn, PaginationConfig={"PageSize": 100}
    ):
        for execution in page["executions"]:
            if execution["startDate"] < since:
                return found, truncated
            found.append(execution)
            if len(found) >= MAX_EXECUTIONS:
                return found, True
    return found, truncated


def _summarise(row, since):
    name, arn = row["sync"]
    executions, truncated = _executions(arn, since)
    counts = {}
    for execution in executions:
        counts[execution["status"]] = counts.get(execution["status"], 0) + 1
    failed = [e for e in executions if e["status"] in ("FAILED", "TIMED_OUT", "ABORTED")]
    last = executions[0] if executions else _latest_ever(arn)
    parent_last = _latest_ever(row["parent"][1]) if row["parent"] else None
    return {
        **row,
        "counts": counts,
        "truncated": truncated,
        "failed": failed[:MAX_FAILED_SHOWN],
        "failed_total": len(failed),
        "last": last,
        "parent_last": parent_last,
    }


def _latest_ever(arn):
    page = _sfn.list_executions(stateMachineArn=arn, maxResults=1)
    return page["executions"][0] if page["executions"] else None


def _duration(execution):
    if not execution:
        return ""
    end = execution.get("stopDate") or datetime.now(timezone.utc)
    seconds = int((end - execution["startDate"]).total_seconds())
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m{secs:02d}s"


def _when(execution, now):
    if not execution:
        return "never"
    age = now - execution["startDate"]
    if age < timedelta(hours=1):
        ago = f"{int(age.total_seconds() // 60)}m ago"
    elif age < timedelta(days=2):
        ago = f"{int(age.total_seconds() // 3600)}h ago"
    else:
        ago = f"{age.days}d ago"
    return f"{execution['startDate']:%m-%d %H:%M}Z ({ago})"


def _badge(status):
    color, label = _STATUS_STYLE.get(status, ("#5f6b7a", status or "NONE"))
    return f'<b style="color:{color}">{html.escape(label)}</b>'


def _console_link(arn, label):
    region = arn.split(":")[3]
    url = f"https://{region}.console.aws.amazon.com/states/home?region={region}#/v2/executions/details/{arn}"
    return f'<a href="{url}" target="_blank">{html.escape(label)}</a>'


def _render(summaries, since, now):
    out = [
        "<table>",
        "<tr><th>Loader</th><th>Health</th><th>Last sync run</th><th>Duration</th>"
        "<th>OK</th><th>Failed</th><th>Orchestrator / controller</th><th>Recent failures</th></tr>",
    ]
    for s in summaries:
        counts = s["counts"]
        bad = s["failed_total"]
        plus = "+" if s["truncated"] else ""
        if bad:
            # Failures win over RUNNING: a long AWS run can be mid-flight with
            # accounts already failed, and that is what needs attention.
            health = _badge("FAILED") + (" (running)" if counts.get("RUNNING") else "")
        elif counts.get("RUNNING"):
            health = _badge("RUNNING")
        elif not s["last"]:
            health = '<b style="color:#5f6b7a">NEVER RUN</b>'
        elif not counts:
            health = '<b style="color:#8d6605">STALE</b>'
        else:
            health = _badge("SUCCEEDED")
        last = s["last"]
        parent = s["parent_last"]
        parent_cell = (
            f"{_badge(parent['status'])} {_when(parent, now)}, {_duration(parent)}" if parent else ""
        )
        failures = "<br>".join(
            _console_link(e["executionArn"], e["name"]) + f" ({e['status'].lower()})" for e in s["failed"]
        )
        if bad > len(s["failed"]):
            failures += f"<br>... {bad - len(s['failed'])} more"
        out.append(
            "<tr>"
            f"<td><b>{html.escape(s['loader'])}</b></td>"
            f"<td>{health}</td>"
            f"<td>{_badge(last['status']) + ' ' + _when(last, now) if last else 'never'}</td>"
            f"<td>{_duration(last)}</td>"
            f"<td>{counts.get('SUCCEEDED', 0)}{plus}</td>"
            f"<td>{bad}{plus}</td>"
            f"<td>{parent_cell}</td>"
            f"<td>{failures}</td>"
            "</tr>"
        )
    out.append("</table>")
    out.append(
        f"<p><small>Window: since {since:%Y-%m-%d %H:%M}Z. Health comes from each loader's "
        "<code>*-lambda-sync</code> machine (one execution per account/split/org); orchestrators "
        "report SUCCEEDED even when children fail. STALE = no sync runs in the window. "
        f"Generated {now:%H:%M:%S}Z.</small></p>"
    )
    return "\n".join(out)


def _since(event, now):
    try:
        start = event["widgetContext"]["timeRange"]["start"]
        return datetime.fromtimestamp(start / 1000, tz=timezone.utc)
    except (KeyError, TypeError):
        return now - timedelta(hours=DEFAULT_LOOKBACK_HOURS)


def handler(event, context):
    if event.get("describe"):
        return (
            "Last run of every `cartography-*lambda-sync` state machine, discovered by name. "
            "Follows the dashboard time range."
        )
    now = datetime.now(timezone.utc)
    since = _since(event, now)
    rows = _loader_rows(_state_machines())
    if not rows:
        return "<p>No <code>cartography-*lambda-sync</code> state machines found.</p>"
    with ThreadPoolExecutor(max_workers=6) as pool:
        summaries = list(pool.map(lambda r: _summarise(r, since), rows))
    return _render(summaries, since, now)
