# Cartography Lambda syncs dashboard

One CloudWatch dashboard, `cartography-lambda-syncs`, for every Lambda
loader. Stack: `cartography-dashboard`.

```sh
cp samconfig.example.toml samconfig.toml   # then edit
sam build && sam deploy
```

The template is YAML only. `AWS::LanguageExtensions` converts the dashboard
body to the JSON string CloudWatch needs (`Fn::ToJsonString`). To copy a widget
designed in the console: widget menu → Edit → Source, then `pbpaste | yq -P`.

## What's on it

| Row | Widget | Source | New loader appears automatically? |
|---|---|---|---|
| 1 | **Last run per loader**: health, last run, duration, OK/failed counts, orchestrator timing, links to failed executions | Custom widget, `src/status_widget.py` | Yes, by name |
| 2 | Failed / succeeded sync executions per day, by loader | `AWS/States`, `SEARCH` on `lambda-sync` | Yes, after its first run |
| 3 | Longest Lambda invocation (900s line); Lambda errors + throttles | `AWS/Lambda`, `SEARCH` on `lambda-loader` | Yes, after its first run |
| 4 | End-to-end run time (orchestrators, AWS controller); Neo4j CPU | `AWS/States` `SEARCH`; `AWS/EC2` | Yes / n/a |
| 5 | Timeouts and errors by loader; skipped or unavailable surfaces; latest errors | Logs Insights | **No**: add the log group to `LoaderLogGroups` |

Health comes from each loader's `*-lambda-sync` state machine, which ends in a
real Fail state (one execution per account, split or org). Orchestrators and
the AWS controller report SUCCEEDED even when children fail, so they're shown
for timing only. Judging health from an orchestrator's status can hide
failing syncs for weeks.

## Adding a loader

1. Name it like the others: state machine `cartography-<name>-lambda-sync`,
   function `cartography-<name>-lambda-loader`, log group
   `/cartography/<name>-lambda-loader`. Rows 1–4 then pick it up with no change
   here (rows 2–4 after it has run once; `SEARCH` only finds metrics with
   recent data).
2. Add its log group to the `LoaderLogGroups` default in `template.yaml` and
   redeploy. Dashboard log widgets can't select log groups by prefix, so this
   is the one list in the stack.

## Cost

- Dashboard: first 3 per account free, then about $3/month.
- Metric widgets: free (AWS-published metrics).
- Status widget Lambda: runs only when the dashboard loads or refreshes; fractions of a cent.
- **Logs Insights widgets: about $0.005 per GB scanned, per refresh.** The cost
  scales with log volume times the time range times refreshes. Keep the range
  short (default 36h) and avoid auto-refresh on long ranges.

## Status widget

`cartography-dashboard-status-widget` (python3.13, arm64, 256 MB, 60s). It
calls `states:ListStateMachines` and `states:ListExecutions` (scoped to
`cartography-*`), nothing else. It follows the dashboard's time range. Per
machine it pages at most 1,000 executions (`MAX_EXECUTIONS_PER_MACHINE`);
counts show `N+` when capped. The AWS sync machine runs once per account split,
so a long time range on AWS can hit the cap.

Tests: `uvx --with boto3 --with pytest pytest -q dashboard/tests`

## Not included (yet)

- Alarms. Most alarm types can't use `SEARCH`, so they'd need a per-loader list
  or Metrics Insights queries. Add when there's somewhere to send them.
- Per-module timing from the AWS loader logs.
