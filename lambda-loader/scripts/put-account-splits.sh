#!/usr/bin/env bash
# Store account-splits.json in SSM, only if it passes the rules.
#
#   1. validate locally (src/splits.py, plain python3)
#   2. validate with the DEPLOYED Lambda (the code that judges the run)
#   3. refuse if an orchestrator run is in progress
#   4. put-parameter, read back, compare
#
# Usage: scripts/put-account-splits.sh [account-splits.json]
set -euo pipefail

cd "$(dirname "$0")/.."
FILE="${1:-account-splits.json}"
REGION="${AWS_REGION:?set AWS_REGION}"
PARAM=/cartography/lambda-orchestrator/account-splits
FUNCTION=cartography-lambda-loader
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ORCH_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:cartography-lambda-orchestrator"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "== 1. local validation"
python3 src/splits.py "$FILE"

COMPACT="$(python3 -c 'import json,sys; sys.path.insert(0,"src"); import splits; print(splits.compact(json.load(open(sys.argv[1]))))' "$FILE")"

echo "== 2. deployed Lambda validation"
python3 -c 'import json,sys; print(json.dumps({"validate_account_splits": json.loads(sys.argv[1])}))' "$COMPACT" > "$TMP/event.json"
aws lambda invoke --region "$REGION" --function-name "$FUNCTION" \
  --cli-binary-format raw-in-base64-out --payload "file://$TMP/event.json" "$TMP/out.json" >/dev/null
if ! python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); [print(e) for e in r.get("errors",[])]; sys.exit(0 if r.get("status")=="VALID" else 1)' "$TMP/out.json"; then
  echo "FAIL: deployed Lambda rejected the config (or doesn't have validate mode yet: $(cat "$TMP/out.json"))"
  exit 1
fi
echo "OK"

echo "== 3. no orchestrator run in progress"
RUNNING="$(aws stepfunctions list-executions --region "$REGION" --state-machine-arn "$ORCH_ARN" \
  --status-filter RUNNING --query 'length(executions)' --output text)"
if [ "$RUNNING" != "0" ]; then
  echo "FAIL: $RUNNING orchestrator run(s) in progress; try again after it finishes"
  exit 1
fi
echo "OK"

echo "== 4. put + read back"
VERSION="$(aws ssm put-parameter --region "$REGION" --name "$PARAM" --overwrite \
  --tier Advanced --type String --value "$COMPACT" --query Version --output text)"
STORED="$(aws ssm get-parameter --region "$REGION" --name "$PARAM" --query Parameter.Value --output text)"
if [ "$STORED" != "$COMPACT" ]; then
  echo "FAIL: read-back differs from what was written (version $VERSION)"
  exit 1
fi
echo "OK: $PARAM version $VERSION (${#COMPACT} bytes)"
