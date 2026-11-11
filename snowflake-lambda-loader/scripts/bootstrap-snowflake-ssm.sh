#!/bin/bash
set -euo pipefail

# One-time bootstrap: takes the Snowflake admin's credential handoff (a JSON
# file with a base64-encoded, DER-encoded, encrypted PKCS8 private key plus
# account metadata) and loads it into SSM Parameter Store, re-encoding the
# key to PEM along the way.
#
# WHY THE RE-ENCODE: `private_key_b64` decodes to raw DER bytes,
# NOT a PEM-wrapped ("-----BEGIN...-----") file. cartography's
# SnowflakeClient._load_private_key() calls
# serialization.load_pem_private_key(), which rejects DER outright
# (`MalformedFraming`). Converting once, here, means neither the Lambda
# handler nor a manual EC2 test needs its own DER-vs-PEM fallback code.
#
# SINGLE SOURCE OF TRUTH: both ../template.yaml (the Lambda) and a manual
# EC2 test (fetch the same parameters via `aws ssm get-parameter
# --with-decryption`) read the exact six parameters this script writes.
# Safe to re-run — every parameter is --overwrite.
#
# Usage:
#   ./bootstrap-snowflake-ssm.sh <path-to-creds-json>
#
# Expected input JSON shape (exactly what the Snowflake admin hands over):
# {
#   "private_key_b64": "<base64 of DER-encoded, encrypted PKCS8 private key>",
#   "private_key_passphrase": "...",
#   "role": "...",
#   "username": "...",
#   "warehouse": "...",
#   "account_identifier": "MYORG-MYACCOUNT"
# }
#
# SSM parameters written (region $AWS_REGION, all --overwrite):
#   /cartography/snowflake/account                 (String)
#   /cartography/snowflake/user                     (String)
#   /cartography/snowflake/role                     (String)
#   /cartography/snowflake/warehouse                (String)
#   /cartography/snowflake/private-key               (SecureString, Standard
#                                                      tier, PEM, re-encrypted
#                                                      with the SAME
#                                                      passphrase — never
#                                                      stored decrypted)
#   /cartography/snowflake/private-key-passphrase    (SecureString)
#
# SECRET-HANDLING NOTES:
# - The decoded/converted key is NEVER written to disk — the DER->PEM
#   conversion runs entirely through in-memory pipes
#   (`base64 -d | openssl pkcs8 ...`).
# - The passphrase is passed to openssl via an environment variable
#   (`env:NAME`), never a `-passin pass:...`/`-passout pass:...` CLI
#   argument — the latter is visible in `ps aux` for the life of the
#   process on a shared host.
# - Every `aws ssm put-parameter` call for a secret value is invoked via
#   `--cli-input-json file://<tempfile>`, NOT `--value`, so the key/
#   passphrase never appear as a process argv entry visible to `ps aux` on a
#   shared host. The temp file is created with `umask 077` (owner-read-only,
#   0600), lives only in `mktemp`'s private per-user tmp dir, and is shredded
#   (`rm -f`, best-effort `shred -u` first) via a `trap ... EXIT` the moment
#   this script exits — including on error, via `set -euo pipefail`.
#   NOTE: `--cli-input-json file:///dev/stdin` was tried first and rejected —
#   confirmed empirically (aws-cli 2.34) that piping JSON into
#   `file:///dev/stdin` fails with `Error parsing parameter 'cli-input-json':
#   Invalid JSON received`, because the CLI's JSON loader needs to `seek()`
#   the file object and a pipe/FIFO is not seekable. A real (if short-lived,
#   tightly-permissioned, immediately-shredded) temp file is the actual fix.
# - Nothing in this script prints key or passphrase material to stdout.

REGION="${AWS_REGION:?set AWS_REGION}"
CREDS_FILE="${1:?Usage: $0 <path-to-creds-json>}"

if [[ ! -f "$CREDS_FILE" ]]; then
  echo "Error: credentials file not found: $CREDS_FILE" >&2
  exit 1
fi

command -v openssl >/dev/null || { echo "Error: openssl is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "Error: python3 is required" >&2; exit 1; }
command -v aws >/dev/null || { echo "Error: aws CLI is required" >&2; exit 1; }

# Private, short-lived scratch dir for the put-parameter JSON payload temp
# files (see "WHY A TEMP FILE" above) — 0700, owned by the running user,
# removed (not just emptied) on exit via the trap below regardless of
# success/failure/interrupt.
SCRATCH_DIR=$(umask 077 && mktemp -d "${TMPDIR:-/tmp}/snowflake-ssm-bootstrap.XXXXXX")
cleanup() {
  # Best-effort overwrite-before-unlink; `shred` is not available on macOS by
  # default, so fall back to a plain (still owner-only, still removed) rm.
  if command -v shred >/dev/null 2>&1; then
    find "$SCRATCH_DIR" -type f -exec shred -u {} + 2>/dev/null || true
  fi
  rm -rf "$SCRATCH_DIR"
}
trap cleanup EXIT

# Parses one top-level field out of the creds JSON via python3 (no hard `jq`
# dependency). Fails loudly (exit
# 1, no stdout) if the key is absent, so a typo'd/missing field in the
# admin's JSON is caught before anything is written to SSM.
read_field() {
  python3 -c "
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
v = d.get(sys.argv[2])
if v is None or v == '':
    sys.exit(1)
sys.stdout.write(str(v))
" "$CREDS_FILE" "$1"
}

ACCOUNT_IDENTIFIER=$(read_field account_identifier) || { echo "Error: missing/empty 'account_identifier' in $CREDS_FILE" >&2; exit 1; }
USERNAME=$(read_field username) || { echo "Error: missing/empty 'username' in $CREDS_FILE" >&2; exit 1; }
ROLE=$(read_field role) || { echo "Error: missing/empty 'role' in $CREDS_FILE" >&2; exit 1; }
WAREHOUSE=$(read_field warehouse) || { echo "Error: missing/empty 'warehouse' in $CREDS_FILE" >&2; exit 1; }
PRIVATE_KEY_B64=$(read_field private_key_b64) || { echo "Error: missing/empty 'private_key_b64' in $CREDS_FILE" >&2; exit 1; }

# Passphrase lives ONLY in this exported env var — never echoed, never a CLI
# argument. `openssl ... env:SNOWFLAKE_KEY_PASSPHRASE` reads it from the
# environment of this process, not from argv.
export SNOWFLAKE_KEY_PASSPHRASE
SNOWFLAKE_KEY_PASSPHRASE=$(read_field private_key_passphrase) || { echo "Error: missing/empty 'private_key_passphrase' in $CREDS_FILE" >&2; exit 1; }

echo "Parsed credentials for account=${ACCOUNT_IDENTIFIER} user=${USERNAME} role=${ROLE} warehouse=${WAREHOUSE}."
echo "Converting Snowflake private key: DER (encrypted PKCS8) -> PEM (encrypted PKCS8)..."

# The passphrase is PRESERVED (re-encrypted with the SAME passphrase, not
# decrypted-at-rest in SSM) — matches cartography's
# --snowflake-private-key-passphrase-env-var contract, and means the
# SSM-stored key is still protected by a secret distinct from IAM/SSM access
# control alone (defense in depth: a principal with ssm:GetParameter +
# kms:Decrypt on this parameter still needs the passphrase to use the key).
SNOWFLAKE_PRIVATE_KEY_PEM=$(
  base64 -d <<<"$PRIVATE_KEY_B64" \
    | openssl pkcs8 -topk8 -inform DER \
        -passin env:SNOWFLAKE_KEY_PASSPHRASE \
        -v2 aes256 \
        -passout env:SNOWFLAKE_KEY_PASSPHRASE
)

if [[ -z "$SNOWFLAKE_PRIVATE_KEY_PEM" ]] || ! grep -q "BEGIN ENCRYPTED PRIVATE KEY" <<<"$SNOWFLAKE_PRIVATE_KEY_PEM"; then
  echo "Error: DER->PEM conversion failed or produced unexpected output (no PEM header found)." >&2
  exit 1
fi
echo "Conversion OK ($(grep -c '.' <<<"$SNOWFLAKE_PRIVATE_KEY_PEM") lines of PEM)."

echo "Writing SSM parameters to region ${REGION} (all --overwrite)..."

# Plain (non-secret) String parameters — safe as --value argv entries, same
# as every other non-secret parameter already in this repo (e.g.
# /cartography/vercel/team-slug, /cartography/okta-lambda/schedule).
aws ssm put-parameter --region "$REGION" --overwrite \
  --name /cartography/snowflake/account \
  --type String --value "$ACCOUNT_IDENTIFIER" >/dev/null
echo "  wrote /cartography/snowflake/account"

aws ssm put-parameter --region "$REGION" --overwrite \
  --name /cartography/snowflake/user \
  --type String --value "$USERNAME" >/dev/null
echo "  wrote /cartography/snowflake/user"

aws ssm put-parameter --region "$REGION" --overwrite \
  --name /cartography/snowflake/role \
  --type String --value "$ROLE" >/dev/null
echo "  wrote /cartography/snowflake/role"

aws ssm put-parameter --region "$REGION" --overwrite \
  --name /cartography/snowflake/warehouse \
  --type String --value "$WAREHOUSE" >/dev/null
echo "  wrote /cartography/snowflake/warehouse"

# Secret SecureString parameters — written via --cli-input-json pointed at a
# private, 0600, immediately-shredded temp file (see "WHY A TEMP FILE" at the
# top of this script for why a plain pipe into file:///dev/stdin does not
# work), rather than --value, so the key/passphrase never appear as a
# process argv entry visible to `ps aux` on this host.
put_secure_param() {
  local param_name="$1"
  local tier="$2"
  local value="$3"
  local payload_file
  payload_file=$(umask 077 && mktemp "$SCRATCH_DIR/payload.XXXXXX")
  python3 -c "
import json, sys
payload = {
    'Name': sys.argv[1],
    'Type': 'SecureString',
    'Overwrite': True,
    'Tier': sys.argv[2],
    'Value': sys.stdin.read(),
}
with open(sys.argv[3], 'w') as f:
    json.dump(payload, f)
" "$param_name" "$tier" "$payload_file" <<<"$value"
  aws ssm put-parameter --region "$REGION" --cli-input-json "file://$payload_file" >/dev/null
  if command -v shred >/dev/null 2>&1; then
    shred -u "$payload_file" 2>/dev/null || rm -f "$payload_file"
  else
    rm -f "$payload_file"
  fi
}

# Standard tier (not Advanced): measured directly — an encrypted PKCS8 PEM
# for a 2048-bit RSA key is ~1,885 characters, under half of Standard SecureString's 4,096-char
# cap. A 4096-bit key would roughly double that and still fit. No need for
# Advanced tier's extra cost here (unlike, e.g.,
# ../../okta-lambda-loader/template.yaml's default-splits parameter, which
# holds a JSON array that can legitimately grow past 4096 chars).
put_secure_param /cartography/snowflake/private-key Standard "$SNOWFLAKE_PRIVATE_KEY_PEM"
echo "  wrote /cartography/snowflake/private-key (SecureString, Standard tier)"

put_secure_param /cartography/snowflake/private-key-passphrase Standard "$SNOWFLAKE_KEY_PASSPHRASE"
echo "  wrote /cartography/snowflake/private-key-passphrase (SecureString)"

unset SNOWFLAKE_PRIVATE_KEY_PEM SNOWFLAKE_KEY_PASSPHRASE PRIVATE_KEY_B64

echo
echo "Done. Spot-check a non-secret value:"
echo "  aws ssm get-parameter --region $REGION --name /cartography/snowflake/account --query Parameter.Value --output text"
echo
echo "Both ../template.yaml (the Snowflake Lambda loader) and a manual EC2"
echo "test (aws ssm start-session + fetch via --with-decryption) read these"
echo "same six parameters as the single source of truth for Snowflake"
echo "credentials — this script is the only place they are written."
