#!/usr/bin/env bash
# Copy the Falcon API client from 1Password into SSM ($AWS_REGION).
# Values flow op -> aws via stdin-free env vars; nothing is echoed.
#
#   AWS_REGION=us-east-1 OP_ITEM="op://<vault>/<item>" ./scripts/bootstrap-crowdstrike-ssm.sh
#
# The item's username/credential/hostname fields hold the client ID, secret
# and API URL. Set OP_ACCOUNT if you use more than one 1Password account.
set -euo pipefail

OP_ITEM="${OP_ITEM:?set OP_ITEM=op://<vault>/<item>}"
REGION="${AWS_REGION:?set AWS_REGION}"
PREFIX="${CROWDSTRIKE_SSM_PREFIX:-/cartography/crowdstrike}"

client_id="$(op read "$OP_ITEM/username")"
client_secret="$(op read "$OP_ITEM/credential")"
api_url="$(op read "$OP_ITEM/hostname")"

put() {
  aws ssm put-parameter --region "$REGION" --overwrite --type "$1" \
    --name "$PREFIX/$2" --value "$3" >/dev/null
  echo "wrote $PREFIX/$2 ($1)"
}

put SecureString client-id "$client_id"
put SecureString client-secret "$client_secret"
put String api-url "$api_url"
