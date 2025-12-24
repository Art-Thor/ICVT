#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${1:-}"
if [[ -z "$ENV_NAME" ]]; then
  echo "Usage: scripts/deploy_env.sh <stage|prod>" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/_common.sh"
source "$ROOT_DIR/scripts/env.sh" "$ENV_NAME"

require_aws

"$ROOT_DIR/scripts/build_artifacts.sh"

log "Uploading portal tarball to s3://$SILVER_BUCKET/$PORTAL_TARBALL_S3_KEY ..."
aws s3 cp \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  "$ROOT_DIR/dist/portal.tar.gz" \
  "s3://$SILVER_BUCKET/$PORTAL_TARBALL_S3_KEY"

log "Updating Lambda code ($BRONZE_LAMBDA, $SILVER_LAMBDA) ..."
aws lambda update-function-code \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --function-name "$BRONZE_LAMBDA" \
  --zip-file "fileb://$ROOT_DIR/dist/bronze-bucket-file-reader.zip" \
  >/dev/null

aws lambda update-function-code \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --function-name "$SILVER_LAMBDA" \
  --zip-file "fileb://$ROOT_DIR/dist/silver-bucket-file-creator.zip" \
  >/dev/null

PORTAL_INSTANCE_ID="$(find_instance_id_by_name "$AWS_PROFILE" "$AWS_REGION" "$PORTAL_INSTANCE_NAME")"
if [[ -z "$PORTAL_INSTANCE_ID" || "$PORTAL_INSTANCE_ID" == "None" ]]; then
  log "Portal instance '$PORTAL_INSTANCE_NAME' not found; skipping portal restart."
  exit 0
fi

log "Restarting portal on instance $PORTAL_INSTANCE_ID via SSM ..."
cmd_id="$(aws ssm send-command \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --document-name AWS-RunShellScript \
  --instance-ids "$PORTAL_INSTANCE_ID" \
  --parameters "commands=[
    \"set -euo pipefail\",
    \"export HOME=/root\",
    \"cd /opt/portal\",
    \"aws s3 cp s3://$SILVER_BUCKET/$PORTAL_TARBALL_S3_KEY ./portal.tar.gz\",
    \"rm -rf ./src_new && mkdir -p ./src_new\",
    \"tar xzf ./portal.tar.gz -C ./src_new --strip-components=1\",
    \"mkdir -p ./src\",
    \"rsync -a --delete --exclude '.env' --exclude 'data/' ./src_new/ ./src/\",
    \"cd ./src\",
    \"/usr/local/bin/docker-compose up -d --build\"
  ]" \
  --query 'Command.CommandId' \
  --output text)"

aws ssm get-command-invocation \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --command-id "$cmd_id" \
  --instance-id "$PORTAL_INSTANCE_ID" \
  --query '{Status:Status,StdOut:StandardOutputContent,StdErr:StandardErrorContent}' \
  --output json

public_ip="$(find_instance_public_ip "$AWS_PROFILE" "$AWS_REGION" "$PORTAL_INSTANCE_ID")"
if [[ -n "$public_ip" && "$public_ip" != "None" ]]; then
  log "Portal URL: http://$public_ip:8000"
fi
