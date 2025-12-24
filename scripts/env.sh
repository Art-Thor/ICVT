#!/usr/bin/env bash
set -euo pipefail

# Centralized environment config.
#
# Usage:
#   source scripts/env.sh <stage|prod>

ENV_NAME="${1:-}"
if [[ -z "$ENV_NAME" ]]; then
  echo "Usage: source scripts/env.sh <stage|prod>" >&2
  return 1 2>/dev/null || exit 1
fi

AWS_PROFILE="${AWS_PROFILE:-Adam}"
AWS_REGION="${AWS_REGION:-ap-southeast-2}"

case "$ENV_NAME" in
  prod)
    BRONZE_BUCKET="bronze-bucket-icvt"
    SILVER_BUCKET="silver-bucket-icvt"
    BRONZE_LAMBDA="bronze-bucket-file-reader"
    SILVER_LAMBDA="silver-bucket-file-creator"
    PORTAL_INSTANCE_NAME="portal-ec2"
    INVOICES_TABLE="invoice-portal-invoices"
    AUDIT_TABLE="invoice-portal-audit"
    ;;
  stage)
    BRONZE_BUCKET="bronze-bucket-icvt-stage"
    SILVER_BUCKET="silver-bucket-icvt-stage"
    BRONZE_LAMBDA="bronze-bucket-file-reader-stage"
    SILVER_LAMBDA="silver-bucket-file-creator-stage"
    PORTAL_INSTANCE_NAME="portal-ec2-stage"
    INVOICES_TABLE="invoice-portal-invoices-stage"
    AUDIT_TABLE="invoice-portal-audit-stage"
    ;;
  *)
    echo "Unknown env: $ENV_NAME (expected stage|prod)" >&2
    return 1 2>/dev/null || exit 1
    ;;
esac

PORTAL_TARBALL_S3_KEY="portal/portal.tar.gz"

# bronze lambda expects OpenAI layer and secret
OPENAI_LAYER_ARN="${OPENAI_LAYER_ARN:-arn:aws:lambda:${AWS_REGION}:292139251108:layer:openai-layer:5}"
OPENAI_SECRET_NAME="${OPENAI_SECRET_NAME:-openai/bronze-bucket-file-reader/api-key}"

