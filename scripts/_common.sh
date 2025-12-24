#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '%s\n' "$*" >&2
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || {
    log "Missing required command: $cmd"
    exit 1
  }
}

require_aws() {
  require_cmd aws
  require_cmd jq
}

aws_account_id() {
  local profile="$1"
  local region="$2"
  aws sts get-caller-identity --profile "$profile" --region "$region" --query Account --output text
}

ensure_ssm_online() {
  local profile="$1"
  local region="$2"
  local instance_id="$3"
  local timeout_s="${4:-300}"
  local start
  start="$(date +%s)"

  while true; do
    local status
    status="$(aws ssm describe-instance-information \
      --profile "$profile" \
      --region "$region" \
      --filters "Key=InstanceIds,Values=$instance_id" \
      --query "InstanceInformationList[0].PingStatus" \
      --output text 2>/dev/null || true)"

    if [[ "$status" == "Online" ]]; then
      return 0
    fi

    local now
    now="$(date +%s)"
    if (( now - start > timeout_s )); then
      log "SSM instance $instance_id is not Online after ${timeout_s}s (status=${status:-unknown})"
      return 1
    fi
    sleep 5
  done
}

find_instance_id_by_name() {
  local profile="$1"
  local region="$2"
  local name="$3"
  aws ec2 describe-instances \
    --profile "$profile" \
    --region "$region" \
    --filters "Name=tag:Name,Values=$name" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query "Reservations[].Instances[0].InstanceId" \
    --output text
}

find_instance_public_ip() {
  local profile="$1"
  local region="$2"
  local instance_id="$3"
  aws ec2 describe-instances \
    --profile "$profile" \
    --region "$region" \
    --instance-ids "$instance_id" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text
}

