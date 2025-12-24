#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/_common.sh"
source "$ROOT_DIR/scripts/env.sh" stage

require_aws

STAGE_ALLOWED_CIDR="${STAGE_ALLOWED_CIDR:-}"
if [[ -z "$STAGE_ALLOWED_CIDR" ]]; then
  if command -v curl >/dev/null 2>&1; then
    ip="$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]' || true)"
    if [[ -n "$ip" ]]; then
      STAGE_ALLOWED_CIDR="${ip}/32"
    fi
  fi
fi

if [[ -z "$STAGE_ALLOWED_CIDR" ]]; then
  log "STAGE_ALLOWED_CIDR is required (e.g. 1.2.3.4/32)."
  exit 1
fi

ACCOUNT_ID="$(aws_account_id "$AWS_PROFILE" "$AWS_REGION")"
log "Using AWS account $ACCOUNT_ID ($AWS_PROFILE, $AWS_REGION)"

PROD_INSTANCE_ID="$(find_instance_id_by_name "$AWS_PROFILE" "$AWS_REGION" portal-ec2)"
if [[ -z "$PROD_INSTANCE_ID" || "$PROD_INSTANCE_ID" == "None" ]]; then
  log "Prod instance 'portal-ec2' not found; cannot infer subnet/vpc/ami."
  exit 1
fi

VPC_ID="$(aws ec2 describe-instances --profile "$AWS_PROFILE" --region "$AWS_REGION" --instance-ids "$PROD_INSTANCE_ID" --query "Reservations[0].Instances[0].VpcId" --output text)"
SUBNET_ID="$(aws ec2 describe-instances --profile "$AWS_PROFILE" --region "$AWS_REGION" --instance-ids "$PROD_INSTANCE_ID" --query "Reservations[0].Instances[0].SubnetId" --output text)"
AMI_ID="$(aws ec2 describe-instances --profile "$AWS_PROFILE" --region "$AWS_REGION" --instance-ids "$PROD_INSTANCE_ID" --query "Reservations[0].Instances[0].ImageId" --output text)"

ensure_bucket() {
  local bucket="$1"
  log "Ensuring bucket: $bucket"
  if aws s3api head-bucket --profile "$AWS_PROFILE" --bucket "$bucket" >/dev/null 2>&1; then
    return 0
  fi

  aws s3api create-bucket \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --bucket "$bucket" \
    --create-bucket-configuration "LocationConstraint=$AWS_REGION" \
    >/dev/null
}

apply_bucket_basics() {
  local bucket="$1"
  local bucket_key_enabled="$2"

  aws s3api put-public-access-block \
    --profile "$AWS_PROFILE" \
    --bucket "$bucket" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true \
    >/dev/null

  aws s3api put-bucket-ownership-controls \
    --profile "$AWS_PROFILE" \
    --bucket "$bucket" \
    --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerEnforced}]' \
    >/dev/null

  aws s3api put-bucket-encryption \
    --profile "$AWS_PROFILE" \
    --bucket "$bucket" \
    --server-side-encryption-configuration "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"AES256\"},\"BucketKeyEnabled\":${bucket_key_enabled}}]}" \
    >/dev/null

  cat >"$ROOT_DIR/dist/_bucket_policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowTextractServiceAccess",
      "Effect": "Allow",
      "Principal": { "Service": "textract.amazonaws.com" },
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::$bucket/*"
    },
    {
      "Sid": "AllowTextractBucketAccess",
      "Effect": "Allow",
      "Principal": { "Service": "textract.amazonaws.com" },
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::$bucket"
    },
    {
      "Sid": "AllowAccountAccess",
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::$ACCOUNT_ID:root" },
      "Action": "s3:*",
      "Resource": ["arn:aws:s3:::$bucket", "arn:aws:s3:::$bucket/*"]
    }
  ]
}
JSON

  aws s3api put-bucket-policy \
    --profile "$AWS_PROFILE" \
    --bucket "$bucket" \
    --policy "file://$ROOT_DIR/dist/_bucket_policy.json" \
    >/dev/null
}

apply_bucket_cors() {
  local bucket="$1"
  local cors_json="$2"
  aws s3api put-bucket-cors \
    --profile "$AWS_PROFILE" \
    --bucket "$bucket" \
    --cors-configuration "$cors_json" \
    >/dev/null
}

mkdir -p "$ROOT_DIR/dist"

ensure_bucket "$BRONZE_BUCKET"
ensure_bucket "$SILVER_BUCKET"

apply_bucket_basics "$BRONZE_BUCKET" true
apply_bucket_basics "$SILVER_BUCKET" false

apply_bucket_cors "$BRONZE_BUCKET" '{
  "CORSRules": [
    {
      "AllowedHeaders": ["*"],
      "AllowedMethods": ["GET","HEAD","PUT"],
      "AllowedOrigins": ["*"],
      "ExposeHeaders": ["ETag","Accept-Ranges","Content-Range","Content-Length"],
      "MaxAgeSeconds": 3000
    }
  ]
}'

apply_bucket_cors "$SILVER_BUCKET" '{
  "CORSRules": [
    {
      "AllowedHeaders": ["*"],
      "AllowedMethods": ["GET","PUT","POST","DELETE","HEAD"],
      "AllowedOrigins": ["*","https://*.console.aws.amazon.com","https://console.aws.amazon.com","https://*.amazonaws.com"],
      "ExposeHeaders": ["ETag","x-amz-server-side-encryption","x-amz-request-id","x-amz-id-2"],
      "MaxAgeSeconds": 3000
    }
  ]
}'

log "Creating DynamoDB tables for stage (if missing)..."
if ! aws dynamodb describe-table --profile "$AWS_PROFILE" --region "$AWS_REGION" --table-name "$INVOICES_TABLE" >/dev/null 2>&1; then
  aws dynamodb create-table \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --table-name "$INVOICES_TABLE" \
    --attribute-definitions \
      AttributeName=job_prefix,AttributeType=S \
      AttributeName=doc_id,AttributeType=S \
      AttributeName=status,AttributeType=S \
      AttributeName=created_at,AttributeType=S \
    --key-schema \
      AttributeName=job_prefix,KeyType=HASH \
      AttributeName=doc_id,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --global-secondary-indexes "[
      {
        \"IndexName\": \"status-created_at\",
        \"KeySchema\": [
          {\"AttributeName\": \"status\", \"KeyType\": \"HASH\"},
          {\"AttributeName\": \"created_at\", \"KeyType\": \"RANGE\"}
        ],
        \"Projection\": {\"ProjectionType\": \"ALL\"},
        \"ProvisionedThroughput\": {\"ReadCapacityUnits\": 5, \"WriteCapacityUnits\": 5}
      }
    ]" \
    >/dev/null
  aws dynamodb wait table-exists --profile "$AWS_PROFILE" --region "$AWS_REGION" --table-name "$INVOICES_TABLE"
fi

if ! aws dynamodb describe-table --profile "$AWS_PROFILE" --region "$AWS_REGION" --table-name "$AUDIT_TABLE" >/dev/null 2>&1; then
  aws dynamodb create-table \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --table-name "$AUDIT_TABLE" \
    --attribute-definitions \
      AttributeName=doc_id,AttributeType=S \
      AttributeName=created_at,AttributeType=S \
    --key-schema \
      AttributeName=doc_id,KeyType=HASH \
      AttributeName=created_at,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    >/dev/null
  aws dynamodb wait table-exists --profile "$AWS_PROFILE" --region "$AWS_REGION" --table-name "$AUDIT_TABLE"
fi

log "Creating IAM roles (if missing)..."
BRONZE_ROLE_NAME="bronze-bucket-lambda-role-stage"
SILVER_ROLE_NAME="silver-bucket-file-creator-role-stage"
PORTAL_ROLE_NAME="portal-ec2-role-stage"
PORTAL_PROFILE_NAME="portal-ec2-profile-stage"

if ! aws iam get-role --profile "$AWS_PROFILE" --role-name "$BRONZE_ROLE_NAME" >/dev/null 2>&1; then
  aws iam create-role \
    --profile "$AWS_PROFILE" \
    --role-name "$BRONZE_ROLE_NAME" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    >/dev/null
fi
aws iam attach-role-policy --profile "$AWS_PROFILE" --role-name "$BRONZE_ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole >/dev/null || true
aws iam put-role-policy \
  --profile "$AWS_PROFILE" \
  --role-name "$BRONZE_ROLE_NAME" \
  --policy-name bronze-stage-inline \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\",\"s3:ListBucket\"],
        \"Resource\": [\"arn:aws:s3:::$BRONZE_BUCKET\",\"arn:aws:s3:::$BRONZE_BUCKET/*\"]
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:PutObject\",\"s3:GetObject\",\"s3:ListBucket\"],
        \"Resource\": [\"arn:aws:s3:::$SILVER_BUCKET\",\"arn:aws:s3:::$SILVER_BUCKET/*\"]
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [
          \"textract:DetectDocumentText\",
          \"textract:AnalyzeDocument\",
          \"textract:StartDocumentAnalysis\",
          \"textract:GetDocumentAnalysis\",
          \"textract:GetDocumentTextDetection\"
        ],
        \"Resource\": \"*\"
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"secretsmanager:GetSecretValue\",\"secretsmanager:DescribeSecret\"],
        \"Resource\": \"arn:aws:secretsmanager:$AWS_REGION:$ACCOUNT_ID:secret:$OPENAI_SECRET_NAME*\"
      }
    ]
  }" \
  >/dev/null

if ! aws iam get-role --profile "$AWS_PROFILE" --role-name "$SILVER_ROLE_NAME" >/dev/null 2>&1; then
  aws iam create-role \
    --profile "$AWS_PROFILE" \
    --role-name "$SILVER_ROLE_NAME" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    >/dev/null
fi
aws iam attach-role-policy --profile "$AWS_PROFILE" --role-name "$SILVER_ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole >/dev/null || true
aws iam put-role-policy \
  --profile "$AWS_PROFILE" \
  --role-name "$SILVER_ROLE_NAME" \
  --policy-name silver-stage-inline \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\"],
        \"Resource\": \"arn:aws:s3:::$BRONZE_BUCKET/*\"
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\",\"s3:PutObject\"],
        \"Resource\": \"arn:aws:s3:::$SILVER_BUCKET/*\"
      }
    ]
  }" \
  >/dev/null

if ! aws iam get-role --profile "$AWS_PROFILE" --role-name "$PORTAL_ROLE_NAME" >/dev/null 2>&1; then
  aws iam create-role \
    --profile "$AWS_PROFILE" \
    --role-name "$PORTAL_ROLE_NAME" \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
    >/dev/null
fi
aws iam attach-role-policy --profile "$AWS_PROFILE" --role-name "$PORTAL_ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore >/dev/null || true
aws iam put-role-policy \
  --profile "$AWS_PROFILE" \
  --role-name "$PORTAL_ROLE_NAME" \
  --policy-name portal-stage-inline \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"s3:GetObject\",\"s3:PutObject\",\"s3:ListBucket\"],
        \"Resource\": [
          \"arn:aws:s3:::$BRONZE_BUCKET\",
          \"arn:aws:s3:::$BRONZE_BUCKET/*\",
          \"arn:aws:s3:::$SILVER_BUCKET\",
          \"arn:aws:s3:::$SILVER_BUCKET/*\"
        ]
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"dynamodb:*\"] ,
        \"Resource\": [
          \"arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$INVOICES_TABLE\",
          \"arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$INVOICES_TABLE/index/*\",
          \"arn:aws:dynamodb:$AWS_REGION:$ACCOUNT_ID:table/$AUDIT_TABLE\"
        ]
      },
      {
        \"Effect\": \"Allow\",
        \"Action\": [\"lambda:InvokeFunction\"],
        \"Resource\": [
          \"arn:aws:lambda:$AWS_REGION:$ACCOUNT_ID:function:$BRONZE_LAMBDA\",
          \"arn:aws:lambda:$AWS_REGION:$ACCOUNT_ID:function:$SILVER_LAMBDA\"
        ]
      }
    ]
  }" \
  >/dev/null

if ! aws iam get-instance-profile --profile "$AWS_PROFILE" --instance-profile-name "$PORTAL_PROFILE_NAME" >/dev/null 2>&1; then
  aws iam create-instance-profile --profile "$AWS_PROFILE" --instance-profile-name "$PORTAL_PROFILE_NAME" >/dev/null
fi
aws iam add-role-to-instance-profile --profile "$AWS_PROFILE" --instance-profile-name "$PORTAL_PROFILE_NAME" --role-name "$PORTAL_ROLE_NAME" >/dev/null || true

log "Building + uploading portal tarball to stage bucket..."
"$ROOT_DIR/scripts/build_artifacts.sh" >/dev/null
aws s3 cp \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  "$ROOT_DIR/dist/portal.tar.gz" \
  "s3://$SILVER_BUCKET/$PORTAL_TARBALL_S3_KEY" \
  >/dev/null

log "Creating/updating Lambda functions for stage..."
BRONZE_ROLE_ARN="$(aws iam get-role --profile "$AWS_PROFILE" --role-name "$BRONZE_ROLE_NAME" --query Role.Arn --output text)"
SILVER_ROLE_ARN="$(aws iam get-role --profile "$AWS_PROFILE" --role-name "$SILVER_ROLE_NAME" --query Role.Arn --output text)"

if ! aws lambda get-function --profile "$AWS_PROFILE" --region "$AWS_REGION" --function-name "$BRONZE_LAMBDA" >/dev/null 2>&1; then
  aws lambda create-function \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --function-name "$BRONZE_LAMBDA" \
    --runtime python3.12 \
    --handler lambda_s3_event_driven.lambda_handler \
    --role "$BRONZE_ROLE_ARN" \
    --timeout 90 \
    --memory-size 256 \
    --layers "$OPENAI_LAYER_ARN" \
    --environment "Variables={BRONZE_BUCKET=$BRONZE_BUCKET,SILVER_BUCKET=$SILVER_BUCKET,OPENAI_SECRET_NAME=$OPENAI_SECRET_NAME}" \
    --zip-file "fileb://$ROOT_DIR/dist/bronze-bucket-file-reader.zip" \
    >/dev/null
else
  aws lambda update-function-code \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --function-name "$BRONZE_LAMBDA" \
    --zip-file "fileb://$ROOT_DIR/dist/bronze-bucket-file-reader.zip" \
    >/dev/null
  aws lambda update-function-configuration \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --function-name "$BRONZE_LAMBDA" \
    --timeout 90 \
    --memory-size 256 \
    --layers "$OPENAI_LAYER_ARN" \
    --environment "Variables={BRONZE_BUCKET=$BRONZE_BUCKET,SILVER_BUCKET=$SILVER_BUCKET,OPENAI_SECRET_NAME=$OPENAI_SECRET_NAME}" \
    >/dev/null
fi

if ! aws lambda get-function --profile "$AWS_PROFILE" --region "$AWS_REGION" --function-name "$SILVER_LAMBDA" >/dev/null 2>&1; then
  aws lambda create-function \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --function-name "$SILVER_LAMBDA" \
    --runtime python3.12 \
    --handler lambda_function.lambda_handler \
    --role "$SILVER_ROLE_ARN" \
    --timeout 3 \
    --memory-size 128 \
    --environment "Variables={BRONZE_BUCKET=$BRONZE_BUCKET,SILVER_BUCKET=$SILVER_BUCKET,EXPORT_PREFIX=export,TEXTRACT_PREFIX=textract-results}" \
    --zip-file "fileb://$ROOT_DIR/dist/silver-bucket-file-creator.zip" \
    >/dev/null
else
  aws lambda update-function-code \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --function-name "$SILVER_LAMBDA" \
    --zip-file "fileb://$ROOT_DIR/dist/silver-bucket-file-creator.zip" \
    >/dev/null
  aws lambda update-function-configuration \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --function-name "$SILVER_LAMBDA" \
    --timeout 3 \
    --memory-size 128 \
    --environment "Variables={BRONZE_BUCKET=$BRONZE_BUCKET,SILVER_BUCKET=$SILVER_BUCKET,EXPORT_PREFIX=export,TEXTRACT_PREFIX=textract-results}" \
    >/dev/null
fi

BRONZE_LAMBDA_ARN="$(aws lambda get-function-configuration --profile "$AWS_PROFILE" --region "$AWS_REGION" --function-name "$BRONZE_LAMBDA" --query FunctionArn --output text)"

log "Configuring S3->Lambda trigger on $BRONZE_BUCKET ..."
aws lambda add-permission \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --function-name "$BRONZE_LAMBDA" \
  --statement-id allow-s3-invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn "arn:aws:s3:::$BRONZE_BUCKET" \
  >/dev/null 2>/dev/null || true

aws s3api put-bucket-notification-configuration \
  --profile "$AWS_PROFILE" \
  --bucket "$BRONZE_BUCKET" \
  --notification-configuration "{
    \"LambdaFunctionConfigurations\": [
      {
        \"Id\": \"ProcessPDFFiles\",
        \"LambdaFunctionArn\": \"$BRONZE_LAMBDA_ARN\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\": \"Suffix\", \"Value\": \".pdf\"}]}}
      },
      {
        \"Id\": \"ProcessPDFFilesUpperCase\",
        \"LambdaFunctionArn\": \"$BRONZE_LAMBDA_ARN\",
        \"Events\": [\"s3:ObjectCreated:*\"],
        \"Filter\": {\"Key\": {\"FilterRules\": [{\"Name\": \"Suffix\", \"Value\": \".PDF\"}]}}
      }
    ]
  }" \
  >/dev/null

log "Creating security group for stage portal..."
PORTAL_SG_NAME="portal-sg-stage"
PORTAL_SG_ID="$(aws ec2 describe-security-groups --profile "$AWS_PROFILE" --region "$AWS_REGION" --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=$PORTAL_SG_NAME" --query "SecurityGroups[0].GroupId" --output text)"
if [[ -z "$PORTAL_SG_ID" || "$PORTAL_SG_ID" == "None" ]]; then
  PORTAL_SG_ID="$(aws ec2 create-security-group --profile "$AWS_PROFILE" --region "$AWS_REGION" --group-name "$PORTAL_SG_NAME" --description "Portal Stage SG" --vpc-id "$VPC_ID" --query GroupId --output text)"
fi

aws ec2 authorize-security-group-ingress \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --group-id "$PORTAL_SG_ID" \
  --ip-permissions "[{\"IpProtocol\":\"tcp\",\"FromPort\":8000,\"ToPort\":8000,\"IpRanges\":[{\"CidrIp\":\"$STAGE_ALLOWED_CIDR\"}]}]" \
  >/dev/null 2>/dev/null || true

log "Launching stage EC2 instance (if missing)..."
STAGE_INSTANCE_ID="$(find_instance_id_by_name "$AWS_PROFILE" "$AWS_REGION" "$PORTAL_INSTANCE_NAME")"
if [[ -z "$STAGE_INSTANCE_ID" || "$STAGE_INSTANCE_ID" == "None" ]]; then
  USERDATA_FILE="$ROOT_DIR/dist/userdata_stage.sh"
  cat >"$USERDATA_FILE" <<EOF
#!/bin/bash
set -euo pipefail
export HOME=/root

amazon-linux-extras install -y docker
yum install -y docker awscli curl rsync
systemctl enable docker
systemctl start docker
usermod -aG docker ec2-user

curl -L "https://github.com/docker/compose/releases/download/v2.23.0/docker-compose-\$(uname -s)-\$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

mkdir -p /opt/portal
cd /opt/portal
aws s3 cp s3://$SILVER_BUCKET/$PORTAL_TARBALL_S3_KEY ./portal.tar.gz
mkdir -p src
tar xzf portal.tar.gz -C src --strip-components=1

if [[ ! -f /opt/portal/src/.env ]]; then
  JWT=\$(cat /proc/sys/kernel/random/uuid)
  cat > /opt/portal/src/.env <<ENV
AWS_REGION=$AWS_REGION
BRONZE_BUCKET=$BRONZE_BUCKET
SILVER_BUCKET=$SILVER_BUCKET
INVOICES_TABLE=$INVOICES_TABLE
AUDIT_TABLE=$AUDIT_TABLE
JWT_SECRET=\${JWT}
ADMIN_EMAIL=admin@example.com
ADMIN_PASSWORD=ChangeMe123!
ADMIN_RESET=1
ENV
fi

cd /opt/portal/src
/usr/local/bin/docker-compose up -d --build
EOF

  STAGE_INSTANCE_ID="$(aws ec2 run-instances \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --image-id "$AMI_ID" \
    --instance-type t3.small \
    --subnet-id "$SUBNET_ID" \
    --security-group-ids "$PORTAL_SG_ID" \
    --iam-instance-profile "Name=$PORTAL_PROFILE_NAME" \
    --user-data "file://$USERDATA_FILE" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$PORTAL_INSTANCE_NAME}]" \
    --query "Instances[0].InstanceId" \
    --output text)"
fi

aws ec2 wait instance-running --profile "$AWS_PROFILE" --region "$AWS_REGION" --instance-ids "$STAGE_INSTANCE_ID"
ensure_ssm_online "$AWS_PROFILE" "$AWS_REGION" "$STAGE_INSTANCE_ID" 600

STAGE_PUBLIC_IP="$(find_instance_public_ip "$AWS_PROFILE" "$AWS_REGION" "$STAGE_INSTANCE_ID")"
log "Stage portal instance: $STAGE_INSTANCE_ID ($STAGE_PUBLIC_IP)"
log "Stage portal URL: http://$STAGE_PUBLIC_IP:8000"
