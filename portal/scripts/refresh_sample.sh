#!/usr/bin/env bash
#
# Refresh S3 test data with a clean sample.
# 1) Pull up to SAMPLE_PER_JOB JSONs from silver (textract-results/<job>/...)
# 2) Download paired source files from bronze when referenced
# 3) Optionally wipe old test data from bronze/silver
# 4) Ready to re-upload the sampled set for clean testing
#
# Defaults are SAFE (DRY_RUN=1). Set DRY_RUN=0 to actually delete.

set -euo pipefail

PROFILE="${PROFILE:-Adam}"
REGION="${REGION:-ap-southeast-2}"
SILVER_BUCKET="${SILVER_BUCKET:-silver-bucket-icvt}"
BRONZE_BUCKET="${BRONZE_BUCKET:-bronze-bucket-icvt}"
SAMPLE_DIR="${SAMPLE_DIR:-./sample_backup}"
SAMPLE_PER_JOB="${SAMPLE_PER_JOB:-25}"
DRY_RUN="${DRY_RUN:-1}" # 1 = no deletions, 0 = delete old data

mkdir -p "$SAMPLE_DIR"

python - <<'PY'
import os, json, random, pathlib, boto3

profile = os.environ.get("PROFILE", "Adam")
region = os.environ.get("REGION", "ap-southeast-2")
silver = os.environ["SILVER_BUCKET"]
bronze = os.environ["BRONZE_BUCKET"]
sample_dir = pathlib.Path(os.environ["SAMPLE_DIR"]).resolve()
per_job = int(os.environ.get("SAMPLE_PER_JOB", "25"))

session = boto3.Session(profile_name=profile, region_name=region)
s3 = session.client("s3")

def list_job_prefixes():
    resp = s3.list_objects_v2(Bucket=silver, Prefix="textract-results/", Delimiter="/")
    prefixes = []
    for pref in resp.get("CommonPrefixes", []):
        p = pref.get("Prefix", "")
        parts = p.split("/")
        if len(parts) >= 3:
            prefixes.append(parts[1])
    return prefixes

def list_keys_for_job(job):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=silver, Prefix=f"textract-results/{job}/"):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".json"):
                keys.append(obj["Key"])
    return keys

def download(key, dest_dir):
    dest = dest_dir / pathlib.Path(key).name
    dest_dir.mkdir(parents=True, exist_ok=True)
    s3.download_file(silver, key, dest.as_posix())
    try:
        data = json.loads(dest.read_text())
    except Exception:
        return
    src_bucket = data.get("source_bucket") or bronze
    src_key = data.get("source_file") or data.get("source_key")
    if not src_key:
        return
    # keep source tree per job
    src_dest = dest_dir / "sources" / src_key
    src_dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        s3.download_file(src_bucket, src_key, src_dest.as_posix())
    except Exception:
        pass

jobs = list_job_prefixes()
print(f"Found jobs: {jobs}")
for job in jobs:
    keys = list_keys_for_job(job)
    if not keys:
        continue
    sample = random.sample(keys, min(per_job, len(keys)))
    out_dir = sample_dir / job
    for k in sample:
        download(k, out_dir)
    print(f"Job {job}: sampled {len(sample)} of {len(keys)}")
print(f"Samples stored under {sample_dir}")
PY

if [[ "${DRY_RUN}" != "0" ]]; then
  echo "DRY_RUN=1 -> skipping deletions. Set DRY_RUN=0 to wipe old data."
  exit 0
fi

echo "Deleting old data from buckets..."
aws s3 rm "s3://${SILVER_BUCKET}/textract-results/" --recursive --profile "$PROFILE" --region "$REGION"
aws s3 rm "s3://${BRONZE_BUCKET}/" --recursive --profile "$PROFILE" --region "$REGION"

echo "Done. Re-upload sampled files from ${SAMPLE_DIR} to bronze as needed for fresh tests."
