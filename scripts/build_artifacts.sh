#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"

mkdir -p "$DIST_DIR"

echo "Building Lambda ZIPs..."
rm -f "$DIST_DIR/bronze-bucket-file-reader.zip" "$DIST_DIR/silver-bucket-file-creator.zip"
(
  cd "$ROOT_DIR"
  zip -j -q "$DIST_DIR/bronze-bucket-file-reader.zip" "lambdas/bronze-bucket-file-reader/lambda_s3_event_driven.py"
  zip -j -q "$DIST_DIR/silver-bucket-file-creator.zip" "lambdas/silver-bucket-file-creator/lambda_function.py"
)

echo "Building portal tarball from tracked files..."
rm -f "$DIST_DIR/portal.tar.gz"
(
  cd "$ROOT_DIR"
  # Use tracked files only (excludes local .env / *.db / node_modules etc).
  git ls-files -z portal | tar --null -czf "$DIST_DIR/portal.tar.gz" --files-from -
)

echo "Artifacts ready:"
ls -lh "$DIST_DIR/bronze-bucket-file-reader.zip" "$DIST_DIR/silver-bucket-file-creator.zip" "$DIST_DIR/portal.tar.gz"

