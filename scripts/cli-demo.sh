#!/usr/bin/env bash
# Generates a multi-GiB demo dataset on S3 and runs `s3-concat --dry-run`
# to surface the multipart upload plan (UploadPartCopy + streamed UploadPart).
#
# Scenario:
#   a.bin = 5 GiB + 2 MiB  -> 1 UploadPartCopy (5 GiB) + 2 MiB tail spills into a stream
#   b.bin = 6 GiB          -> stream picks up b's head, then 2x UploadPartCopy on b
#   c.bin = 3 MiB          -> small, tail-end stream
#
# Usage:
#   BUCKET=my-bucket scripts/cli-demo.sh [prefix]
#
# Notes:
#   - PUT requests to S3 are free, but ~11 GiB has to actually transfer.
#   - Source objects are left in place so you can re-run --dry-run cheaply.
#     Clean up with: aws s3 rm s3://$BUCKET/$PREFIX/ --recursive

set -euo pipefail

BUCKET=${BUCKET:?BUCKET env var is required}
PREFIX=${1:-cli-demo-$(date +%s)}

CLI=${CLI:-./dist/cli.mjs}

MIB=$((1024 * 1024))

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "Generating local files in $TMP..."
dd if=/dev/zero of="$TMP/a.bin" bs=$MIB count=$((5 * 1024 + 2)) status=none
dd if=/dev/zero of="$TMP/b.bin" bs=$MIB count=$((6 * 1024))     status=none
dd if=/dev/zero of="$TMP/c.bin" bs=$MIB count=3                  status=none
ls -lh "$TMP"

echo ""
echo "Uploading to s3://$BUCKET/$PREFIX/src/ ..."
aws s3 sync "$TMP" "s3://$BUCKET/$PREFIX/src/"

echo ""
echo "Source listing:"
aws s3 ls "s3://$BUCKET/$PREFIX/src/"

echo ""
echo "Dry-run plan:"
"$CLI" \
  --src-bucket "$BUCKET" \
  --dst-bucket "$BUCKET" \
  --src-prefix "$PREFIX/src" \
  --dst-prefix "$PREFIX/out" \
  --concat-file-name merged.bin \
  --join-order keyNameAsc \
  --dry-run

echo ""
echo "Done. Source kept at s3://$BUCKET/$PREFIX/src/"
echo "Clean up with: aws s3 rm s3://$BUCKET/$PREFIX/ --recursive"
