#!/usr/bin/env bash
# Run the test suite against several @aws-sdk/client-s3 versions, then
# restore the original lockfile state.
#
# Usage:
#   scripts/sdk-matrix.sh [test|e2e]
#
#   test  - run `pnpm test` (floci, default)
#   e2e   - run `pnpm e2e`  (real AWS S3; requires ambient creds)
#
# Override versions with: SDK_VERSIONS="3.726.0 latest" scripts/sdk-matrix.sh
set -euo pipefail

MODE="${1:-test}"
case "$MODE" in
  test) PNPM_CMD=(pnpm test) ;;
  e2e)  PNPM_CMD=(pnpm e2e) ;;
  *)
    echo "Usage: $0 [test|e2e]" >&2
    exit 2
    ;;
esac

read -r -a SDK_VERSIONS <<<"${SDK_VERSIONS:-3.726.0 3.1070.0 latest}"

BACKUP_DIR=$(mktemp -d)
cp package.json pnpm-lock.yaml "$BACKUP_DIR/"

restore() {
  printf '\n--- Restoring original package.json / pnpm-lock.yaml ---\n'
  cp "$BACKUP_DIR/package.json" "$BACKUP_DIR/pnpm-lock.yaml" .
  pnpm install --frozen-lockfile >/dev/null
  rm -rf "$BACKUP_DIR"
}
trap restore EXIT

passed=()
failed=()
for v in "${SDK_VERSIONS[@]}"; do
  printf '\n=== @aws-sdk/client-s3@%s (mode=%s) ===\n' "$v" "$MODE"
  pnpm add -D --save-exact "@aws-sdk/client-s3@${v}" >/dev/null
  if "${PNPM_CMD[@]}"; then
    passed+=("$v")
  else
    failed+=("$v")
  fi
done

printf '\n--- summary ---\n'
[ ${#passed[@]} -gt 0 ] && printf 'PASS: %s\n' "${passed[*]}"
[ ${#failed[@]} -gt 0 ] && printf 'FAIL: %s\n' "${failed[*]}"

if [ ${#failed[@]} -gt 0 ]; then
  exit 1
fi
