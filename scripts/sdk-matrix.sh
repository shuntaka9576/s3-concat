#!/usr/bin/env bash
# Run the test suite against several @aws-sdk/client-s3 versions, then
# restore the original lockfile state.
#
# Usage:
#   scripts/sdk-matrix.sh [test|e2e]
#
#   test  - run `pnpm test` (floci, default)
#   e2e   - run `pnpm e2e`  (real AWS S3; requires ambient credentials)
#
# Override versions with: SDK_VERSIONS="3.726.0 latest" scripts/sdk-matrix.sh
#
# Safe-chain note: aikido-pnpm spins up its registry proxy at the *top*
# shell-function level, so env vars set inside this script never reach it.
# When Safe-chain is active, invoke the matrix from the calling shell with:
#
#   SAFE_CHAIN_MINIMUM_PACKAGE_AGE_HOURS=24 \
#   SAFE_CHAIN_MINIMUM_PACKAGE_AGE_EXCLUSIONS="@aws-sdk/*,@smithy/*" \
#     pnpm test:sdk-matrix
#
# Otherwise newly-cut @smithy/* sub-deps will trip the minimum-package-age
# guard and the matrix will abort mid-run.
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

# Default matrix: every 100th minor in v3 from 3.300.0, plus `latest`.
# Earlier versions are skipped because their transitive deps trip the
# supply-chain minimum-package-age guard. Missing versions
# (3.500.0, 3.900.0 were never published) fall back to the nearest below.
read -r -a SDK_VERSIONS <<<"${SDK_VERSIONS:-3.300.0 3.400.0 3.499.0 3.600.0 3.700.0 3.800.0 3.899.0 3.1000.0 latest}"

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
