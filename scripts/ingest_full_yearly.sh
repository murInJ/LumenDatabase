#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

FROM_YEAR="${1:-2010}"
TO_YEAR="${2:-$(date -u +%Y)}"

for y in $(seq "$FROM_YEAR" "$TO_YEAR"); do
  s="${y}-01-01"
  e="${y}-12-31"
  echo "[ingest_full_yearly] $y  ($s ~ $e)"
  python -m datasource.jobs.ingest_ohlcva \
    --universe ALL_A \
    --mode full \
    --start "$s" --end "$e" \
    --db "$LUMEN_DB_FILE" --data-root "$LUMEN_DATA_ROOT"
done
