#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

INDEX_CODE="${1:?用法: $0 000300 [START] [END] [MODE]}"
START="${2:-2010-01-01}"
END="${3:-$(date -u +%F)}"
MODE="${4:-auto}"              # full | incremental | auto
LOOKBACK_DAYS="${LOOKBACK_DAYS:-1}"

echo "[ingest_index] index=$INDEX_CODE $START ~ $END mode=$MODE"
python -m datasource.jobs.ingest_ohlcva \
  --index "$INDEX_CODE" \
  --mode "$MODE" --lookback-days "$LOOKBACK_DAYS" \
  --start "$START" --end "$END" \
  --db "$LUMEN_DB_FILE" --data-root "$LUMEN_DATA_ROOT"
