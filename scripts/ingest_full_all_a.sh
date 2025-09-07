#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

START="${1:-2010-01-01}"
END="${2:-$(date -u +%F)}"

echo "[ingest_full_all_a] start=$START end=$END data_root=$LUMEN_DATA_ROOT db=$LUMEN_DB_FILE"
python -m datasource.jobs.ingest_ohlcva \
  --universe ALL_A \
  --mode full \
  --start "$START" --end "$END" \
  --db "$LUMEN_DB_FILE" --data-root "$LUMEN_DATA_ROOT"
