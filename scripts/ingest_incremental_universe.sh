#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

# 默认跑最近一段窗口；auto: 已有历史→增量；无历史→全量
START="${1:-$(date -u -d '30 days ago' +%F)}"
END="${2:-$(date -u +%F)}"
MODE="${MODE:-auto}"           # 可改成 incremental
LOOKBACK_DAYS="${LOOKBACK_DAYS:-1}"

echo "[ingest_incremental_universe] $START ~ $END mode=$MODE lookback=$LOOKBACK_DAYS"
python -m datasource.jobs.ingest_ohlcva \
  --universe ALL_A \
  --mode "$MODE" --lookback-days "$LOOKBACK_DAYS" \
  --start "$START" --end "$END" \
  --db "$LUMEN_DB_FILE" --data-root "$LUMEN_DATA_ROOT"
