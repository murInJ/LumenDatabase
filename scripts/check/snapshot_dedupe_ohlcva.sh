#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../env.sh"

SNAP_ROOT="${1:-data_snapshot_$(date -u +%Y%m%d)}"
echo "[snapshot] exporting deduped dataset to $SNAP_ROOT"

# 让视图存在
python - <<'PY'
import os
os.environ.setdefault("PYTHONPATH", os.getcwd()+"/src")
from database.lumen_database import LumenDatabase
db = LumenDatabase(os.environ["LUMEN_DB_FILE"], data_root=os.environ["LUMEN_DATA_ROOT"],
                   auto_discover_specs=True, quiet=False)
db.ensure_views("ohlcva", ["1d"], create_empty_ok=True)
db.close()
PY

if ! command -v duckdb >/dev/null 2>&1; then
  echo "[snapshot] 需要 duckdb CLI 才能一键导出，请先安装（或让我改成 Python 版）。"
  exit 1
fi

duckdb -c "
OPEN '${LUMEN_DB_FILE}';
PRAGMA threads=${LUMEN_CONCURRENCY};

WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY symbol, ts, interval ORDER BY ingest_ts DESC NULLS LAST) AS rn,
    EXTRACT(year FROM trading_day) AS year,
    LPAD(CAST(EXTRACT(month FROM trading_day) AS INT)::VARCHAR, 2, '0') AS month
  FROM ohlcva_1d_v
)
COPY (
  SELECT ts,trading_day,symbol,interval,open,high,low,close,volume,amount,source,ingest_ts,year,month
  FROM ranked
  WHERE rn=1
) TO '${SNAP_ROOT}/ohlcva/1d' (FORMAT PARQUET, PARTITION_BY (symbol, year, month));
"

echo "[snapshot] done -> $SNAP_ROOT"