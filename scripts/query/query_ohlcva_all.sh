#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../env.sh"

START="${1:-2010-01-01}"
END="${2:-$(date -u +%F)}"
LIMIT="${3:-}"   # 可选：限制输出行数

echo "[query_ohlcva_all] start=$START end=$END data_root=$LUMEN_DATA_ROOT db=$LUMEN_DB_FILE"

START="$START" END="$END" LIMIT="${LIMIT:-}" \
python - <<'PY'
import os
from database.lumen_database import LumenDatabase

start  = os.environ.get("START")
end    = os.environ.get("END")
limit  = os.environ.get("LIMIT")

db = LumenDatabase(
    db_path=os.environ["LUMEN_DB_FILE"],
    data_root=os.environ["LUMEN_DATA_ROOT"],
    auto_discover_specs=True,
    ensure_views_on_init=False,
    extensions=("parquet",),
    quiet=True,
)

# 确保视图存在（无数据时自动放置占位文件）
db.ensure_views("ohlcva", variants=["1d"], create_empty_ok=True)

cols = [
    "ts","trading_day","symbol","interval",
    "open","high","low","close","volume","amount",
    "source","ingest_ts",
]

where = "ts >= CAST(? AS DATE) AND ts < CAST(? AS DATE) + INTERVAL 1 DAY"
params = [start, end]

lm = int(limit) if (limit or "").strip().isdigit() else None

df = db.select(
    "ohlcva",
    variant="1d",
    columns=cols,
    where=where,
    params=params,
    order_by="symbol, ts",
    limit=lm,
)

import sys
sys.stdout.write(df.to_csv(index=False))
PY
