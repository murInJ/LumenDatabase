#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../env.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $(basename "$0") <SYMBOL> [START:YYYY-MM-DD] [END:YYYY-MM-DD] [LIMIT]"
  echo "Example: $(basename "$0") 000001.SZ 2022-01-01 2022-12-31 100"
  exit 1
fi

SYMBOL="$1"
START="${2:-2010-01-01}"
END="${3:-$(date -u +%F)}"
LIMIT="${4:-}"   # 可选：限制输出行数

echo "[query_ohlcva_symbol] symbol=$SYMBOL start=$START end=$END data_root=$LUMEN_DATA_ROOT db=$LUMEN_DB_FILE"

SYMBOL="$SYMBOL" START="$START" END="$END" LIMIT="${LIMIT:-}" \
python - <<'PY'
import os, sys
from database.lumen_database import LumenDatabase

symbol = os.environ.get("SYMBOL")
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

where = "symbol=? AND ts >= CAST(? AS DATE) AND ts < CAST(? AS DATE) + INTERVAL 1 DAY"
params = [symbol, start, end]

# 可选 LIMIT
lm = int(limit) if (limit or "").strip().isdigit() else None

df = db.select(
    "ohlcva",
    variant="1d",
    columns=cols,
    where=where,
    params=params,
    order_by="ts",
    limit=lm,
)

# 输出 CSV 到 stdout
import sys
sys.stdout.write(df.to_csv(index=False))
PY
