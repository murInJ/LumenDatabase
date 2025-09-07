#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

python - <<'PY'
import os
os.environ.setdefault("PYTHONPATH", os.getcwd()+"/src")
from database.lumen_database import LumenDatabase
db = LumenDatabase(os.environ["LUMEN_DB_FILE"], data_root=os.environ["LUMEN_DATA_ROOT"],
                   auto_discover_specs=True, quiet=False)
db.ensure_views("ohlcva", ["1d"], create_empty_ok=True)
print("== row count ==")
print(db.query_df("SELECT count(*) AS rows FROM ohlcva_1d_v"))
print("== sample range per symbol ==")
print(db.query_df("""
  SELECT symbol, min(trading_day) AS start_d, max(trading_day) AS end_d, count(*) AS n
  FROM ohlcva_1d_v
  GROUP BY 1
  ORDER BY 1
  LIMIT 20
"""))
db.close()
PY
