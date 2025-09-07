#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"

echo "[clean] remove $LUMEN_DATA_ROOT/ohlcva"
rm -rf "$LUMEN_DATA_ROOT/ohlcva" || true

# 清理 manifest（需要 duckdb CLI；若没有可跳过）
if command -v duckdb >/dev/null 2>&1; then
  duckdb -c "
  OPEN '${LUMEN_DB_FILE}';
  CREATE TABLE IF NOT EXISTS ingest_manifest(dataset TEXT, file_path TEXT, rows BIGINT, created_at TIMESTAMP, extra JSON);
  DELETE FROM ingest_manifest WHERE dataset='ohlcva';"
else
  echo "[clean] duckdb CLI 不存在，跳过 manifest 清理（不影响功能）"
fi
