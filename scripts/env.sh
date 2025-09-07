#!/usr/bin/env bash
# 通用环境变量（可被外部覆盖）
export PYTHONPATH="${PYTHONPATH:-$PWD}"
export LUMEN_DATA_ROOT="${LUMEN_DATA_ROOT:-/mnt/win/datasets/lumen/data}"
export LUMEN_DB_FILE="${LUMEN_DB_FILE:-/mnt/win/datasets/lumen/catalog.duckdb}"

# 并发与速率（按需调整）
export LUMEN_CONCURRENCY="${LUMEN_CONCURRENCY:-4}"
export AKSHARE_RATE="${AKSHARE_RATE:-8}"
export AKSHARE_TIMEOUT="${AKSHARE_TIMEOUT:-20}"
export AKSHARE_RETRIES="${AKSHARE_RETRIES:-3}"
export AKSHARE_ADJUST="${AKSHARE_ADJUST:-hfq}"  # "", "qfq", "hfq"
