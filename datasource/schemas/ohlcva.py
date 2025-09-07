# 规范列 & dtypes & 分区策略（与占位文件一致）
CANONICAL_COLUMNS = [
    "ts","trading_day","symbol","interval",
    "open","high","low","close","volume","amount","source","ingest_ts"
]
DTYPES = {
    "ts": "datetime64[ns]",
    "trading_day": "datetime64[ns]",
    "symbol": "object",
    "interval": "object",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "amount": "float64",
    "source": "object",
    "ingest_ts": "datetime64[ns]",
}
# 分区列（对应 database_spec/ohlcva.py 的目录布局）
PARTITIONS = ["symbol","year","month"]  # 注意：写入时需从 ts/trading_day 派生 year/month
