# -*- coding: utf-8 -*-
"""
最小校验与规范化工具：
- enforce_columns: 选择并按顺序输出规范列，缺失列补空
- enforce_dtypes: 强制 dtypes（尽量 safe-cast）
- drop_pk_duplicates: 删除 (symbol, ts, interval) 主键重复
- basic_sanity_checks: 基础检查（价格和成交量/额的合法性）
- finalize_ohlcva: 一站式规范化 + 去重 + 基本检查
"""
from __future__ import annotations
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np

# 统一 schema（与数据库占位 Parquet 保持一致）
CANONICAL_COLUMNS: List[str] = [
    "ts", "trading_day", "symbol", "interval",
    "open", "high", "low", "close",
    "volume", "amount",
    "source", "ingest_ts",
]

DTYPES: Dict[str, str] = {
    "ts": "datetime64[ns, UTC]",
    "trading_day": "datetime64[ns]",       # 本地无时区（自然日）
    "symbol": "object",
    "interval": "object",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "amount": "float64",
    "source": "object",
    "ingest_ts": "datetime64[ns, UTC]",
}

PRIMARY_KEY = ["symbol", "ts", "interval"]

def enforce_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CANONICAL_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[CANONICAL_COLUMNS]
    return out

def enforce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # 时间列
    if "ts" in out:
        out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    if "trading_day" in out:
        out["trading_day"] = pd.to_datetime(out["trading_day"], errors="coerce").dt.normalize()
    if "ingest_ts" in out:
        out["ingest_ts"] = pd.to_datetime(out["ingest_ts"], utc=True, errors="coerce")

    # 数值列
    for col in ["open","high","low","close","volume","amount"]:
        if col in out:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # 文本列
    for col in ["symbol","interval","source"]:
        if col in out:
            out[col] = out[col].astype("object")

    return out

def drop_pk_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset=PRIMARY_KEY)

def basic_sanity_checks(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    做最小化健康检查：价格<=0、最高/最低关系、成交量/额负值
    返回 (清洗后 df, 统计报告)
    """
    out = df.copy()
    report = {"neg_or_zero_price": 0, "hi_lo_inconsistent": 0, "neg_volume_amount": 0}

    # 价格不能 <= 0（保留缺失）
    mask_bad = (out[["open","high","low","close"]] <= 0).any(axis=1)
    report["neg_or_zero_price"] = int(mask_bad.fillna(False).sum())
    out = out[~mask_bad.fillna(False)]

    # high >= low
    mask_bad2 = (out["high"] < out["low"])
    report["hi_lo_inconsistent"] = int(mask_bad2.fillna(False).sum())
    out = out[~mask_bad2.fillna(False)]

    # 成交量/额不得为负
    mask_bad3 = ((out["volume"] < 0) | (out["amount"] < 0))
    report["neg_volume_amount"] = int(mask_bad3.fillna(False).sum())
    out = out[~mask_bad3.fillna(False)]

    # 排序（可选）
    out = out.sort_values(["symbol","ts","interval"]).reset_index(drop=True)
    return out, report

def finalize_ohlcva(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    一站式：选列→dtypes→去重→检查
    """
    out = enforce_columns(df)
    out = enforce_dtypes(out)
    out = drop_pk_duplicates(out)
    out, report = basic_sanity_checks(out)
    return out, report
