# -*- coding: utf-8 -*-
"""
OHLCVA 数据集的 DatasetSpec 定义：
- 已实现：1d（日线）→ <data_root>/ohlcva/1d/symbol=*/year=*/month=*/part-*.parquet
- ensure_ready：当本地没有任何匹配文件时，自动创建一个“空 Parquet 占位文件”，
                以便上层能顺利创建视图和跑通查询链路（返回空结果）。
"""

from __future__ import annotations

import uuid
from pathlib import Path

import duckdb  # 仅类型提示/运行时 COPY 用

from database.lumen_database import DatasetSpec


# ---------- 1) 目录与视图命名 ----------

def _glob_1d(dataset_root: str) -> str:
    root = dataset_root.rstrip("/")
    return f"{root}/1d/symbol=*/year=*/month=*/part-*.parquet"


def _ohlcva_glob_builder(variant: str, dataset_root: str) -> str:
    v = (variant or "").lower().strip()
    if v == "1d":
        return _glob_1d(dataset_root)
    raise ValueError(f"Unsupported ohlcva variant: {variant!r}. Only '1d' implemented for now.")


def _view_name_builder(variant: str) -> str:
    v = (variant or "").strip()
    return f"ohlcva_{v}_v" if v else "ohlcva_v"


# ---------- 2) 无文件时的占位构建 ----------

def _ensure_ready_1d(dataset_root: str, con: duckdb.DuckDBPyConnection) -> None:
    """
    创建一个空的 OHLCVA:1d 占位文件：
      <dataset_root>/1d/symbol=__placeholder__/year=1970/month=01/part-empty-<id>.parquet
    仅在目录无任何匹配文件时由上层调用。
    """
    base = dataset_root.rstrip("/")
    out_dir = Path(base) / "1d" / "symbol=__placeholder__" / "year=1970" / "month=01"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"part-empty-{uuid.uuid4().hex[:8]}.parquet"

    # 用 DuckDB 生成带完整 schema 的 0 行 Parquet
    con.execute("""
    CREATE TEMP TABLE _empty_ohlcva_1d (
      ts TIMESTAMP,
      trading_day DATE,
      symbol TEXT,
      interval TEXT,
      open DOUBLE,
      high DOUBLE,
      low DOUBLE,
      close DOUBLE,
      volume DOUBLE,
      amount DOUBLE,
      source TEXT,
      ingest_ts TIMESTAMP
    );
    """)

    # 先把路径里的单引号转成 SQL 字面量可用的两个单引号
    path_sql = out_file.as_posix().replace("'", "''")
    con.execute(f"COPY _empty_ohlcva_1d TO '{path_sql}' (FORMAT PARQUET);")
    con.execute("DROP TABLE _empty_ohlcva_1d;")


def _ensure_ready(variant: str, dataset_root: str, con: duckdb.DuckDBPyConnection) -> None:
    v = (variant or "").lower().strip()
    if v == "1d":
        _ensure_ready_1d(dataset_root, con)
    else:
        # 其它粒度未来实现；当前不做处理
        pass


# ---------- 3) 导出 SPEC ----------

SPEC = DatasetSpec(
    name="ohlcva",
    variants=("1d",),  # 未来扩展分钟线后再加入 '1m','5m','15m','30m','1h'
    glob_builder=_ohlcva_glob_builder,
    view_name_builder=_view_name_builder,
    ensure_ready=_ensure_ready,  # 关键：把“占位构建”逻辑交给 spec
)


# 可选自检：python -m database.database_spec.ohlcva
if __name__ == "__main__":  # pragma: no cover
    print("SPEC:", SPEC)
    print("1d glob:", _ohlcva_glob_builder("1d", "/abs/path/to/data/ohlcva"))
