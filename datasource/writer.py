# -*- coding: utf-8 -*-
from __future__ import annotations
import os, hashlib, json
from pathlib import Path
import pandas as pd
from database.lumen_database import LumenDatabase

# 分区列（与 database_spec/ohlcva.py 保持一致）
PARTITIONS = ["symbol", "year", "month"]

def _derive_partitions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year"] = df["trading_day"].dt.year.astype("int32")
    df["month"] = df["trading_day"].dt.month.astype("int16").map(lambda m: f"{m:02d}")
    return df

def _make_filename(df: pd.DataFrame) -> str:
    # 简单幂等：symbol + [start,end] + hash
    sym = df["symbol"].iloc[0]
    start = pd.to_datetime(df["ts"].min()).strftime("%Y%m%d")
    end = pd.to_datetime(df["ts"].max()).strftime("%Y%m%d")
    h = hashlib.md5(f"{sym}-{start}-{end}".encode()).hexdigest()[:8]
    return f"part-{start}-{end}-{h}.parquet"

def write_ohlcva_parquet(df: pd.DataFrame, data_root: str) -> Path | None:
    """
    目录：<data_root>/ohlcva/1d/symbol=<sym>/year=<YYYY>/month=<MM>/part-*.parquet
    返回：最后写出的文件路径（若无数据返回 None）
    """
    if df is None or df.empty:
        return None

    df = _derive_partitions(df)
    sym = df["symbol"].iloc[0]
    base = Path(data_root).joinpath("ohlcva", "1d", f"symbol={sym}")

    paths: list[Path] = []
    for (year, month), g in df.groupby(["year", "month"], sort=True):
        out_dir = base / f"year={int(year)}" / f"month={month}"
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = _make_filename(g)
        tmp = out_dir / (fname + ".tmp")
        out = out_dir / fname
        g.drop(columns=["year", "month"]).to_parquet(tmp, index=False)
        os.replace(tmp, out)  # 原子替换，避免半成品
        paths.append(out)

    return paths[-1] if paths else None

def log_manifest(db: LumenDatabase, dataset: str, file_path: str, rows: int, extra: dict | None = None):
    """
    记录一次入湖写入。
    表结构：
      dataset TEXT,
      file_path TEXT,
      rows BIGINT,
      created_at TIMESTAMP DEFAULT now(),
      extra JSON
    """
    extra_json = json.dumps(extra or {}, ensure_ascii=False, separators=(",", ":"))
    db.execute("""
    CREATE TABLE IF NOT EXISTS ingest_manifest(
        dataset TEXT,
        file_path TEXT,
        rows BIGINT,
        created_at TIMESTAMP DEFAULT now(),
        extra JSON
    );
    """)
    db.execute(
        "INSERT INTO ingest_manifest(dataset, file_path, rows, extra) VALUES (?,?,?,?);",
        [dataset, file_path, int(rows), extra_json],
    )
