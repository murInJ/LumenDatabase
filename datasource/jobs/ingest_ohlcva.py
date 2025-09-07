# -*- coding: utf-8 -*-
"""
datasource/jobs/ingest_ohlcva.py

将 A 股 OHLCVA 日线数据从数据源（默认 AKShare）拉取并入湖到 Parquet，
合并进度为单一总进度条（以 symbol 粒度计数），写文件/累计行数显示在同一进度条的 postfix。
支持全量/增量/自动三种抓取模式，随后创建/刷新 DuckDB 视图。

修复：统一 tz-naive / tz-aware，避免 TypeError: can't compare offset-naive and offset-aware datetimes
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set

import pandas as pd
from tqdm.auto import tqdm
from datetime import timezone  # 统一处理为 UTC-aware

from database.lumen_database import LumenDatabase
from database.utils import sql_literal
from datasource.base import FetchRequest
from datasource.planner import plan_ohlcva
from datasource.registry import get_source
from datasource.writer import write_ohlcva_parquet, log_manifest
from datasource.universe import (
    all_a_symbols,
    index_constituents,
    industry_constituents,
    concept_constituents,
)
from datasource.utils import normalize_cn_a_symbol


# ---------------------------- CLI & 参数解析 ----------------------------

def parse_dt(s: str) -> dt.datetime:
    """将字符串解析为 UTC 时间；允许 'YYYY-MM-DD' 或 'YYYY-MM-DD HH:MM:SS'"""
    return pd.Timestamp(s, tz="UTC").to_pydatetime()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ingest A-share OHLCVA (1d) into Parquet + DuckDB views with a single progress bar."
    )
    p.add_argument("--db", default="catalog.duckdb", help="DuckDB catalog path")
    p.add_argument("--data-root", default="data", help="Parquet data lake root")
    p.add_argument("--interval", default="1d", choices=["1d"], help="bar interval")

    # 符号 / 范围（互斥，必须至少提供一种）
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument(
        "--symbols",
        nargs="+",
        help="symbols like 000001.SZ 600000.SH (可混入不带后缀的6位代码，内部会规范化)",
    )
    g.add_argument("--universe", help="全市场别名：ALL_A | A_SHARE | CN_A")
    g.add_argument("--index", help="指数代码（如 000300 表示沪深300），将解析成份股")
    g.add_argument("--industry", help="东财行业名或板块代码（如 '小金属' 或 BK1027）")
    g.add_argument("--concept", help="东财概念名或板块代码（如 '绿色电力' 或 BK0715）")

    # 抓取模式
    p.add_argument(
        "--mode",
        choices=["full", "incremental", "auto"],
        default="auto",
        help=(
            "full: 完全按 [--start, --end] 抓取；"
            "incremental: 仅抓取每个 symbol 已有数据之后的增量；"
            "auto: 若已有数据则增量，否则全量。"
        ),
    )
    p.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="增量模式回看天数（>0 可覆盖供应商修订/跳分等情况）",
    )
    p.add_argument("--dry-run", action="store_true", help="仅展示解析/规划与进度，不实际落盘")

    p.add_argument("--start", type=parse_dt, required=True, help="UTC date/time, e.g. 2022-01-01")
    p.add_argument("--end", type=parse_dt, required=True, help="UTC date/time, e.g. 2022-12-31")
    return p


def _resolve_symbols(args) -> List[str]:
    """
    将 CLI 输入解析为 symbols（可能是 6 位代码或带 .SZ/.SH）。
    """
    if args.symbols:
        return args.symbols

    if args.universe:
        u = args.universe.strip().upper()
        if u in ("ALL_A", "A_SHARE", "CN_A"):
            return all_a_symbols()  # 返回 6 位代码
        raise ValueError(f"未知 universe: {args.universe}")

    if args.index:
        return index_constituents(args.index)

    if args.industry:
        return industry_constituents(args.industry)

    if args.concept:
        return concept_constituents(args.concept)

    raise ValueError("必须指定 symbols/universe/index/industry/concept 之一")


# ---------------------------- 增量规划工具 ----------------------------

def _storage_symbol(sym: str) -> str:
    """转为入湖存储符号（带 .SZ/.SH）"""
    return normalize_cn_a_symbol(sym)[1]


def _glob_for_symbol(data_root: str, symbol: str) -> str:
    """该 symbol 在数据湖中的 parquet 路径 pattern。"""
    base = data_root.rstrip("/")
    return f"{base}/ohlcva/1d/symbol={symbol}/year=*/month=*/part-*.parquet"


def latest_trading_day_for_symbol(db: LumenDatabase, data_root: str, symbol: str) -> Optional[pd.Timestamp]:
    """
    用 DuckDB 在该 symbol 的分区上计算已存在的最大 trading_day。
    若无文件或空表，返回 None。

    返回值统一为 UTC-aware 的 pandas.Timestamp，避免后续比较时报 tz 错误。
    """
    pattern = _glob_for_symbol(data_root, symbol)
    try:
        sql = f"SELECT max(trading_day) FROM read_parquet('{sql_literal(pattern)}');"
        row = db.query(sql)
        if not row or row[0][0] is None:
            return None
        ts = pd.to_datetime(row[0][0])
        if isinstance(ts, pd.Timestamp):
            ts = ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")
        else:
            ts = pd.Timestamp(ts, tz="UTC")
        return ts
    except Exception:
        return None


def build_incremental_groups(
    db: LumenDatabase,
    data_root: str,
    symbols: Iterable[str],
    user_start: dt.datetime,
    user_end: dt.datetime,
    mode: str = "auto",
    lookback_days: int = 0,
) -> Dict[pd.Timestamp, List[str]]:
    """
    计算每个 symbol 的实际起始日期，并按起始日期进行分组。
    - full: 统一使用 user_start
    - incremental: 从 (max(trading_day)+1 - lookback) 与 user_start 的较大者开始
    - auto: 有历史则按 incremental，无历史则 full
    """
    groups: Dict[pd.Timestamp, List[str]] = defaultdict(list)

    def _to_aware_utc(x) -> dt.datetime:
        """兜底：把任何 pandas/py 的 datetime 变成 UTC-aware 的 datetime.datetime。"""
        if isinstance(x, pd.Timestamp):
            x = x.tz_localize("UTC") if x.tz is None else x.tz_convert("UTC")
            return x.to_pydatetime()
        if isinstance(x, dt.datetime):
            return x if x.tzinfo is not None else x.replace(tzinfo=timezone.utc)
        return pd.Timestamp(x, tz="UTC").to_pydatetime()

    # 入参也做一次保险（幂等）
    user_start = _to_aware_utc(user_start)
    user_end = _to_aware_utc(user_end)

    for sym in symbols:
        store_sym = _storage_symbol(sym)
        if mode == "full":
            real_start = user_start
        else:
            last = latest_trading_day_for_symbol(db, data_root, store_sym)
            if mode == "incremental":
                real_start = user_start if last is None else (pd.Timestamp(last) + pd.Timedelta(days=1))
            else:  # auto
                real_start = user_start if last is None else (pd.Timestamp(last) + pd.Timedelta(days=1))

        # 全部统一为 UTC-aware 的 python datetime 再参与后续运算/比较
        real_start = _to_aware_utc(real_start)

        if lookback_days > 0:
            real_start = real_start - dt.timedelta(days=int(lookback_days))
        if real_start < user_start:
            real_start = user_start
        if real_start > user_end:
            continue

        groups[pd.Timestamp(real_start.date())].append(store_sym)

    return groups


# ---------------------------- 主流程（单进度条） ----------------------------

async def run(args):
    # 解析 symbols（可能是 6 位或带 .SZ/.SH）；入湖前统一转带后缀
    raw_symbols = _resolve_symbols(args)
    symbols = [_storage_symbol(s) for s in raw_symbols]
    print(f"[ingest] resolved symbols: {len(symbols)}")

    # 初始化数据库（自动发现 DatasetSpec）
    db = LumenDatabase(args.db, data_root=args.data_root, auto_discover_specs=True, quiet=False)

    # 计算增量/全量的实际抓取起点，并按起点分组
    groups = build_incremental_groups(
        db,
        args.data_root,
        symbols,
        args.start,
        args.end,
        mode=args.mode,
        lookback_days=max(0, int(args.lookback_days)),
    )

    total_symbols_to_fetch = sum(len(v) for v in groups.values())
    skipped = len(symbols) - total_symbols_to_fetch
    print(f"[ingest] plan groups: {len(groups)} | to_fetch: {total_symbols_to_fetch} | skipped(up-to-date): {skipped}")

    # 无需抓取也保证视图存在
    if total_symbols_to_fetch == 0:
        db.ensure_views("ohlcva", variants=["1d"], create_empty_ok=True)
        print("[ingest] nothing to do; views ensured.")
        db.close()
        return

    files_written, rows_written = 0, 0
    processed_once: Set[str] = set()  # 每个 symbol 只计一次

    # 单一总进度条（以 symbol 为单位）
    pbar = tqdm(total=total_symbols_to_fetch, desc="ingest", unit="sym", leave=True)
    pbar.set_postfix(files=files_written, rows=rows_written)

    def _update_postfix():
        pbar.set_postfix(files=files_written, rows=rows_written)

    # 逐组（相同起点）规划与抓取
    for group_start, group_syms in groups.items():
        plan = plan_ohlcva(
            group_syms,
            start=pd.Timestamp(group_start, tz="UTC").to_pydatetime(),
            end=args.end,
            interval=args.interval,
            policy={"primary": "akshare", "batch_size": 64},
        )

        for task in plan.tasks:
            src = get_source(task.source)

            req = FetchRequest(
                dataset="ohlcva",
                interval=task.interval,
                symbols=task.symbols,
                start=task.start,
                end=task.end,
                options=task.options or {},
            )

            seen_in_this_task: Set[str] = set()

            async for df in src.fetch_ohlcva(req):
                if df is None or df.empty:
                    continue

                # 进度：以 symbol 粒度计一次
                sym = str(df["symbol"].iloc[0])
                if sym not in processed_once:
                    processed_once.add(sym)
                    seen_in_this_task.add(sym)
                    pbar.update(1)

                if not args.dry_run:
                    path = write_ohlcva_parquet(df, args.data_root)
                    if path:
                        files_written += 1
                        rows_written += int(len(df))
                        _update_postfix()

            # 该批任务结束后，若仍有未产出数据的 symbol（例如上游空返回），也要推进进度
            remaining = set(task.symbols) - processed_once
            if remaining:
                processed_once.update(remaining)
                pbar.update(len(remaining))
                _update_postfix()

    pbar.close()

    # 创建/刷新视图（本地尚无文件时将由 ohlcva.SPEC.ensure_ready 创建占位）
    db.ensure_views("ohlcva", variants=["1d"], create_empty_ok=True)

    print(f"[ingest] files_written={files_written} rows_written={rows_written} skipped={skipped}")
    db.close()


# ---------------------------- 入口 ----------------------------

if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    asyncio.run(run(args))
