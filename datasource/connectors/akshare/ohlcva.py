# -*- coding: utf-8 -*-
"""
AKShare A股日线 OHLCVA 适配（稳健版）：
- 并发抓取但为每个 symbol 增加重试 / 指数退避 / 超时
- 单只失败返回 None，不会让整个任务中断
"""
from __future__ import annotations
from typing import AsyncIterator, Optional
import datetime as dt
import pandas as pd
import asyncio, time, random

from datasource.base import DataSource, FetchRequest
from datasource.validation import finalize_ohlcva
from datasource.config.settings import load_settings
from datasource.utils import normalize_cn_a_symbol

# 懒加载 akshare，避免未用时引入依赖
_ak = None
def _akshare():
    global _ak
    if _ak is None:
        import akshare as ak
        _ak = ak
    return _ak

def _to_yyyymmdd(ts: dt.datetime) -> str:
    """接受 naive 或 tz-aware 的 datetime，统一转为 UTC 后格式化 YYYYMMDD。"""
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.strftime("%Y%m%d")

def _localize_to_utc_midnight(trading_day_series: pd.Series) -> pd.Series:
    """
    将交易日视为上海时区 00:00，再转 UTC 作为 ts。
    """
    s = pd.to_datetime(trading_day_series, errors="coerce")
    if getattr(s.dt, "tz", None) is None:
        s = s.dt.tz_localize("Asia/Shanghai", nonexistent="shift_forward", ambiguous="NaT")
    else:
        s = s.dt.tz_convert("Asia/Shanghai")
    return s.dt.tz_convert("UTC")

class AKShareOHLCVA(DataSource):
    name = "akshare"

    async def fetch_ohlcva(self, req: FetchRequest) -> AsyncIterator[pd.DataFrame]:
        """
        按 symbol 并发抓取；内部对每个 symbol 加入重试/退避/超时；失败返回 None。
        """
        loop = asyncio.get_event_loop()
        settings = load_settings().akshare
        sem = asyncio.Semaphore(max(1, int(load_settings().concurrency)))

        async def _task(user_sym: str):
            fetch_code, store_symbol = normalize_cn_a_symbol(user_sym)
            async with sem:
                # 简单速率限制：每个并发槽位之间 sleep
                await asyncio.sleep(1.0 / max(1.0, settings.rate_limit_per_sec))
                try:
                    return await loop.run_in_executor(
                        None, self._fetch_symbol,
                        fetch_code, store_symbol, req.start, req.end, req.interval,
                        settings.adjust, float(settings.timeout), int(settings.retries)
                    )
                except Exception:
                    # 单只失败不抛出，返回 None 让上层推进进度
                    return None

        tasks = [_task(sym) for sym in req.symbols]
        for fut in asyncio.as_completed(tasks):
            try:
                df = await fut
            except Exception:
                df = None
            if df is None or df.empty:
                continue
            yield df

    # ---------- 实际调用 AKShare（带重试/退避） ----------
    def _fetch_symbol(
        self,
        fetch_code: str,
        store_symbol: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str = "1d",
        adjust: str = "",
        timeout: float = 20.0,
        retries: int = 2,
    ) -> Optional[pd.DataFrame]:
        """
        以重试/退避包装 akshare.stock_zh_a_hist 调用。
        - retries: 失败重试次数（总尝试=1+retries）
        - timeout: 单次请求超时（秒）
        """
        if interval.lower() != "1d":
            return None

        ak = _akshare()
        start_s, end_s = _to_yyyymmdd(start), _to_yyyymmdd(end)

        last_err: Optional[Exception] = None
        for attempt in range(retries + 1):
            try:
                # 兼容不同版本签名
                try:
                    raw = ak.stock_zh_a_hist(
                        symbol=fetch_code, period="daily",
                        start_date=start_s, end_date=end_s,
                        adjust=adjust, timeout=timeout
                    )
                except TypeError:
                    raw = ak.stock_zh_a_hist(
                        symbol=fetch_code,
                        start_date=start_s, end_date=end_s,
                        adjust=adjust, timeout=timeout
                    )

                if raw is None or raw.empty:
                    return None

                df = raw.rename(columns={
                    "日期": "trading_day",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                    "成交额": "amount",
                })

                # 生成 ts、补充元数据
                df["ts"] = _localize_to_utc_midnight(df["trading_day"])
                df["symbol"] = store_symbol
                df["interval"] = "1d"
                df["source"] = self.name
                df["ingest_ts"] = pd.Timestamp.now(tz="UTC")

                # 统一列顺序 & dtypes & 去重 & 基本校验
                df, _ = finalize_ohlcva(df)
                return df

            except Exception as e:
                last_err = e
                if attempt < retries:
                    # 指数退避 + 抖动
                    backoff = min(60.0, (2.0 ** attempt) + random.uniform(0, 1.0))
                    time.sleep(backoff)
                    continue
                else:
                    # 最终失败
                    return None

        # 理论走不到
        return None
