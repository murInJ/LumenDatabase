from __future__ import annotations
from dataclasses import dataclass
from typing import AsyncIterator, Iterable, Optional, Dict, Any, List
import datetime as dt
import pandas as pd
import asyncio

@dataclass(frozen=True)
class FetchRequest:
    dataset: str          # 'ohlcva'
    interval: str         # '1d'
    symbols: List[str]
    start: dt.datetime    # UTC
    end: dt.datetime      # UTC
    options: Dict[str, Any] = None  # 透传：复权/调整等

class DataSource:
    name: str = "abstract"

    async def fetch_ohlcva(self, req: FetchRequest) -> AsyncIterator[pd.DataFrame]:
        """产生一个或多个DataFrame（按symbol/时间分片）。必须输出规范schema列"""
        raise NotImplementedError

    # 可选：能力声明
    def capabilities(self) -> Dict[str, Any]:
        return {"ohlcva": {"intervals": ["1d"], "tz": "UTC"}}

    # 通用并发/限流工具（子类可复用）
    _sema = asyncio.Semaphore(8)
