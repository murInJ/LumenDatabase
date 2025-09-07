from __future__ import annotations
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Any
from datasource.base import FetchRequest
from datasource.registry import list_sources

@dataclass
class FetchTask:
    source: str
    symbols: List[str]
    start: dt.datetime
    end: dt.datetime
    interval: str
    options: Dict[str, Any]

@dataclass
class FetchPlan:
    tasks: List[FetchTask]

def plan_ohlcva(symbols: List[str], start: dt.datetime, end: dt.datetime, interval: str="1d",
                policy: Dict[str, Any]=None) -> FetchPlan:
    """
    简单策略：优先使用配置的主源（默认 akshare），按批切符号并发抓取；
    将来可加入：覆盖率评分、价格/量对齐、延迟/失败率等指标。
    """
    policy = policy or {}
    primary = (policy.get("primary") or "akshare")
    batch = int(policy.get("batch_size") or 50)
    tasks: List[FetchTask] = []
    for i in range(0, len(symbols), batch):
        tasks.append(FetchTask(primary, symbols[i:i+batch], start, end, interval, {}))
    return FetchPlan(tasks)
