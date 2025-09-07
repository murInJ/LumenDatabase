# -*- coding: utf-8 -*-
"""
datasource/config/settings.py
极简配置：
- Settings: 数据根目录、并发
- AKShareSettings: 复权、速率、超时、重试
- load_settings(): 支持用环境变量覆盖默认
"""
from __future__ import annotations
from dataclasses import dataclass, field
import os

@dataclass
class AKShareSettings:
    adjust: str = ""                 # "", "qfq", "hfq"
    rate_limit_per_sec: float = 8.0
    timeout: float = 20.0
    retries: int = 2

@dataclass
class Settings:
    data_root: str = "data"
    concurrency: int = 8
    # 关键修复：用 default_factory 避免可变默认值错误
    akshare: AKShareSettings = field(default_factory=AKShareSettings)

def _getenv_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default

def _getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return default

def load_settings() -> Settings:
    s = Settings()
    s.data_root = os.getenv("LUMEN_DATA_ROOT", s.data_root)
    s.concurrency = _getenv_int("LUMEN_CONCURRENCY", s.concurrency)

    ak = s.akshare  # 拿到实例再覆盖
    ak.adjust = os.getenv("AKSHARE_ADJUST", ak.adjust)
    ak.rate_limit_per_sec = _getenv_float("AKSHARE_RATE", ak.rate_limit_per_sec)
    ak.timeout = _getenv_float("AKSHARE_TIMEOUT", ak.timeout)
    ak.retries = _getenv_int("AKSHARE_RETRIES", ak.retries)
    s.akshare = ak
    return s
