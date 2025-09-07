# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List
import pandas as pd

# 懒加载 akshare
_ak = None
def _akshare():
    global _ak
    if _ak is None:
        import akshare as ak
        _ak = ak
    return _ak

def all_a_symbols() -> List[str]:
    """
    获取全市场 A 股代码（六位数字），后续你可转存储符号如 000001.SZ/600000.SH
    来源：东财实时 A 股全表 stock_zh_a_spot_em
    """
    ak = _akshare()
    df = ak.stock_zh_a_spot_em()
    # 文档说明该接口返回所有沪深京 A 股并含“代码”列。:contentReference[oaicite:3]{index=3}
    codes = df["代码"].astype(str).str.zfill(6).tolist()
    return codes

def index_constituents(index_code: str) -> List[str]:
    """
    指数成份股（最新）：优先用 index_stock_cons（或其 csindex 版本）
    """
    ak = _akshare()
    try:
        df = ak.index_stock_cons(symbol=index_code)   # 例如 "000300"
    except Exception:
        # 备选：csindex 来源或新浪变体，视本机 akshare 版本可用性决定
        df = ak.index_stock_cons_csindex(symbol=index_code)
    # 这些接口一般提供成份“成分券代码/品种代码”等字段
    for col in ["品种代码", "成分券代码", "code", "代码"]:
        if col in df.columns:
            return df[col].astype(str).str.zfill(6).tolist()
    raise RuntimeError("无法从指数成份返回中识别代码列")

def industry_constituents(name_or_code: str) -> List[str]:
    """
    行业板块成份：东财行业板块成份股
    """
    ak = _akshare()
    df = ak.stock_board_industry_cons_em(symbol=name_or_code)
    # 一般列名为“代码”
    return df["代码"].astype(str).str.zfill(6).tolist()

def concept_constituents(name_or_code: str) -> List[str]:
    """
    概念板块成份：东财概念板块成份股
    """
    ak = _akshare()
    df = ak.stock_board_concept_cons_em(symbol=name_or_code)
    return df["代码"].astype(str).str.zfill(6).tolist()
