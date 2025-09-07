# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Tuple

def normalize_cn_a_symbol(sym: str) -> Tuple[str, str]:
    """
    规范化 A 股代码：
    - 输入可为 "000001", "000001.SZ", "600000.SH" 等
    - 返回: (fetch_code, store_symbol)
        fetch_code  -> akshare.stock_zh_a_hist 需要的 6 位数字，如 "000001"
        store_symbol-> 入湖使用的规范符号，带交易所后缀，如 "000001.SZ"/"600000.SH"
                       若输入已带后缀则保留；否则按首位猜交易所(0/3=SZ, 6=SH)，其余默认 SZ
    """
    s = sym.strip().upper()
    if "." in s:
        code, exch = s.split(".", 1)
        code = code.zfill(6)
        if exch not in ("SZ", "SH"):
            exch = "SZ"
        return code, f"{code}.{exch}"
    # 无后缀：按常见规则推断
    code = s.zfill(6)
    if code.startswith(("0", "3")):
        exch = "SZ"
    elif code.startswith("6"):
        exch = "SH"
    else:
        exch = "SZ"
    return code, f"{code}.{exch}"
