# -*- coding: utf-8 -*-
"""
utils.py
Lumen 的小工具集合：
- SQL 标识符/字面量转义
- URL/路径判断
- 从 parquet glob 中提取“根目录”
- 自动发现 database_spec/ 下导出的 SPEC / SPECS
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from typing import Iterable, List

from types import ModuleType


def quote_ident(ident: str) -> str:
    """
    SQL 标识符安全引用。
    遇到空格、斜杠、点等特殊字符时用双引号包裹，并对内部双引号转义。
    """
    if any(c in ident for c in ' -./\\`"\'' ):
        return f'"{ident.replace("\"", "\"\"")}"'
    return ident


def sql_literal(s: str) -> str:
    """SQL 字面量转义单引号。"""
    return s.replace("'", "''")


def is_remote_url(p: str) -> bool:
    """是否是 s3/http/https 等远端 URL。"""
    low = p.lower()
    return low.startswith("s3://") or low.startswith("http://") or low.startswith("https://")


def glob_root_dir(glob: str) -> str:
    """
    从一个 parquet glob（含 * ? [ ]）中提取“稳定的前缀目录”（用于存在性快速检查）。
    例：/a/b/symbol=*/year=*/part-*.parquet -> /a/b/symbol=*/year=  -> parent() -> /a/b/symbol=*/
    """
    cut = len(glob)
    for ch in ["*", "?", "["]:
        pos = glob.find(ch)
        if pos != -1:
            cut = min(cut, pos)
    prefix = glob[:cut]
    return str(Path(prefix).parent)


def discover_specs(package_name: str = "database_spec"):
    """
    自动发现并导入 package_name 下的模块，收集模块中的 `SPEC`（单个）或 `SPECS`（可迭代）变量。
    返回一个“DatasetSpec”对象的列表（保持导入顺序）。
    - 若包不存在，返回空列表
    - 导入异常会被吞并（避免影响主流程）；你可以在上层选择打印警告
    """
    specs = []
    try:
        pkg = importlib.import_module(package_name)
    except Exception:
        return specs  # 包不存在或导入失败，忽略

    def collect_from_module(mod: ModuleType):
        if hasattr(mod, "SPEC"):
            specs.append(getattr(mod, "SPEC"))
        if hasattr(mod, "SPECS"):
            try:
                for s in getattr(mod, "SPECS"):
                    specs.append(s)
            except Exception:
                pass

    # 先收集包根（__init__.py）里的
    collect_from_module(pkg)

    # 再扫描子模块
    pkg_path = getattr(pkg, "__path__", None)
    if not pkg_path:
        return specs

    for m in pkgutil.walk_packages(pkg_path, prefix=package_name + "."):
        name = m.name
        try:
            mod = importlib.import_module(name)
            collect_from_module(mod)
        except Exception:
            # 单个模块失败不影响整体
            continue

    return specs
