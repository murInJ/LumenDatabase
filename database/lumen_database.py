# -*- coding: utf-8 -*-
"""
lumen_database.py
极简而可扩展的 LumenDatabase：
- 连接/关闭、PRAGMA、扩展加载、执行/查询、表/视图存在性等“基础能力”
- 新增：data_root（统一数据根目录，按数据集名各自一个子文件夹）
- 新增：数据集注册表（DatasetSpec），从 database.database_spec/ 自动发现并注册
- 新增：ensure_view(s) / drop_dataset_views / select 等统一入口，隐藏具体读取细节
- 保持通用：数据集专属行为（如占位文件创建）通过 DatasetSpec.ensure_ready 扩展

运行自测：
    export PYTHONPATH="$PWD/src"
    python -m database.lumen_database
"""

from __future__ import annotations

import contextlib
import glob as _pyglob
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import duckdb  # pip install duckdb

from database.utils import (
    quote_ident,
    sql_literal,
    is_remote_url,
    glob_root_dir,
    discover_specs,
)

# --------------------------- 类型定义 ---------------------------

PathLike = Union[str, Path]
Row = Tuple
Params = Union[Sequence, dict]

# 构造 parquet 路径 glob 的回调：
#   参数1：variant（如 '1d'、'raw'），参数2：dataset_root（= data_root/<dataset_name>）
GlobBuilder = Callable[[str, str], str]
# 生成视图名的回调：参数是 variant
ViewNameBuilder = Callable[[str], str]
# 当本地无匹配文件时，用于“准备/构建”数据根目录（如创建占位 parquet）
#   参数：variant、dataset_root、duckdb 连接
EnsureReadyCallable = Callable[[str, str, duckdb.DuckDBPyConnection], None]


@dataclass(frozen=True)
class DatasetSpec:
    """
    描述“如何读取一个数据集”的最小信息：
    - name：数据集名（用于 data_root/<name>）
    - variants：该数据集的变体列表（ohlcva: ('1d','1m'...)；news: ('raw',)）
    - glob_builder(variant, dataset_root) -> str：返回 DuckDB read_parquet 的 glob
    - view_name_builder(variant) -> str：自定义视图名（默认 <name>_<variant>_v 或 <name>_v）
    - ensure_ready(variant, dataset_root, con)：可选，当本地无匹配文件时，由数据集自己“准备”目录/占位文件
    """
    name: str
    variants: Tuple[str, ...]
    glob_builder: GlobBuilder
    view_name_builder: Optional[ViewNameBuilder] = None
    ensure_ready: Optional[EnsureReadyCallable] = None

    def view_name(self, variant: Optional[str]) -> str:
        if self.view_name_builder:
            return self.view_name_builder(variant or "")
        return f"{self.name}_{variant}_v" if (variant or "") else f"{self.name}_v"


# --------------------------- 主类实现 ---------------------------

class LumenDatabase:
    """
    目标：保持“basic”的基础能力不变，同时内聚 Lumen 的数据集注册与统一读取入口。
    所有数据集专属逻辑（如占位文件、目录布局）均下沉到各自的 DatasetSpec 中。
    """

    def __init__(
            self,
            db_path: PathLike = "catalog.duckdb",
            *,
            read_only: bool = False,
            create_if_missing: bool = True,
            threads: Optional[int] = None,
            extensions: Optional[Iterable[str]] = None,  # 默认：('parquet',)；S3/HTTP 需 ('parquet','httpfs')
            auto_connect: bool = True,
            quiet: bool = True,
            # Lumen 扩展能力：
            data_root: Optional[PathLike] = None,  # 统一的数据根目录（每个 spec 一个子文件夹）
            auto_discover_specs: bool = True,  # 启动时自动扫描 database.database_spec/ 并注册 SPEC/SPECS
            ensure_views_on_init: bool = False,  # 启动后立即为各 spec 创建视图（若目录不存在则交由 spec.ensure_ready）
    ) -> None:
        # ---- 连接配置 ----
        self.db_path = Path(db_path) if db_path != ":memory:" else db_path  # type: ignore
        self.read_only = read_only
        self.create_if_missing = create_if_missing
        self.threads = threads
        self.extensions = tuple(extensions) if extensions is not None else ("parquet",)
        self.quiet = quiet

        self.con: Optional[duckdb.DuckDBPyConnection] = None

        # ---- Lumen 配置 ----
        self._data_root: Optional[str] = str(data_root) if data_root is not None else None
        self._registry: Dict[str, DatasetSpec] = {}

        if auto_connect:
            self.connect()

        # ---- 启动期：自动发现/注册/视图占位（可选）----
        if auto_discover_specs:
            for spec in discover_specs("database.database_spec"):
                self.register_dataset(spec)

        if ensure_views_on_init:
            for name, spec in list(self._registry.items()):
                try:
                    self.ensure_views(name, variants=spec.variants or None, create_empty_ok=True)
                except Exception as e:
                    if not self.quiet:
                        print(f"[LumenDatabase] ensure_views on init warn for '{name}': {e}")

    # ---------------- 生命周期 ----------------

    def connect(self) -> "LumenDatabase":
        if self.con is not None:
            return self

        # :memory:
        if self.db_path == ":memory:":
            self.con = duckdb.connect(self.db_path)  # type: ignore
            self._init_env()
            return self

        # file-based
        assert isinstance(self.db_path, Path)
        if self.read_only:
            if not self.db_path.exists():
                raise FileNotFoundError(f"duckdb 文件不存在（只读模式）：{self.db_path}")
            self.con = duckdb.connect(str(self.db_path), read_only=True)
        else:
            if not self.db_path.exists() and not self.create_if_missing:
                raise FileNotFoundError(f"duckdb 文件不存在，且不允许自动创建：{self.db_path}")
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self.con = duckdb.connect(str(self.db_path), read_only=False)

        self._init_env()
        return self

    def close(self) -> None:
        if self.con is not None:
            try:
                self.con.close()
            finally:
                self.con = None

    # ---------------- 基础能力 ----------------

    def db_exists(self) -> bool:
        if self.db_path == ":memory:":
            return True
        assert isinstance(self.db_path, Path)
        return self.db_path.exists()

    def execute(self, sql: str, params: Optional[Params] = None) -> None:
        self._ensure_conn()
        self.con.execute(sql) if params is None else self.con.execute(sql, params)

    def executemany(self, sql: str, seq_of_params) -> None:
        self._ensure_conn()
        self.con.executemany(sql, seq_of_params)

    def query(self, sql: str, params: Optional[Params] = None) -> List[Row]:
        self._ensure_conn()
        cur = self.con.execute(sql, params) if params is not None else self.con.execute(sql)
        return cur.fetchall()

    def query_df(self, sql: str, params: Optional[Params] = None):
        self._ensure_conn()
        try:
            import pandas as pd  # noqa: F401
        except Exception as e:
            raise RuntimeError("query_df() 需要 pandas，请先 `pip install pandas`") from e
        cur = self.con.execute(sql, params) if params is not None else self.con.execute(sql)
        return cur.df()

    def table_exists(self, name: str) -> bool:
        self._ensure_conn()
        sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema IN ('main','temp')
          AND lower(table_name) = lower(?)
          AND lower(table_type) = 'base table'
        LIMIT 1;
        """
        return self.con.execute(sql, [name]).fetchone() is not None  # type: ignore

    def view_exists(self, name: str) -> bool:
        self._ensure_conn()
        sql = """
        SELECT 1
        FROM information_schema.views
        WHERE table_schema IN ('main','temp')
          AND lower(table_name) = lower(?)
        LIMIT 1;
        """
        return self.con.execute(sql, [name]).fetchone() is not None  # type: ignore

    def relation_exists(self, name: str) -> bool:
        return self.table_exists(name) or self.view_exists(name)

    def list_tables(self) -> List[str]:
        self._ensure_conn()
        sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema IN ('main','temp')
          AND lower(table_type)='base table'
        ORDER BY 1;
        """
        return [r[0] for r in self.con.execute(sql).fetchall()]  # type: ignore

    def list_views(self) -> List[str]:
        self._ensure_conn()
        sql = """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema IN ('main','temp')
        ORDER BY 1;
        """
        return [r[0] for r in self.con.execute(sql).fetchall()]  # type: ignore

    @contextlib.contextmanager
    def transaction(self):
        self._ensure_conn()
        try:
            self.con.execute("BEGIN;")
            yield
            self.con.execute("COMMIT;")
        except Exception:
            self.con.execute("ROLLBACK;")
            raise

    # ---------------- Lumen 扩展：data_root & spec 注册 ----------------

    @property
    def data_root(self) -> Optional[str]:
        return self._data_root

    def set_data_root(self, root: PathLike) -> None:
        self._data_root = str(root)

    def dataset_root(self, dataset_name: str) -> str:
        """
        返回某个数据集的专属根目录：<data_root>/<dataset_name>
        """
        if not self._data_root:
            raise ValueError("data_root 未设置，请先 set_data_root() 或在初始化时传入。")
        return f"{self._data_root.rstrip('/')}/{dataset_name}"

    def register_dataset(self, spec: DatasetSpec) -> None:
        """
        注册/覆盖一个数据集的读取规则。仅登记，不创建视图。
        """
        self._registry[spec.name] = spec

    def has_dataset(self, name: str) -> bool:
        return name in self._registry

    def list_datasets(self) -> List[str]:
        return sorted(self._registry.keys())

    # ---------------- 视图：创建/删除/查询 ----------------

    def ensure_view(
            self,
            dataset: str,
            variant: Optional[str] = None,
            *,
            create_empty_ok: bool = True,
    ) -> str:
        """
        为 (dataset, variant) 创建/替换视图，返回视图名。
        若本地路径无匹配文件且 create_empty_ok=True：
          - 若 spec.ensure_ready 存在：调用其准备本地目录/占位文件
          - 否则保持原样（交由 DuckDB 报错或你自行先写入数据）
        """
        self._ensure_conn()
        if dataset not in self._registry:
            raise KeyError(f"尚未注册数据集：{dataset}")
        spec = self._registry[dataset]

        if spec.variants and (variant is None):
            raise ValueError(f"数据集 '{dataset}' 需要 variant，候选：{spec.variants}")
        if (not spec.variants) and variant is not None:
            raise ValueError(f"数据集 '{dataset}' 不需要 variant。")

        dataset_root = self.dataset_root(spec.name)
        view_name = spec.view_name(variant)
        iv = (variant or "").strip()
        glob = spec.glob_builder(iv, dataset_root)

        # 本地存在性与空匹配处理（远端 URL 跳过）
        if not is_remote_url(glob):
            root_dir = Path(glob_root_dir(glob))
            root_dir.mkdir(parents=True, exist_ok=True)

            if create_empty_ok and not self._glob_has_matches(glob) and spec.ensure_ready:
                # 交给数据集自定义逻辑准备（例如创建占位 parquet）
                spec.ensure_ready(iv, dataset_root, self.con)
                # 再次检查是否已有匹配文件
                # （若仍无匹配，继续走视图创建——可能由 DuckDB 抛错，便于暴露目录配置问题）

                # 不做强制失败，保留 DuckDB 的错误以便定位
                pass

        # 创建/替换视图
        self.execute(f"""
        CREATE OR REPLACE VIEW {quote_ident(view_name)} AS
        SELECT * FROM read_parquet('{sql_literal(glob)}');
        """)
        return view_name

    def ensure_views(
            self,
            dataset: str,
            variants: Optional[Iterable[str]] = None,
            *,
            create_empty_ok: bool = True,
    ) -> List[str]:
        """
        为一个数据集批量创建/替换视图；variants 为空时默认用 spec.variants。
        """
        if dataset not in self._registry:
            raise KeyError(f"尚未注册数据集：{dataset}")
        spec = self._registry[dataset]
        created: List[str] = []
        if spec.variants:
            use = list(variants) if variants is not None else list(spec.variants)
            for v in use:
                created.append(self.ensure_view(dataset, v, create_empty_ok=create_empty_ok))
        else:
            created.append(self.ensure_view(dataset, None, create_empty_ok=create_empty_ok))
        return created

    def drop_dataset_views(self, dataset: str) -> None:
        if dataset not in self._registry:
            return
        spec = self._registry[dataset]
        targets = [spec.view_name(v) for v in (spec.variants or (None,))]
        for v in targets:
            if self.view_exists(v):
                self.execute(f"DROP VIEW {quote_ident(v)};")

    def select(
            self,
            dataset: str,
            *,
            variant: Optional[str] = None,
            columns: Union[str, Iterable[str]] = "*",
            where: Optional[str] = None,
            params: Optional[Iterable] = None,
            order_by: Optional[str] = None,
            limit: Optional[int] = None,
    ):
        """
        统一查询入口（隐藏视图名）。适合 80% 简单场景。
        """
        if dataset not in self._registry:
            raise KeyError(f"尚未注册数据集：{dataset}")
        vname = self._registry[dataset].view_name(variant)
        if not self.view_exists(vname):
            raise RuntimeError(f"视图尚未创建：{vname}，请先 ensure_view/ensure_views。")

        cols = columns if isinstance(columns, str) else ", ".join(columns)
        parts = [f"SELECT {cols} FROM {quote_ident(vname)}"]
        if where:
            parts.append(f"WHERE {where}")
        if order_by:
            parts.append(f"ORDER BY {order_by}")
        if limit is not None:
            parts.append(f"LIMIT {int(limit)}")
        sql = "\n".join(parts)
        return self.query_df(sql, list(params) if params is not None else None)

    # ---------------- 内部：初始化/保障 ----------------

    def _init_env(self) -> None:
        self._ensure_conn()
        # 线程数
        threads = self._decide_threads(self.threads)
        self.con.execute(f"PRAGMA threads={threads};")
        # 扩展
        for ext in self.extensions:
            try:
                self.con.execute(f"INSTALL {ext};")
                self.con.execute(f"LOAD {ext};")
            except Exception as e:
                if not self.quiet:
                    print(f"[LumenDatabase] 扩展 '{ext}' 加载失败：{e}")

    @staticmethod
    def _decide_threads(threads: Optional[int]) -> int:
        if threads is not None:
            return max(1, int(threads))
        try:
            import multiprocessing
            return max(1, multiprocessing.cpu_count())
        except Exception:
            return 4

    def _ensure_conn(self) -> None:
        if self.con is None:
            raise RuntimeError("DuckDB 连接未建立，请先调用 connect()。")

    @staticmethod
    def _glob_has_matches(pattern: str) -> bool:
        try:
            return len(_pyglob.glob(pattern, recursive=True)) > 0
        except Exception:
            return False


# --------------------------- 快速自测 __main__ ---------------------------
if __name__ == "__main__":
    """
    运行方式（从项目根目录）：
        export PYTHONPATH="$PWD/src"
        python -m database.lumen_database
    环境变量（可选）：
        LUMEN_DATA_ROOT  覆盖 data_root，默认 'data'
        LUMEN_DB_FILE    覆盖 duckdb 文件名，默认 'catalog.duckdb'
    """
    import os
    import traceback

    data_root = os.environ.get("LUMEN_DATA_ROOT", "data")
    dbfile = os.environ.get("LUMEN_DB_FILE", "catalog.duckdb")

    print("[selftest] DuckDB version:", duckdb.__version__)
    print("[selftest] data_root    :", data_root)
    print("[selftest] db_file      :", dbfile)

    db = LumenDatabase(
        db_path=dbfile,
        data_root=data_root,
        auto_discover_specs=True,  # 从 database.database_spec 自动发现 SPEC
        ensure_views_on_init=False,  # 启动时不强制建视图
        extensions=("parquet",),  # 如需 S3/HTTP：("parquet","httpfs")
        quiet=False,
    )

    print("[selftest] db_exists     :", db.db_exists())
    print("[selftest] datasets(reg) :", db.list_datasets())

    # 若未发现到 ohlcva，尝试手动注册（容错）
    if "ohlcva" not in db.list_datasets():
        try:
            from database.database_spec.ohlcva import SPEC as OHLCVA_SPEC

            db.register_dataset(OHLCVA_SPEC)
            print("[selftest] registered builtin 'ohlcva' SPEC")
        except Exception as e:
            print("[selftest][warn] failed to import builtin SPEC:", repr(e))

    # 确保日线视图：无文件则由 ohlcva.SPEC.ensure_ready 创建占位 Parquet
    try:
        created = db.ensure_views("ohlcva", variants=["1d"], create_empty_ok=True)
        print("[selftest] ensure_views(ohlcva,1d):", created)
    except Exception as e:
        print("[selftest][error] ensure_views failed:", repr(e))
        traceback.print_exc()

    print("[selftest] views         :", db.list_views())

    # 试着查询（可能为空结果，但视图应可查询）
    try:
        df = db.select(
            "ohlcva",
            variant="1d",
            columns=["ts", "symbol", "close"],
            where="symbol=? AND ts BETWEEN ? AND ?",
            params=["000001.SZ", "2022-01-01", "2022-12-31"],
            order_by="ts",
            limit=5,
        )
        print("[selftest] query rows    :", 0 if df is None else len(df))
        if df is not None:
            print(df.head())
    except Exception as e:
        print("[selftest][warn] select failed:", repr(e))
        traceback.print_exc()

    db.close()
    print("[selftest] done.")
