# database/stock.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb
import pandas as pd

# 从你之前实现的 setting.py 引入
from database.config.setting import GetDatabaseConfig
from datasource.stock import GetStockList


def _resolve_stock_db_path(cfg: Dict[str, Any]) -> Path:
    """
    从配置里解析出 stock_db_root，并标准化为绝对路径。
    优先顺序：
      1) cfg['database']['stock_db_root']
      2) cfg['paths']['stock_db_root']
      3) cfg['stock_db_root'] / cfg['stock_db'] / cfg['stockdb']
    若给的是目录而不是文件名，则默认拼接 'stock.duckdb'
    """
    candidates = []
    # database.stock_db_root
    if isinstance(cfg.get("database"), dict):
        val = cfg["database"].get("stock_db_root")
        if val:
            candidates.append(val)
    # paths.stock_db_root
    if isinstance(cfg.get("paths"), dict):
        val = cfg["paths"].get("stock_db_root")
        if val:
            candidates.append(val)
    # 顶层一些常见命名
    for k in ("stock_db_root", "stock_db", "stockdb"):
        if cfg.get(k):
            candidates.append(cfg[k])

    if not candidates:
        raise KeyError(
            "stock_db_root not found in config. Expected one of: "
            "database.stock_db_root / paths.stock_db_root / stock_db_root / stock_db / stockdb"
        )

    raw = next((c for c in candidates if c), None)
    p = Path(raw).expanduser()
    # 若是目录，补默认文件名
    if not p.suffix:
        p = p / "stock.duckdb"
    return p.resolve()


def create_stockdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    """
    创建（初始化）DuckDB 文件并建表。仅在文件不存在时调用。
    当前只建一张表：stock_info(stock_id, symbol, name)
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    # 采用 SEQUENCE，避免对旧版本 DuckDB 的 IDENTITY 语法依赖
    con.execute("CREATE SEQUENCE IF NOT EXISTS stock_info_id_seq START 1;")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_info (
            stock_id BIGINT PRIMARY KEY DEFAULT nextval('stock_info_id_seq'),
            symbol   VARCHAR NOT NULL UNIQUE,
            name     VARCHAR NOT NULL
        );
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_stock_info_symbol ON stock_info(symbol);")
    return con



class StockDB:
    """
    基于 DuckDB 的简易股票信息库。
    - 初始化自动读取配置，解析 stock_db_root
    - 确保父目录存在
    - 若 DB 文件不存在则创建，并初始化表结构
    - 否则直接连接
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self.cfg: Dict[str, Any] = config or GetDatabaseConfig()
        self.db_path: Path = _resolve_stock_db_path(self.cfg)

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        if not self.db_path.exists():
            # 首次创建
            self.con: duckdb.DuckDBPyConnection = create_stockdb(self.db_path)
        else:
            # 直接连接现有 DB
            self.con = duckdb.connect(str(self.db_path))

    def close(self) -> None:
        if getattr(self, "con", None) is not None:
            self.con.close()
            self.con = None

    # 方便 with 语法使用
    def __enter__(self) -> "StockDB":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def list_stocks(self, update: bool = False) -> pd.DataFrame:
        """
        返回全部股票信息（symbol, name, stock_id）。
        - 若表为空或 update=True，则先从数据源拉取并更新 stock_info，再返回。
        """
        # 判断表是否为空
        is_empty = len(self.con.execute("SELECT 1 FROM stock_info LIMIT 1;").fetchall()) == 0

        if update or is_empty:
            try:
                df = GetStockList(source="akshare")
            except Exception as e:
                # 如果表本来就空而且拉取失败，向外抛出；否则打印警告后继续返回已有数据
                if is_empty:
                    raise
                else:
                    print(f"[list_stocks] 更新失败，返回已有数据：{e}")
            else:
                if df is not None and not df.empty:
                    # 兜底列名映射
                    if not {'symbol', 'name'}.issubset(df.columns):
                        rename_map = {}
                        if '代码' in df.columns: rename_map['代码'] = 'symbol'
                        if '名称' in df.columns: rename_map['名称'] = 'name'
                        df = df.rename(columns=rename_map)

                    df2 = df[['symbol', 'name']].copy()
                    df2['symbol'] = df2['symbol'].astype(str).str.strip()
                    df2['name'] = df2['name'].astype(str).str.strip()
                    df2 = df2[df2['symbol'] != ''].drop_duplicates(subset=['symbol'], keep='last')

                    if not df2.empty:
                        # 批量 upsert
                        self.con.register('tmp_stock_df', df2)
                        self.con.execute("""
                            MERGE INTO stock_info AS t
                            USING tmp_stock_df AS s
                            ON t.symbol = s.symbol
                            WHEN MATCHED THEN UPDATE SET name = s.name
                            WHEN NOT MATCHED THEN INSERT (symbol, name) VALUES (s.symbol, s.name);
                        """)
                        self.con.unregister('tmp_stock_df')

        # 统一返回查询结果
        return self.con.execute(
            "SELECT stock_id, symbol, name FROM stock_info ORDER BY symbol;"
        ).fetchdf()


if __name__ == "__main__":
    db = StockDB()
    print(db.list_stocks())

