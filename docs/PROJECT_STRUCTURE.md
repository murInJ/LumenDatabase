# 项目结构

本项目围绕 **DuckDB + Parquet 数据湖** 搭建的“多源数据接入 → 规范化 → 入湖 → 视图查询”结构。以下为目录与文件职责说明。

## 顶层目录

```
LumenDatabase/
  database/            # 读层：DuckDB 封装、数据集（DatasetSpec）与视图管理
  datasource/          # 写层/接入层：数据源插件、规划器、校验与入湖写入
  scripts/             # 批处理脚本：全量/增量/校验/清理/快照
  data/                # Parquet 数据湖（默认根，可自定义）
  catalog.duckdb       # DuckDB catalog 文件（可自定义路径/名称）
  README.md            #（可选）项目说明
  docs/                #（可选）文档目录（本页建议放置于此）
```

---

## `database/`（读层）

```
database/
  lumen_database.py     # DuckDB 通用读层：连接/PRAGMA/扩展；DatasetSpec 注册与 ensure_view(s)/select
  utils.py              # SQL/标识符转义、glob 根解析、spec 自动发现等工具
  database_spec/
    __init__.py
    ohlcva.py          # OHLCVA 数据集规范：视图名、Parquet 路径模式、ensure_ready(占位文件)
```

* `lumen_database.py`：提供 `LumenDatabase` 与 `DatasetSpec`；自动发现 `database_spec/` 下的 `SPEC` 并注册；统一管理视图读入口。
* `database_spec/ohlcva.py`：定义 `ohlcva` 的目录布局与视图名；在本地无数据时生成零行占位 Parquet，确保视图可用。

---

## `datasource/`（写层 / 数据接入）

```
datasource/
  __init__.py
  base.py               # DataSource 抽象；FetchRequest 定义
  registry.py           # 数据源注册表
  planner.py            # 抓取规划（源选择、批次切片）
  writer.py             # 写入 Parquet：按 symbol/year/month 分区；原子落盘；记录 manifest
  validation.py         # 规范化/类型校验/主键去重/基础健康检查
  utils.py              # A 股代码规范化等工具（000001 ↔ 000001.SZ/600000.SH）
  universe.py           # “全市场/指数/行业/概念” → 动态解析 symbols
  config/
    __init__.py
    settings.py         # 全局配置加载（并发、速率、复权、超时、重试等，支持环境变量覆盖）
  connectors/
    __init__.py
    akshare/
      __init__.py
      ohlcva.py        # AKShare 适配：调用 stock_zh_a_hist → 产出规范化 OHLCVA DataFrame
  jobs/
    __init__.py
    ingest_ohlcva.py   # CLI 作业：解析范围/规划 → 拉数 → 校验 → 入湖 → 刷新视图（含进度与增量模式）
```

* `base.py`：统一数据源接口，约定输出“规范化 DataFrame”。
* `connectors/akshare/ohlcva.py`：实现 A 股日线 OHLCVA 拉取与标准化。
* `validation.py`：与湖表 schema 对齐的列顺序与类型、去重及基本健康检查。
* `writer.py`：分区写入（`symbol/year/month`）、原子落盘（`.tmp → .parquet`）、`ingest_manifest` 记录。
* `universe.py`：从“全市场/指数/行业/概念板块”接口动态获取 symbol 列表。
* `jobs/ingest_ohlcva.py`：可执行入口（含进度可视化与全量/增量/自动模式）。

---

## `scripts/`（批处理脚本）

```
scripts/
  fetch/
    ingest_full_all_a.sh           # 全市场全量
    ingest_full_yearly.sh          # 按年全量
    ingest_incremental_universe.sh # 全市场增量/自动（支持回看天数）
    ingest_index.sh                # 指数成份（全量/增量）
  check/
    verify_ohlcva.sh               # 快速验数与时间范围检查
    clean_ohlcva.sh                # 清理 OHLCVA 分区与 manifest
    snapshot_dedupe_ohlcva.sh      # 可选：导出“去重后的黄金快照”到新根
  env.sh                         # 通用环境变量（并发、速率、数据根、DB 文件等）

```

---

## `data/`（Parquet 数据湖，默认根）

```
data/
  ohlcva/
    1d/
      symbol=000001.SZ/
        year=2024/
          month=01/
            part-*.parquet
      symbol=600000.SH/
        year=2024/
          month=01/
            part-*.parquet
```

* 目录分区：`symbol / year / month`（与读取视图及写入策略一致）。
* 真实数据与占位文件（零行）共享同一 schema，保证视图可创建与稳定查询。

---

## 顶层产物

```
catalog.duckdb   # DuckDB catalog（存放内置元信息与可选的 manifest 表）
```
