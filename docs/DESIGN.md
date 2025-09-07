# 架构设计与模块协同思路

面向“多源数据接入 → 规范化 → 入湖 → 视图查询”的数据产品闭环，整体采用 **分层解耦 + 规范 Schema + 数据湖（Parquet）+ 轻量查询引擎（DuckDB）** 的架构。核心目标：

* 多源并行、可切换、可扩展
* 入湖幂等、可追溯、可修订
* 读写分离、快速联机分析
* 统一数据契约（Schema 与目录规范）

---

## 一、分层与职责

1. 数据接入层（datasource）

* connectors/：各数据源适配器（如 akshare），只负责“拉数 + 字段映射 → 产出规范化 DataFrame”。
* validation.py：强制列顺序与类型、主键去重、基础质量检查，确保入湖一致性。
* writer.py：分区落盘（symbol/year/month）、原子写入（.tmp → .parquet）、记录 ingest\_manifest（血缘与批次日志）。
* planner.py：抓取规划（源选择、批次拆分、并发度）；后续可扩展多源评分与容灾切换。
* universe.py：将“全市场/指数/行业/概念”等业务范围解析为具体 symbols。
* config/settings.py：统一读取配置与环境变量（并发、速率、复权、超时、重试等）。

2. 数据读层（database）

* lumen\_database.py：轻量 DuckDB 封装；`DatasetSpec` 注册中心；`ensure_views()` 将 Parquet 目录映射为逻辑视图；`select()` 提供统一查询入口。
* database\_spec/：为每个数据集定义视图名与目录规范，并提供 `ensure_ready`（无数据时写入零行占位 Parquet，保证视图随时可用）。
* utils.py：SQL/标识符转义、glob 根解析、自动发现 SPEC。

3. 调度作业（jobs）

* ingest\_ohlcva.py：端到端任务（解析范围 → 规划 → 并行拉数 → 校验 → 入湖 → 刷新视图），支持 full/incremental/auto 模式与进度可视化。

4. 脚本编排（scripts）

* 承载常用批处理场景（全量/增量/分年/指数/清理/验数/去重快照），统一环境变量，方便 CI/CD 与运维。

---

## 二、数据契约（Schema 与目录）

1. 统一列（OHLCVA）

* ts（UTC 时间戳）、trading\_day（本地交易日）、symbol、interval（如 1d）
* open、high、low、close、volume、amount
* source（来源标识）、ingest\_ts（入湖时间）

2. 目录规范（分区）

* data/ohlcva/1d/symbol=<SYMBOL>/year=<YYYY>/month=<MM>/part-\*.parquet
* 读层视图统一命名：ohlcva\_1d\_v（由 SPEC 生成）

3. 契约落地

* connectors 必须产出规范化 DataFrame（validation 最终兜底）。
* writer 只接受契约一致的数据，保证湖内一致性与可剪枝性（分区/统计）。

---

## 三、核心协同流程

A) 读取范围 → 解算符号
universe/index/industry/concept/symbols → symbols 列表（可能为 6 位代码或带后缀）。

B) 规划 → 并发抓取
planner 将 symbols 切批，决定数据源与并发；jobs 持续消费任务，调用 connectors。

C) 规范化 → 质量校验
connectors 输出初步映射；validation.enforce\_columns/dtypes、去重与基础 sanity checks。

D) 入湖 → 清单（manifest）
writer 按分区原子写入；记录 ingest\_manifest（dataset、file\_path、rows、时间、extra）。

E) 刷新视图 → 在线查询
database.ensure\_views() 将 Parquet 目录映射为 DuckDB 视图；上层统一用 select/SQL 读取。

F) 增量与修订
jobs 根据本地分区 `max(trading_day)` 计算实际起点；支持 lookback 回看 N 天覆盖供应商修订；长期可定期出“去重黄金快照”。

---

## 四、并发与容错

* 并发：planner 侧批次拆分 + connectors 内部 Semaphore/速率限制，兼顾吞吐与对端友好。
* 容错：connectors 对单只股票失败可重试/跳过；任务整体不中断；writer 原子落盘避免半成品。
* 幂等：文件名含 symbol + 时间范围 + hash；manifest 记录可回溯；必要时用快照任务导出去重集。

---

## 五、扩展点与演进

1. 多源融合

* registry 增加新源（如 tushare/yahoo）；planner 评分（覆盖率、新鲜度、稳定性、成本）决定主备链路。
* 在同一 schema 下 UNION 多源；用 source/ingest\_ts 进行行级选择或优先级压制。

2. 新数据集

* 在 database\_spec/ 新增 SPEC，定义目录与视图；在 datasource 中补 writer/validation/connectors；jobs 编写对应 ingest 作业。
* 通过 lumen\_database 自动发现 SPEC，无需改动读层代码。

3. 高级治理

* 质量监控：在 validation 基础上增加异常检测（停牌日零量、价格跳变、重复日等）与告警。
* 生命周期：冷/热分层，旧分区重写压缩；快照发布为稳定消费入口。
* 元数据：后续引入 securities 主表，联动上市/退市信息与板块标签。

---

## 六、关键设计取舍

* 读写分离：读层只做视图映射与查询；写层包办接入与落盘，边界清晰。
* 规范优先：强制统一 schema 与目录，降低多源接入与下游使用复杂度。
* 懒视图 + 占位：无数据时亦能创建视图（零行占位），提高可用性与可调试性。
* 数据湖而非数据库直写：Parquet 多文件带来更好的可扩展性与成本优势，DuckDB 负责高效本地分析；必要时再做快照固化/去重重写。

---

## 七、典型调用路径（时序文字版）

1. 用户执行脚本（例如 ingest\_full\_all\_a.sh）
2. jobs/ingest\_ohlcva.py 解析 CLI → universe 解算 symbols
3. planner 生成 FetchPlan（源=akshare，批量=64）
4. connectors/akshare/ohlcva.py 并发调用上游 → DataFrame
5. validation.finalize\_ohlcva() 规范化与检查
6. writer.write\_ohlcva\_parquet() 分区写入 + log\_manifest()
7. database.ensure\_views("ohlcva","1d") 刷新视图
8. 上层通过 ohlcva\_1d\_v 查询或出报表

---

## 八、目录与模块边界（要点速览）

* datasource.\* 只产出/落盘“规范化 DataFrame”，不感知 DuckDB 表结构；
* database.\* 只感知 Parquet 目录与视图，不感知具体数据源；
* 脚本 scripts/\* 只负责编排参数与批处理，不承载业务逻辑。
