# LumenDatabase  (Latest Version 0.0.2)

## Fetch Data

### 全市场「全量」更新（长窗口一次跑）

```sh
# 从 2010-01-01 跑到今天（UTC）
scripts/fetch/ingest_full_all_a.sh 2010-01-01

# 或显式指定结束日期
# scripts/ingest_full_all_a.sh 2010-01-01 $(date -u +%F)
```

### 全市场「按年全量」批跑（更稳，易重试）

```sh
# 2010 年到 2025 年逐年全量
scripts/fetch/ingest_full_yearly.sh 2010 2025
```

### 全市场「增量 / 自动」更新（含回看天数）

```sh
# 自动：已有历史走增量，无历史走全量；回看 1 天覆盖修订
MODE=auto LOOKBACK_DAYS=1 scripts/fetch/ingest_incremental_universe.sh 2024-01-01 2025-09-06

# 仅增量：强制增量 + 回看 3 天
MODE=incremental LOOKBACK_DAYS=3 scripts/fetch/ingest_incremental_universe.sh 2025-08-01 2025-09-06
```

### 指数成份股示例（沪深 300）

```sh
# 全量
scripts/fetch/ingest_index.sh 000300 2015-01-01 2025-09-06 full

# 增量（回看 1 天）
MODE=incremental LOOKBACK_DAYS=1 scripts/fetch/ingest_index.sh 000300 2025-01-01 2025-09-06 incremental
```
### 临时调优（并发 & 速率，可与任一脚本连用）

```sh
# 提升并发与速率（按机器/网络调整）
LUMEN_CONCURRENCY=16 AKSHARE_RATE=12 scripts/fetch/ingest_full_all_a.sh 2010-01-01 2025-09-06
```

## Check Data

### 快速验数 / 范围检查

```sh
scripts/check/verify_ohlcva.sh
```

### 清空后重建（可选）

```sh
# 清理 OHLCVA 分区与 manifest
scripts/check/clean_ohlcva.sh

# 重新全量构建
scripts/fetch/ingest_full_all_a.sh 2010-01-01
```

### 导出“去重后的黄金快照”（可选）

```sh
# 导出去重后的稳定分区到新目录 data_snapshot_2025Q3
scripts/check/snapshot_dedupe_ohlcva.sh data_snapshot_2025Q3
```

## Query Data

### 查询“指定股票 + 日期范围”

```sh
# 查询平安银行(000001.SZ)在 2024H1 的日线，保存为 CSV
scripts/query/query_ohlcva_symbol.sh 000001.SZ 2024-01-01 2024-06-30 > out/000001_SZ_2024H1.csv

# 仅看最近一个季度的前 20 条
scripts/query/query_ohlcva_symbol.sh 600000.SH 2024-04-01 2024-06-30 20

# 使用默认日期范围（2010-01-01 ~ 今天），直接打印到屏幕
scripts/query/query_ohlcva_symbol.sh 000001.SZ

```

### 查询“全市场所有股票 + 日期范围”
```sh
# 导出 2024 年 1 月全市场日线
scripts/query/query_ohlcva_all.sh 2024-01-01 2024-01-31 > out/all_A_202401.csv

# 导出 2024H1 的采样 1000 行（快速抽样看格式）
scripts/query/query_ohlcva_all.sh 2024-01-01 2024-06-30 1000 > out/sample_2024H1.csv

# 使用默认日期范围（2010-01-01 ~ 今天）
scripts/query/query_ohlcva_all.sh

```