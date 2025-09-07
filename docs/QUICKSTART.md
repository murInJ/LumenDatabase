# 快速启动：一步步使用 `scripts/*.sh`（可直接复制执行）

## 1) 初始化（只需一次）

```sh
# 进入你的项目根目录（$PWD 即项目根）
# 安装依赖（如未安装）
pip install duckdb pandas pyarrow akshare tqdm

# 脚本赋权 & 环境准备
chmod +x scripts/*.sh
export PYTHONPATH="$PWD"
source scripts/env.sh
```

## 2) 全市场「全量」更新（长窗口一次跑）

```sh
# 从 2010-01-01 跑到今天（UTC）
scripts/ingest_full_all_a.sh 2010-01-01

# 或显式指定结束日期
# scripts/ingest_full_all_a.sh 2010-01-01 $(date -u +%F)
```

## 3) 全市场「按年全量」批跑（更稳，易重试）

```sh
# 2010 年到 2025 年逐年全量
scripts/ingest_full_yearly.sh 2010 2025
```

## 4) 全市场「增量 / 自动」更新（含回看天数）

```sh
# 自动：已有历史走增量，无历史走全量；回看 1 天覆盖修订
MODE=auto LOOKBACK_DAYS=1 scripts/ingest_incremental_universe.sh 2024-01-01 2025-09-06

# 仅增量：强制增量 + 回看 3 天
MODE=incremental LOOKBACK_DAYS=3 scripts/ingest_incremental_universe.sh 2025-08-01 2025-09-06
```

## 5) 指数成份股示例（沪深 300）

```sh
# 全量
scripts/ingest_index.sh 000300 2015-01-01 2025-09-06 full

# 增量（回看 1 天）
MODE=incremental LOOKBACK_DAYS=1 scripts/ingest_index.sh 000300 2025-01-01 2025-09-06 incremental
```

## 6) 快速验数 / 范围检查

```sh
scripts/verify_ohlcva.sh
```

## 7) 清空后重建（可选）

```sh
# 清理 OHLCVA 分区与 manifest
scripts/clean_ohlcva.sh

# 重新全量构建
scripts/ingest_full_all_a.sh 2010-01-01
```

## 8) 导出“去重后的黄金快照”（可选）

```sh
# 导出去重后的稳定分区到新目录 data_snapshot_2025Q3
scripts/snapshot_dedupe_ohlcva.sh data_snapshot_2025Q3
```

## 9) 临时调优（并发 & 速率，可与任一脚本连用）

```sh
# 提升并发与速率（按机器/网络调整）
LUMEN_CONCURRENCY=16 AKSHARE_RATE=12 scripts/ingest_full_all_a.sh 2010-01-01 2025-09-06
```
