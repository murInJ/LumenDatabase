from __future__ import annotations
import os
from pathlib import Path
from functools import lru_cache
from typing import Any, Dict

import yaml


@lru_cache(maxsize=1)
def GetDatabaseConfig() -> Dict[str, Any]:
    """
    读取并返回 /database/config/config.yaml 的配置字典。

    解析顺序（优先级从高到低）：
    1) 环境变量 LUMEN_DB_CONFIG 或 LUMEN_CONFIG 指定的文件路径
    2) 与本文件 setting.py 同目录下的 config.yaml
    3) 向上逐级搜索，匹配 <任意父目录>/database/config/config.yaml

    无论本项目作为根项目，还是被其他项目以子模块方式调用，都能稳定定位。
    结果缓存（同一进程内）以避免重复 IO。
    """
    # 1) 环境变量覆盖
    env_path = os.getenv("LUMEN_DB_CONFIG") or os.getenv("LUMEN_CONFIG")
    candidates = []
    if env_path:
        candidates.append(Path(env_path).expanduser().resolve())

    # 2) 同目录：.../database/config/setting.py -> .../database/config/config.yaml
    here = Path(__file__).resolve()
    candidates.append(here.parent / "config.yaml")

    # 3) 向上搜索父目录中的 database/config/config.yaml（容错，防止包布局改变）
    for p in here.parents:
        candidates.append(p / "database" / "config" / "config.yaml")

    # 依次尝试候选路径
    for path in candidates:
        try:
            if path.is_file():
                with path.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                if not isinstance(data, dict):
                    raise ValueError(f"YAML root must be a mapping, got {type(data).__name__}")
                return data
        except Exception as e:
            # 命中但解析失败，立即抛出更清晰的错误
            raise RuntimeError(f"Failed to load database config from '{path}': {e}") from e

    # 所有候选都不存在时给出提示
    raise FileNotFoundError(
        "Could not find 'config.yaml'. Tried (in order):\n  - "
        + "\n  - ".join(str(p) for p in candidates)
        + "\nSet LUMEN_DB_CONFIG env var to specify a custom path."
    )


if __name__ == "__main__":
    print(GetDatabaseConfig())