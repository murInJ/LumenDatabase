from typing import Dict, Type
from datasource.base import DataSource
from datasource.connectors.akshare.ohlcva import AKShareOHLCVA
# 将来可加 yahoo、tushare...
_REG: Dict[str, Type[DataSource]] = {
    "akshare": AKShareOHLCVA,
}

def get_source(name: str) -> DataSource:
    cls = _REG[name]
    return cls()

def list_sources() -> Dict[str, Type[DataSource]]:
    return dict(_REG)
