from datasource.source.akshare.stock.stock_info import GetStockList as _getStockList_akshare
def GetStockList(source="akshare"):
    fetcher = {
        "akshare" : _getStockList_akshare
    }

    return fetcher[source]()

if __name__ == '__main__':
    print(GetStockList())