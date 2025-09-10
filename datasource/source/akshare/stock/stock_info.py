import akshare as ak
import pandas as pd


def _getStockList_sh_a():
    # 序号 代码 名称 最新价 涨跌幅 涨跌额 成交量 成交额 振幅 最高 最低 今开 昨收 量比 换手率 市盈率-动态 市净率 总市值 流通市值 涨速 5分钟涨跌 60日涨跌幅 年初至今涨跌幅
    stock_sh_a_spot_em_df = ak.stock_sh_a_spot_em()
    #代码 名称
    df = stock_sh_a_spot_em_df[['代码','名称']].rename(columns={'代码':'symbol','名称':'name'})
    df['symbol'] = df['symbol'] + '.sh'
    return df

def _getStockList_sz_a():
    # 序号 代码 名称 最新价 涨跌幅 涨跌额 成交量 成交额 振幅 最高 最低 今开 昨收 量比 换手率 市盈率-动态 市净率 总市值 流通市值 涨速 5分钟涨跌 60日涨跌幅 年初至今涨跌幅
    stock_sz_a_spot_em_df = ak.stock_sz_a_spot_em()
    #代码 名称
    df = stock_sz_a_spot_em_df[['代码','名称']].rename(columns={'代码':'symbol','名称':'name'})
    df['symbol'] = df['symbol'] + '.sz'
    return df

def _getStockList_bj_a():
    # 序号 代码 名称 最新价 涨跌幅 涨跌额 成交量 成交额 振幅 最高 最低 今开 昨收 量比 换手率 市盈率-动态 市净率 总市值 流通市值 涨速 5分钟涨跌 60日涨跌幅 年初至今涨跌幅
    stock_bj_a_spot_em_df = ak.stock_bj_a_spot_em()
    #代码 名称
    df = stock_bj_a_spot_em_df[['代码','名称']].rename(columns={'代码':'symbol','名称':'name'})
    df['symbol'] = df['symbol'] + '.bj'
    return df

def _getStockList_cy_a():
    # 序号 代码 名称 最新价 涨跌幅 涨跌额 成交量 成交额 振幅 最高 最低 今开 昨收 量比 换手率 市盈率-动态 市净率 总市值 流通市值 涨速 5分钟涨跌 60日涨跌幅 年初至今涨跌幅
    stock_cy_a_spot_em_df = ak.stock_cy_a_spot_em()
    #代码 名称
    df = stock_cy_a_spot_em_df[['代码','名称']].rename(columns={'代码':'symbol','名称':'name'})
    df['symbol'] = df['symbol'] + '.cy'
    return df

def _getStockList_kc_a():
    # 序号 代码 名称 最新价 涨跌幅 涨跌额 成交量 成交额 振幅 最高 最低 今开 昨收 量比 换手率 市盈率-动态 市净率 总市值 流通市值 涨速 5分钟涨跌 60日涨跌幅 年初至今涨跌幅
    stock_kc_a_spot_em_df = ak.stock_kc_a_spot_em()
    #代码 名称
    df = stock_kc_a_spot_em_df[['代码','名称']].rename(columns={'代码':'symbol','名称':'name'})
    df['symbol'] = df['symbol'] + '.kc'
    return df

def _getStockList_us():
    # 序号 名称 最新价 涨跌额 涨跌幅 开盘价 最高价 最低价 昨收价 总市值 市盈率 成交量 成交额 振幅 换手率 代码
    stock_us_spot_em_df = ak.stock_us_spot_em()
    #代码 名称
    df = stock_us_spot_em_df[['代码','名称']].rename(columns={'代码':'symbol','名称':'name'})
    df['symbol'] = df['symbol'] + '.us'
    return df

def _getStockList_hk():
    # 序号 代码 名称 最新价 涨跌额 涨跌幅 今开 最高 最低 昨收 成交量 成交额
    stock_us_spot_em_df = ak.stock_us_spot_em()
    #代码 名称
    df = stock_us_spot_em_df[['代码','名称']].rename(columns={'代码':'symbol','名称':'name'})
    df['symbol'] = df['symbol'] + '.hk'
    return df

def GetStockList(
    markets=('sh','sz','bj','cy','kc','us','hk'),
    dedup_on='symbol',      # 去重依据：'symbol' 或 None
    keep='first',           # 去重保留：'first' / 'last'
    sort=True,              # 是否按 symbol 排序
    errors='ignore',        # 'ignore'：某市场失败继续；'raise'：抛错
    unify_a_to_zh=False     # True：新增列 symbol_zh，把 A 股后缀统一成 .zh
):
    fetcher = {
        'sh': _getStockList_sh_a,
        'sz': _getStockList_sz_a,
        'bj': _getStockList_bj_a,
        'cy': _getStockList_cy_a,
        'kc': _getStockList_kc_a,
        'us': _getStockList_us,
        'hk': _getStockList_hk,
    }

    frames = []
    for m in markets:
        fn = fetcher.get(m)
        if fn is None:
            if errors == 'raise':
                raise ValueError(f'Unknown market: {m}')
            continue
        try:
            tmp = fn()
            # 兜底：确保有 symbol / name
            if not {'symbol','name'}.issubset(tmp.columns):
                rename_map = {}
                if '代码' in tmp.columns: rename_map['代码'] = 'symbol'
                if '名称' in tmp.columns: rename_map['名称'] = 'name'
                tmp = tmp.rename(columns=rename_map)[['symbol','name']]
            tmp = tmp.copy()
            tmp['symbol'] = tmp['symbol'].astype(str).str.strip()
            tmp['name']   = tmp['name'].astype(str).str.strip()
            frames.append(tmp[['symbol','name']])
        except Exception as e:
            if errors == 'raise':
                raise
            else:
                print(f'[GetStockList] Skip {m} due to error: {e}')

    if not frames:
        cols = ['symbol','name']
        if unify_a_to_zh: cols.append('symbol_zh')
        return pd.DataFrame(columns=cols)

    df_all = pd.concat(frames, ignore_index=True)

    if unify_a_to_zh:
        a_mask = df_all['symbol'].str.endswith(('.sh','.sz','.bj','.kc','.cy'), na=False)
        df_all['symbol_zh'] = df_all['symbol']
        df_all.loc[a_mask, 'symbol_zh'] = df_all.loc[a_mask, 'symbol_zh'] \
            .str.replace(r'\.(sh|sz|bj|kc|cy)$', '.zh', regex=True)

    if dedup_on:
        df_all = df_all.drop_duplicates(subset=[dedup_on], keep=keep)

    if sort:
        df_all = df_all.sort_values('symbol').reset_index(drop=True)

    return df_all

if __name__ == '__main__':
    print(GetStockList())

