import re

import pandas as pd
from tqdm import tqdm

from jqdata_fetcher.db.query import (query_all_futures_kind,
                                     query_latest_open_interest_daily,
                                     query_open_interest_daily_bar_by_kind)
from jqdata_fetcher.db.saver import (save_consecutive_contract,
                                     save_dominant_contract)


def get_dominant_contract(df: pd.DataFrame):
    """
    返回值：
      返回一个DataFrame, 包含日期和主力、次主力合约代码。
      注意这里的主力算法只是简单的昨持仓最大的合约，并不会考虑切换主力后的再次切回。
    - index: date
    - columns: dominant, subdominant
    - values: 日期对应的合约代码
    """
    df = df.set_index(['date', 'code']).sort_index()

    # 获取主力合约
    df.loc[:, 'dominant'] = False
    max_oi_idx = df.groupby('date')['open_interest'].idxmax().dropna()
    df.loc[max_oi_idx, 'dominant'] = True
    df.loc[max_oi_idx, 'open_interest'] = 0
    df['dominant'] = df.groupby(level=1)['dominant'].shift(1)

    # 获取次主力合约
    df.loc[:, 'subdominant'] = False
    submax_oi_idx = df.groupby('date')['open_interest'].idxmax().dropna()
    df.loc[submax_oi_idx, 'subdominant'] = True
    df['subdominant'] = df.groupby(level=1)['subdominant'].shift(1)

    # 构建新的数据
    dominant_codes = df[df['dominant'] == True].index.get_level_values('code')
    subdominant_codes = df[df['subdominant'] == True].index.get_level_values('code')
    dates = df.index.get_level_values('date').unique()

    new_df = pd.DataFrame(index=dates)
    new_df.loc[df[df['dominant'] == True].index.get_level_values('date'), 'dominant'] = dominant_codes
    new_df.loc[df[df['subdominant'] == True].index.get_level_values('date'), 'subdominant'] = subdominant_codes

    return new_df


def get_consecutive_contract(df: pd.DataFrame):
    """
    返回值：
      返回一个DataFrame, 包含日期和特定月份连续合约，以及连一连二一直到连十九。
    - index: date
    - columns: 00, 01, 02, 03, ..., 19, 01M, 02M, 03M, ..., 12M
    - values: 日期对应的合约代码      
    """
    df = df.set_index('date').sort_index()
    sorted_codes = df.groupby('date')['code'].apply(lambda x: sorted(x)).to_dict()

    new_df = pd.DataFrame(index=df.index.unique())

    # 当月，连一……连十九
    for n in range(20):
        col = f'{n:02d}'
        new_df[col] = [sorted_codes[date][n] if len(sorted_codes[date]) > n else None
                       for date in new_df.index]

    # 01M, 02M, 03M, ..., 12M
    for n in range(12):
        col = f'{(n+1):02d}M'
        new_df[col] = [next((code for code in sorted_codes[date]
                             if re.match(r'^([A-Z])+\d{2}(' + f'{n+1:02d}' + r')', code)), None)
                       for date in new_df.index]

    return new_df


def save_all_continuous_contract():
    kinds = query_all_futures_kind()
    for kind in tqdm(kinds):
        oi_df = query_open_interest_daily_bar_by_kind(kind)

        df = get_dominant_contract(oi_df)
        save_dominant_contract(df)

        df = get_consecutive_contract(oi_df)
        save_consecutive_contract(df)


def save_daily_continuous_contract():
    kinds = query_all_futures_kind()
    all_oi_df = query_latest_open_interest_daily()
    for kind in tqdm(kinds):
        oi_df = all_oi_df[all_oi_df['code'].str.match(f'^{kind}\\d')]
        if oi_df.empty:
            continue

        df = get_dominant_contract(oi_df)
        save_dominant_contract(df)

        df = get_consecutive_contract(oi_df)
        save_consecutive_contract(df)
