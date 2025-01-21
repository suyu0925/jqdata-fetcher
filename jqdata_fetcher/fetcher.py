import datetime
import os

import jqdatasdk as jq
import pandas as pd
from dotenv import load_dotenv

from jqdata_fetcher.db.query import query_all_futures_info, query_today_futures_info

load_dotenv()

jq.auth(os.getenv('JQDATA_USERNAME'), os.getenv('JQDATA_PASSWORD'))


def fetch_all_futures_info():
    all_futures = jq.get_all_securities(types=['futures'])
    all_futures.index.name = 'code'
    all_futures = all_futures.reset_index()
    return all_futures


def fetch_today_futures_info():
    future_list = jq.get_all_securities(types=['futures'], date=datetime.date.today())
    future_list.index.name = 'code'
    future_list = future_list.reset_index()
    return future_list


def fetch_today_futures_daily_bar():
    future_list = query_today_futures_info()
    codes = future_list['code'].to_list()
    daily_bar = jq.get_bars(codes, fields=[
        'open', 'high', 'low', 'close', 'volume', 'money', 'open_interest',
        'paused', 'high_limit', 'low_limit', 'avg', 'pre_close',
    ], count=1, unit='1d')
    df = daily_bar.droplevel(1)
    df.index.name = 'code'
    df = df.reset_index()
    df['paused'] = df['paused'].astype(bool)
    return df


def fetch_full_single_future_daily_bar(code: str):
    daily_bar = jq.get_price(
        code,
        fields=[
            'open', 'high', 'low', 'close', 'volume', 'money', 'open_interest',
            'paused', 'high_limit', 'low_limit', 'avg', 'pre_close',
        ],
        frequency='1d',
        fq='none',
    )
    df = daily_bar
    df.index.name = 'date'
    df = df.reset_index()
    df['code'] = code
    df['paused'] = df['paused'].astype(bool)
    return df


def fetch_all_futures_daily_bar():
    all_futures = query_all_futures_info()
    df = pd.DataFrame()
    for _, row in all_futures.iterrows():
        code = row['code']
        daily_bar = fetch_full_single_future_daily_bar(code)
        df = pd.concat([df, daily_bar])
    df = df.reset_index(drop=True)
    return df
