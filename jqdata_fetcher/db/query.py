import datetime

import pandas as pd

from jqdata_fetcher.db.db import Session, select, and_, text, desc
from jqdata_fetcher.db.tables import FuturesDailyBar, FuturesInfo


def query_all_futures_info():
    with Session() as session:
        return pd.read_sql_query(select(FuturesInfo), session.bind)


def query_today_futures_info():
    with Session() as session:
        return pd.read_sql_query(select(FuturesInfo).where(
            and_(
                FuturesInfo.start_date <= datetime.date.today(),
                FuturesInfo.end_date >= datetime.date.today(),
            ),
        ), session.bind)


def query_all_futures_kind() -> list[str]:
    with Session() as session:
        return [x[0] for x in
                session.execute(text(r"""
                    SELECT DISTINCT
                        (regexp_matches(code, '^([A-Z]+)\d'))[1] AS prefix
                    FROM
                        jqdata.futures_info
                    ORDER BY prefix ASC;
                """)).fetchall()
                ]


def query_all_daily_bar_by_kind(kind: str):
    with Session() as session:
        return pd.read_sql_query(select(FuturesDailyBar).where(
            FuturesDailyBar.code.regexp_match(f'^{kind}\\d'),
        ), session.bind)


def query_open_interest_daily_bar_by_kind(kind: str):
    with Session() as session:
        return pd.read_sql_query(select(FuturesDailyBar.date, FuturesDailyBar.code, FuturesDailyBar.open_interest).where(
            FuturesDailyBar.code.regexp_match(f'^{kind}(?!7777|8888|9999)\\d'),
        ), session.bind)


def query_latest_open_interest_daily(days=30):
    """
    获取最近n天的持仓量数据, 减少计算量
    """
    with Session() as session:
        return pd.read_sql_query(select(FuturesDailyBar.date, FuturesDailyBar.code, FuturesDailyBar.open_interest)
                                 .where(FuturesDailyBar.date >= datetime.date.today() - datetime.timedelta(days=days)), session.bind)
