import datetime

import pandas as pd

from jqdata_fetcher.db.db import Session, select, and_
from jqdata_fetcher.db.tables import FuturesInfo


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
