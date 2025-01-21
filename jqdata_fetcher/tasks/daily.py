from jqdata_fetcher.db import FuturesInfo, Session, insert
from jqdata_fetcher.db.tables import FuturesDailyBar
from jqdata_fetcher.fetcher import (fetch_today_futures_daily_bar,
                                    fetch_today_futures_info)


def save_futures_info(futures_info):
    df = futures_info[['code', 'display_name', 'name', 'start_date', 'end_date']]

    stmt = insert(FuturesInfo).values(df.to_dict('records'))

    stmt = stmt.on_conflict_do_update(
        index_elements=['code'],
        set_={
            'display_name': stmt.excluded.display_name,
            'name': stmt.excluded.name,
            'start_date': stmt.excluded.start_date,
            'end_date': stmt.excluded.end_date,
        }
    )

    with Session() as session:
        session.execute(stmt)
        session.commit()


def save_daily_bar(daily_bar):
    df = daily_bar[[
        'code', 'date', 'open', 'high', 'low', 'close', 'volume', 'money',  'open_interest',
        'paused', 'high_limit', 'low_limit', 'avg', 'pre_close',
    ]]

    stmt = insert(FuturesDailyBar).values(df.to_dict('records'))
    stmt = stmt.on_conflict_do_update(
        index_elements=['code', 'date'],
        set_={
            'open': stmt.excluded.open,
            'high': stmt.excluded.high,
            'low': stmt.excluded.low,
            'close': stmt.excluded.close,
            'volume': stmt.excluded.volume,
            'money': stmt.excluded.money,
            'open_interest': stmt.excluded.open_interest,
            'paused': stmt.excluded.paused,
            'high_limit': stmt.excluded.high_limit,
            'low_limit': stmt.excluded.low_limit,
            'avg': stmt.excluded.avg,
            'pre_close': stmt.excluded.pre_close,
        }
    )

    with Session() as session:
        session.execute(stmt)
        session.commit()


def handle_futures_info():
    futures_info = fetch_today_futures_info()
    save_futures_info(futures_info)


def handle_futures_bar():
    daily_bar = fetch_today_futures_daily_bar()
    save_daily_bar(daily_bar)


def run_daily_task():
    handle_futures_info()
    handle_futures_bar()
