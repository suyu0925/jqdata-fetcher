import re

from jqdata_fetcher.db.db import Session, insert
from jqdata_fetcher.db.tables import FuturesContinuousContract, FuturesDailyBar, FuturesInfo


def save_futures_info(futures_info):
    if futures_info.empty:
        return

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
    if daily_bar.empty:
        return

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


def save_dominant_contract(df):
    if df.empty:
        return

    demo_contract_code = df.loc[df['dominant'].notna(), 'dominant'].iloc[0]
    [kind, year_and_month, exchange] = re.match(r'^([A-Z]+)(\d{4})\.([A-Z]+)$', demo_contract_code).groups()

    dominant_values = (
        df[['dominant']]
        .rename(columns={'dominant': kind + '.' + exchange})
        .reset_index()
        .melt(id_vars=['date'], var_name='continuous_code', value_name='contract_code')
        .dropna()
        .to_dict('records')
    )

    subdominant_values = (
        df[['subdominant']]
        .rename(columns={'subdominant': kind + '_S.' + exchange})
        .reset_index()
        .melt(id_vars=['date'], var_name='continuous_code', value_name='contract_code')
        .dropna()
        .to_dict('records')
    )

    stmt = insert(FuturesContinuousContract).values(dominant_values + subdominant_values)
    stmt = stmt.on_conflict_do_update(
        index_elements=['date', 'continuous_code'],
        set_={
            'contract_code': stmt.excluded.contract_code,
        }
    )

    with Session() as session:
        session.execute(stmt)
        session.commit()


def save_consecutive_contract(df):
    if df.empty:
        return

    demo_contract_code = df.loc[df['00'].notna(), '00'].iloc[0]
    [kind, year_and_month, exchange] = re.match(r'^([A-Z]+)(\d{4})\.([A-Z]+)$', demo_contract_code).groups()

    values = []

    for col in df.columns:
        values = values + (
            df[[col]]
            .rename(columns={col: kind + col + '.' + exchange})
            .reset_index()
            .melt(id_vars=['date'], var_name='continuous_code', value_name='contract_code')
            .dropna()
            .to_dict('records')
        )

    stmt = insert(FuturesContinuousContract).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=['date', 'continuous_code'],
        set_={
            'contract_code': stmt.excluded.contract_code,
        }
    )

    with Session() as session:
        session.execute(stmt)
        session.commit()
