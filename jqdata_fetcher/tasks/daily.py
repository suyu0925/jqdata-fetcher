from jqdata_fetcher.continuous_contract import save_daily_continuous_contract
from jqdata_fetcher.db import save_daily_bar, save_futures_info
from jqdata_fetcher.fetcher import (fetch_today_futures_daily_bar,
                                    fetch_today_futures_info)


def run_daily_task():
    futures_info = fetch_today_futures_info()
    save_futures_info(futures_info)

    daily_bar = fetch_today_futures_daily_bar()
    save_daily_bar(daily_bar)

    save_daily_continuous_contract()


if __name__ == '__main__':
    run_daily_task()
