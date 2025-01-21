from tqdm import tqdm

from jqdata_fetcher.db.query import query_all_futures_info
from jqdata_fetcher.fetcher import (fetch_all_futures_info,
                                    fetch_full_single_future_daily_bar)
from jqdata_fetcher.tasks.daily import save_daily_bar, save_futures_info


def run_full_task():
    futures_info = fetch_all_futures_info()
    save_futures_info(futures_info)

    all_futures = query_all_futures_info()
    with tqdm(total=len(all_futures)) as pbar:
        for _, row in all_futures.iterrows():
            code = row['code']
            daily_bar = fetch_full_single_future_daily_bar(code)
            save_daily_bar(daily_bar)
            pbar.update(1)
