import { pool } from '../src/db'
import { fetchTodayDailyFuturesBar, fetchTodayFutureList, fetchTodayMinutelyFuturesBar, isTodayTradeDay } from '../src/tasks'

if (await isTodayTradeDay()) {
  await fetchTodayFutureList()

  await fetchTodayDailyFuturesBar()

  await fetchTodayMinutelyFuturesBar()
}

await pool.end()
