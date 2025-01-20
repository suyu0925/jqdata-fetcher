import { pool } from '../src/db'
import { fetchTodayDailyFuturesBar, fetchTodayFutureList, isTodayTradeDay } from '../src/tasks'

if (await isTodayTradeDay()) {
  await fetchTodayFutureList()

  await fetchTodayDailyFuturesBar()
}

await pool.end()
