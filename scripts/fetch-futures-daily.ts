import { pool } from '../src/db'
import { fetchTodayFutureList, fetchTodayFuturesBar } from '../src/tasks'

await fetchTodayFutureList()

await fetchTodayFuturesBar()

await pool.end()
