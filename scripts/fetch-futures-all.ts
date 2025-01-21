import { pool } from '../src/db'
import { fetchAllFutureList, fetchHistoricalFuturesDailyBar } from '../src/tasks'

await fetchAllFutureList()

await fetchHistoricalFuturesDailyBar(5)

await pool.end()
