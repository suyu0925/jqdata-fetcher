import { pool, queryTodayFuturesInfo, upsertFuturesMinutelyBar } from '../src/db'
import JqData, { type JqCode, type JqFreqencyUnit } from '../src/jqdata'
import { fetchTodayFutureList, fetchTodayDailyFuturesBar } from '../src/tasks'
import { addDays, endOfDay, startOfDay, setHours, setMinutes } from 'date-fns'
import cliProgress from 'cli-progress'


if (!process.env.JQDATA_USERNAME || !process.env.JQDATA_PASSWORD) {
  throw new Error('JQDATA_USERNAME and JQDATA_PASSWORD are required')
}

const jq = new JqData(process.env.JQDATA_USERNAME, process.env.JQDATA_PASSWORD)

await jq.getTradeDays(new Date(), new Date())

const futureContracts = (await queryTodayFuturesInfo()).map(info => info.code)

const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
bar.start(futureContracts.length, 0)

await Promise.all(futureContracts.map(async code => {
  // 取昨天16点到今天16点的分钟数据
  const bars = await jq.getBars({
    code,
    unit: '1m' as JqFreqencyUnit,
    start_date: setHours(addDays(startOfDay('2025-01-17'), -1), 16),
    end_date: setHours(startOfDay('2025-01-17'), 16),
  })
  await upsertFuturesMinutelyBar(bars)

  bar.increment()
}))

bar.stop()

await pool.end()
