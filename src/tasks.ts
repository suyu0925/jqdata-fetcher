import cliProgress from 'cli-progress'
import { addDays, setHours, startOfDay } from 'date-fns'
import { queryTodayFuturesInfo, upsertFuturesDailyBar, upsertFuturesInfo, upsertFuturesMinutelyBar } from './db'
import JqData, { type JqFreqencyUnit } from './jqdata'

if (!process.env.JQDATA_USERNAME || !process.env.JQDATA_PASSWORD) {
  throw new Error('JQDATA_USERNAME and JQDATA_PASSWORD are required')
}

const jq = new JqData(process.env.JQDATA_USERNAME, process.env.JQDATA_PASSWORD)

export async function isTodayTradeDay() {
  const tradeDays = await jq.getTradeDays(new Date(), new Date())
  return tradeDays.length > 0
}

export async function fetchTodayFutureList() {
  const futureList = await jq.getFutureList(new Date())
  await upsertFuturesInfo(futureList)
}

export async function fetchTodayDailyFuturesBar() {
  const futureContracts = (await queryTodayFuturesInfo()).map(info => info.code)

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
  bar.start(futureContracts.length, 0)

  await Promise.all(futureContracts.map(async code => {
    const bars = await jq.getDailyBars({
      code,
      count: 1,
    })
    await upsertFuturesDailyBar(bars)

    bar.increment()
  }))

  bar.stop()
}

export async function fetchTodayMinutelyFuturesBar() {
  const futureContracts = (await queryTodayFuturesInfo()).map(info => info.code)

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
  bar.start(futureContracts.length, 0)

  await Promise.all(futureContracts.map(async code => {
    // 取昨天16点到今天16点的分钟数据
    const bars = await jq.getBars({
      code,
      unit: '1m' as JqFreqencyUnit,
      start_date: setHours(addDays(startOfDay(new Date()), -1), 16),
      end_date: setHours(startOfDay(new Date()), 16),
    })
    await upsertFuturesMinutelyBar(bars)

    bar.increment()
  }))

  bar.stop()
}
