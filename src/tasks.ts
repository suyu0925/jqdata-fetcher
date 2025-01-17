import { queryTodayFuturesInfo, upsertFuturesDailyBar, upsertFuturesInfo } from './db'
import JqData, { JqFreqencyUnit } from './jqdata'
import cliProgress from 'cli-progress'

if (!process.env.JQDATA_USERNAME || !process.env.JQDATA_PASSWORD) {
  throw new Error('JQDATA_USERNAME and JQDATA_PASSWORD are required')
}

const jq = new JqData(process.env.JQDATA_USERNAME, process.env.JQDATA_PASSWORD)

export async function fetchTodayFutureList() {
  const futureList = await jq.getFutureList(new Date())
  await upsertFuturesInfo(futureList)
}

export async function fetchTodayFuturesBar() {
  const futureContracts = (await queryTodayFuturesInfo()).map(info => info.code)

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
  bar.start(futureContracts.length, 0)

  await Promise.all(futureContracts.map(async code => {
    const bars = await jq.getBars({
      code,
      unit: '1d' as JqFreqencyUnit,
      start_date: new Date(),
      end_date: new Date(),
    })
    const hasError = bars.some(bar => isNaN(bar.close))
    if (hasError) {
      console.error(`${code} has error`, bars)
    } else {
      await upsertFuturesDailyBar(bars)
    }

    bar.increment()
  }))

  bar.stop()
}
