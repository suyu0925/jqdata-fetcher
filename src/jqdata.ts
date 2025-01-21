import { format as formatDate } from 'date-fns'
import type { Tagged } from 'type-fest'
import { RateLimiter } from './rateLimiter'

export type JqCode = Tagged<string, 'JqCode'> // 聚宽证券代码

export type JqDate = Tagged<string, 'JqDate'> // 格式为'yyyy-MM-dd'
export type JqDatetime = Tagged<string, 'JqDatetime'> // 格式为'yyyy-MM-dd HH:mm:ss'

export type JqFutureKind = Tagged<string, 'JqFutureKind'> // 期货品种，比如'AG'。

export type JqFutureContract = Tagged<JqCode, 'JqFutureContract'> // 期货合约，比如'AU1701.XSGE'。

export type JqFreqencyUnit = Tagged<string, 'JqFreqencyUnit'> // bar的时间单位，可为'1m' - '120m', '1d', '1w', '1M'

export type SecurityType =
  | 'stock' // 股票
  | 'futures' // 期货
  | 'index' // 指数
  | 'etf' // ETF基金
  | 'fja' // 分级A
  | 'fjb' // 分级B
  | 'fjm' // 分级母基金
  | 'mmf' // 场内货币基金
  | 'open_fund' // 场外开放式基金
  | 'bond_fund' // 债券基金
  | 'stock_fund' // 股票基金
  | 'QDII_fund' // QDII基金
  | 'money_market_fund' // 场外货币基金
  | 'mixture_fund' // 混合型基金
  | 'option' // 期权

export type SecurityInfo = {
  code: JqCode
  display_name: string
  name: string
  start_date: JqDate // 上市日期。
  end_date: JqDate // 下市日期。如果未下市，则为2200-01-01
  type: SecurityType
}

export type Bar = {
  code: JqCode
  date: JqDatetime
  open: number
  high: number
  low: number
  close: number
  volume: number
  money: number
  open_interest: number
}

export type DailyBar = Omit<Bar, 'date'> & {
  date: JqDate
  paused: boolean
  high_limit: number
  low_limit: number
  avg: number
  pre_close: number
}

const parseError = (text: string) => {
  const errorMatch = text.match(/error.*(?:\n|$)/)
  if (errorMatch) {
    throw new Error(errorMatch[0])
  }

  const tooManyRequestMatch = text.match(/[Tt]oo [Mm]any [Rr]equests/)
  if (tooManyRequestMatch) {
    throw new Error('too many requests')
  }
}

const parseCSV = <T extends any>(csv: string) => {
  const lines = csv.trim().split('\n')
  const headers = lines[0].split(',')

  return lines.slice(1).map(line => {
    const values = line.split(',')
    return headers.reduce((obj, header, index) => {
      obj[header as keyof T] = values[index]
      return obj
    }, {} as {
      [K in keyof T]: string // csv中所有字段都是string
    })
  })
}

class JqData {
  private token: string | null = null
  private url = 'https://dataapi.joinquant.com/v2/apis'
  private rateLimiter: RateLimiter
  private isFetchingToken = false

  constructor(private username: string, private password: string) {
    this.rateLimiter = new RateLimiter(1, 10)
  }

  private async fetchToken() {
    const response = await fetch(this.url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        method: 'get_current_token',
        mob: this.username,
        pwd: this.password,
      })
    })
    const text = await response.text()
    // 如果返回错误，status code仍然为200，需要分析text
    parseError(text)
    this.token = text
  }

  private async _post(params: Record<string, unknown>): Promise<string> {
    if (!this.token) {
      await this.fetchToken()
    }

    const response = await this.rateLimiter.execute(() => fetch(this.url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        token: this.token,
        ...params,
      }),
    }))
    const text = await response.text()

    try {
      parseError(text)
    } catch (err) {
      if (err instanceof Error) {
        if (err.message.includes('token')) {
          // 如果错误原因是token过期，将token置为空重新获取一次
          console.warn('token过期, 重新获取')
          this.token = null
          return this._post(params)
        }
      }
      throw err
    }

    return text
  }

  async getQueryCount() {
    const numberText = await this._post({
      method: 'get_query_count',
    })
    return parseInt(numberText)
  }

  /**
   * 获取指定范围交易日
   * 
   * @see https://www.joinquant.com/help/api/help#JQDataHttpV2:get_trade_days-获取指定范围交易日
   */
  async getTradeDays(startDate: Date, endDate: Date) {
    const multilineText = await this._post({
      method: 'get_trade_days',
      start_date: formatDate(startDate, 'yyyy-MM-dd'),
      end_date: formatDate(endDate, 'yyyy-MM-dd'),
    })

    return (multilineText ? multilineText.split('\n') : []) as JqDate[]
  }

  /**
   * 获取所有股票信息
   * 
   * @see https://www.joinquant.com/help/api/help#JQDataHttpV2:get_all_securities-获取所有标的信息
   */
  async getStockList(date?: Date) {
    const csvText = await this._post({
      method: 'get_all_securities',
      code: 'stock',
      date: date ? formatDate(date, 'yyyy-MM-dd') : undefined,
    })

    return parseCSV<SecurityInfo>(csvText) as (SecurityInfo & { type: 'futures' })[]
  }

  /**
   * 获取所有期货信息
   * 
   * @see https://www.joinquant.com/help/api/help#JQDataHttpV2:get_all_securities-获取所有标的信息
   */
  async getFutureList(date: Date) {
    const csvText = await this._post({
      method: 'get_all_securities',
      code: 'futures',
      date: formatDate(date, 'yyyy-MM-dd'),
    })

    return parseCSV<SecurityInfo>(csvText) as (SecurityInfo & { type: 'futures' })[]
  }

  async getAllFutureList() {
    const csvText = await this._post({
      method: 'get_all_securities',
      code: 'futures',
    })

    return parseCSV<SecurityInfo>(csvText) as (SecurityInfo & { type: 'futures' })[]
  }

  /**
   * 获取期货可交易合约列表
   * 
   * @see https://www.joinquant.com/help/api/help#JQDataHttpV2:get_future_contracts-获取期货可交易合约列表
   */
  async getFutureContracts(kind: JqFutureKind, date: Date) {
    const multilineText = await this._post({
      method: 'get_future_contracts',
      code: kind,
      date: formatDate(date, 'yyyy-MM-dd'),
    })

    return multilineText.split('\n') as JqFutureContract[]
  }

  /**
   * 获取主力合约对应的标的
   * 
   * @see https://www.joinquant.com/help/api/help#JQDataHttpV2:get_dominant_future-获取主力合约对应的标的
   */
  async getDominantFuture(kind: JqFutureKind, date: Date) {
    const text = await this._post({
      method: 'get_dominant_future',
      code: kind,
      date: formatDate(date, 'yyyy-MM-dd'),
    })

    return text as JqFutureContract
  }

  async getDailyBars(params: {
    code: JqCode
    count?: number
    start_date?: Date
    end_date?: Date
    include_now?: boolean // 是否包含当前bar ,默认为True
    skip_paused?: boolean // 是否跳过停牌，默认为True
    fq_ref_date?: JqDate // 复权基准日期，该参数为空时返回不复权数据
  }): Promise<DailyBar[]> {
    const csvText = await this._post({
      method: 'get_price',
      unit: '1d',
      ...params,
      ...(params.start_date && { start_date: formatDate(params.start_date, 'yyyy-MM-dd HH:mm:ss') }),
      ...(params.end_date && { end_date: formatDate(params.end_date, 'yyyy-MM-dd HH:mm:ss') }),
    })

    return parseCSV<DailyBar>(csvText).map(bar => ({
      code: params.code as JqCode,
      date: bar.date as JqDate,
      open: parseFloat(bar.open),
      high: parseFloat(bar.high),
      low: parseFloat(bar.low),
      close: parseFloat(bar.close),
      volume: parseFloat(bar.volume),
      money: parseFloat(bar.money),
      paused: !!parseInt(bar.paused),
      high_limit: parseFloat(bar.high_limit),
      low_limit: parseFloat(bar.low_limit),
      avg: parseFloat(bar.avg),
      pre_close: parseFloat(bar.pre_close),
      open_interest: parseFloat(bar.open_interest),
    } satisfies DailyBar))
  }

  /**
   * 获取行情数据
   * 
   * @see https://www.joinquant.com/help/api/help#JQDataHttpV2:get_bars-获取行情数据
   */
  async getBars(params: {
    code: JqCode
    unit: JqFreqencyUnit
    count?: number
    start_date?: Date
    end_date?: Date
    include_now?: boolean // 是否包含当前bar ,默认为True
    skip_paused?: boolean // 是否跳过停牌，默认为True
    fq_ref_date?: JqDate // 复权基准日期，该参数为空时返回不复权数据
  }): Promise<Bar[]> {
    if (params.unit === '1d') {
      throw new Error('please use getDailyBars instead')
    }

    const csvText = await this._post({
      method: 'get_price',
      ...params,
      ...(params.start_date && { start_date: formatDate(params.start_date, 'yyyy-MM-dd HH:mm:ss') }),
      ...(params.end_date && { end_date: formatDate(params.end_date, 'yyyy-MM-dd HH:mm:ss') }),
    })

    return parseCSV<Omit<Bar, 'code'>>(csvText).map(bar => ({
      code: params.code as JqCode,
      date: bar.date as JqDatetime,
      open: parseFloat(bar.open),
      high: parseFloat(bar.high),
      low: parseFloat(bar.low),
      close: parseFloat(bar.close),
      volume: parseFloat(bar.volume),
      money: parseFloat(bar.money),
      open_interest: parseFloat(bar.open_interest),
    } satisfies Bar))
  }
}

export default JqData
