import { format } from 'date-fns'
import pg from 'pg'
import { Bar, JqFutureContract, SecurityInfo } from './jqdata'

if (!process.env.PGURL) {
  throw new Error('PGURL environment variable is required')
}

export const pool = new pg.Pool({
  connectionString: process.env.PGURL,
})

export const queryTodayFuturesInfo = async () => {
  const today = new Date()
  const res = await pool.query<Omit<SecurityInfo, 'type' | 'code'> & { code: JqFutureContract }>(`
    SELECT code, display_name, name, start_date, end_date 
    FROM futures_info 
    WHERE 
      start_date <= '${format(today, 'yyyy-MM-dd')}' 
      AND end_date >= '${format(today, 'yyyy-MM-dd')}'
  `)
  return res.rows
}

export const upsertFuturesInfo = async (futuresInfo: (SecurityInfo & { type: 'futures' })[]) => {
  await pool.query(`
    INSERT INTO futures_info (code, display_name, name, start_date, end_date)
    VALUES ${futuresInfo.map(info => `('${info.code}', '${info.display_name}', '${info.name}', '${info.start_date}', '${info.end_date}')`).join(', ')}
    ON CONFLICT (code) DO UPDATE SET
      display_name = EXCLUDED.display_name,
      name = EXCLUDED.name,
      start_date = EXCLUDED.start_date,
      end_date = EXCLUDED.end_date
  `)
}

export const upsertFuturesDailyBar = async (bars: Bar[]) => {
  await pool.query(`
    INSERT INTO futures_daily_bar 
      (
        code, date, open, high, low, close, volume, money, 
        paused, high_limit, low_limit, avg, pre_close, open_interest
      )
    VALUES 
      ${bars.map(bar => `
        (
          '${bar.code}', '${bar.date}', ${bar.open}, ${bar.high}, ${bar.low}, ${bar.close}, ${bar.volume}, ${bar.money},
          ${bar.paused}, ${bar.high_limit}, ${bar.low_limit}, ${bar.avg}, ${bar.pre_close}, ${bar.open_interest}
        )
      `).join(', ')}
    ON CONFLICT (code, date) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      money = EXCLUDED.money,
      paused = EXCLUDED.paused,
      high_limit = EXCLUDED.high_limit,
      low_limit = EXCLUDED.low_limit,
      avg = EXCLUDED.avg,
      pre_close = EXCLUDED.pre_close,
      open_interest = EXCLUDED.open_interest
  `)
}

export const initDb = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS futures_info (
      code text PRIMARY KEY,
      display_name text NOT NULL,
      name text NOT NULL,
      start_date date NOT NULL,
      end_date date NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_start_date ON futures_info (start_date);
    CREATE INDEX IF NOT EXISTS idx_end_date ON futures_info (end_date);
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS futures_daily_bar (
      code text NOT NULL,
      date date NOT NULL,
      open numeric NOT NULL,
      high numeric NOT NULL,
      low numeric NOT NULL,
      close numeric NOT NULL,
      volume numeric NOT NULL,
      money numeric NOT NULL,
      paused boolean NOT NULL,
      high_limit numeric NOT NULL,
      low_limit numeric NOT NULL,
      avg numeric NOT NULL,
      pre_close numeric NOT NULL,
      open_interest numeric NOT NULL,
      PRIMARY KEY (code, date)
    );
    CREATE INDEX IF NOT EXISTS idx_code ON futures_daily_bar (code);
    CREATE INDEX IF NOT EXISTS idx_date ON futures_daily_bar (date);
  `)
}

pool.on('connect', (client) => {
  const schema = new URL(process.env.PGURL!).searchParams.get('schema') || 'public'
  client.query(`SET search_path TO ${schema}`)
})

await initDb()
