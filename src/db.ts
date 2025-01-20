import { format, parse } from 'date-fns'
import pg from 'pg'
import type { Bar, DailyBar, JqFutureContract, SecurityInfo } from './jqdata'

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
  if (futuresInfo.length === 0) {
    return
  }

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

export const upsertFuturesDailyBar = async (bars: DailyBar[]) => {
  if (bars.length === 0) {
    return
  }

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

export const upsertFuturesMinutelyBar = async (bars: Bar[]) => {
  if (bars.length === 0) {
    return
  }

  await pool.query(`
    INSERT INTO futures_minutely_bar 
      (
        code, time, open, high, low, close, volume, 
        money, open_interest
      )
    VALUES 
      ${bars.map((_, i) => `(
        $${i * 9 + 1}, $${i * 9 + 2}::timestamp with time zone, $${i * 9 + 3}, $${i * 9 + 4}, $${i * 9 + 5}, $${i * 9 + 6}, $${i * 9 + 7}, 
        $${i * 9 + 8}, $${i * 9 + 9})
      `).join(', ')}
    ON CONFLICT (code, time) DO UPDATE SET
      open = EXCLUDED.open,
      high = EXCLUDED.high,
      low = EXCLUDED.low,
      close = EXCLUDED.close,
      volume = EXCLUDED.volume,
      money = EXCLUDED.money,
      open_interest = EXCLUDED.open_interest
  `, bars.map(bar => [
    bar.code, parse(bar.date, 'yyyy-MM-dd HH:mm:ss', new Date()), bar.open, bar.high, bar.low, bar.close, bar.volume,
    bar.money, bar.open_interest,
  ]).flat())
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS futures_minutely_bar (
      code text NOT NULL,
      time timestamp with time zone NOT NULL,
      open numeric NOT NULL,
      high numeric NOT NULL,
      low numeric NOT NULL,
      close numeric NOT NULL,
      volume numeric NOT NULL,
      money numeric NOT NULL,
      open_interest numeric NOT NULL,
      PRIMARY KEY (code, time)
    );
    CREATE INDEX IF NOT EXISTS idx_code ON futures_minutely_bar (code);
    CREATE INDEX IF NOT EXISTS idx_time ON futures_minutely_bar (time);
  `)
}

pool.on('connect', (client) => {
  const schema = new URL(process.env.PGURL!).searchParams.get('schema') || 'public'
  client.query(`
    SET search_path TO ${schema};
    SET TIMEZONE TO 'Asia/Shanghai';
  `)
})

await initDb()
