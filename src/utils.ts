import { addDays, differenceInDays } from 'date-fns'

/**
 * 将日期范围按n天一组进行分组
 * @param startDate 开始日期 (YYYY-MM-DD格式)
 * @param endDate 结束日期 (YYYY-MM-DD格式)
 * @returns 日期范围数组，每组包含开始和结束日期
 */
export function splitDateRangeIntoDayChunks(
  startDate: Date,
  endDate: Date,
  chunkSize: number,
): Array<{ start: Date, end: Date }> {
  const chunks: Array<{ start: Date, end: Date }> = []
  let currentStart = startDate
  while (differenceInDays(endDate, currentStart) > 0) {
    const chunkEnd = differenceInDays(endDate, currentStart) >= chunkSize
      ? addDays(currentStart, chunkSize - 1)
      : endDate
    chunks.push({
      start: currentStart,
      end: chunkEnd,
    })
    currentStart = addDays(chunkEnd, 1)
  }

  return chunks
}
