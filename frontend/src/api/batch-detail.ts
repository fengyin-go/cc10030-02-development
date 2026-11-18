import { fetchJson, request } from './client'

export type CaliberOption = { value: string; label: string }
export type BatchDetailOptions = {
  calibers: CaliberOption[]
  clean_results: string[]
}

export type BatchSummary = {
  id: number
  批次号: string
  管道ID: number
  管道名称: string
  目标湖区: string
  采集时间: string
  全部采集时间: string[]
  来源文件: string[]
  口径: string
  口径说明: string
  批次状态: string
  重跑次数: number
  明细版本: string
  净数据版本: string
  净数据路径: string
  校验和: string
  有效行数: number
  重复行数: number
  脏行数: number
  校验摘要: Record<string, number | string>
  消费凭据: { receipt_no: string; checksum: string; consumed_at: string } | null
  回滚依据: Record<string, unknown> | null
  改动点: { 去重键: string; 涉及文件: string[]; 采集时间: string[]; 行号: number[] }[]
  更新时间: string
}

export type BatchLine = {
  id: number
  批次ID: number
  来源文件: string
  源文件偏移: string
  原始行号: number
  样本序号: number
  去重键: string
  采集时间: string
  业务生效日: string
  清洗结果: string
  隔离原因: string
  隔离通道: string
  明细版本: string
  净数据版本: string
  净数据路径: string
  校验指纹: string
  异常摘要: string
  拒绝回执ID: string
  已确认: boolean
  重跑序号: number
  更新时间: string
  原始样本: Record<string, string>
  补报改动?: boolean
}

export type ReviewItem = {
  id: number
  批次ID: number
  批次号: string
  管道ID: number
  明细版本: string
  净数据版本: string
  跨模块状态: string
  批次状态快照: string
  最近刷新时间: string
}

async function postJson<T>(path: string, body: unknown = {}): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  return (await response.json()) as T
}

export function loadBatchOptions(): Promise<BatchDetailOptions> {
  return fetchJson<BatchDetailOptions>('/api/batch-detail/options')
}

export function parseBatch(
  pipelineId: number,
  caliber: string,
): Promise<{ ok: boolean; batch?: BatchSummary; error?: string; summary?: Record<string, unknown> }> {
  return postJson('/api/batch-detail/parse', { pipeline_id: pipelineId, caliber })
}

export async function listBatches(
  pipelineId?: number,
  status?: string,
): Promise<{ items: BatchSummary[]; total: number }> {
  const params = new URLSearchParams()
  if (pipelineId) params.set('pipeline_id', String(pipelineId))
  if (status) params.set('status', status)
  const query = params.toString()
  return fetchJson(`/api/batch-detail/batches${query ? `?${query}` : ''}`)
}

export function listBatchLines(
  batchId: number,
  cleanResult?: string,
  keyword?: string,
): Promise<{ ok: boolean; items: BatchLine[]; total: number; batch: BatchSummary; error?: string }> {
  const params = new URLSearchParams()
  if (cleanResult) params.set('clean_result', cleanResult)
  if (keyword) params.set('keyword', keyword)
  const query = params.toString()
  return fetchJson(`/api/batch-detail/batches/${batchId}/lines${query ? `?${query}` : ''}`)
}

export function rerunBatch(batchId: number) {
  return postJson<{ ok: boolean; error?: string; batch?: BatchSummary }>(
    `/api/batch-detail/batches/${batchId}/rerun`,
  )
}

export function runQualityGate(batchId: number, sampleRatio = 1) {
  return postJson<{
    ok: boolean
    error?: string
    receipt?: {
      回执编号: string
      采样比例: number
      采样行数: number
      拦截行数: number
      回执校验和: string
      完整回执: boolean
      回执条目: Record<string, unknown>[]
    }
  }>(`/api/batch-detail/batches/${batchId}/quality-gate`, { sample_ratio: sampleRatio })
}

export function confirmBatch(
  batchId: number,
): Promise<{ ok: boolean; error?: string; batch?: BatchSummary }> {
  return postJson(`/api/batch-detail/batches/${batchId}/confirm`)
}

export function listReviews(): Promise<{ items: ReviewItem[]; total: number }> {
  return fetchJson('/api/batch-detail/reviews')
}

export function refreshReview(reviewId: number) {
  return postJson<{ ok: boolean; review?: ReviewItem; error?: string }>(
    `/api/batch-detail/reviews/${reviewId}/refresh`,
  )
}

export function lineReceipt(lineId: number) {
  return fetchJson<{
    ok: boolean
    line: BatchLine
    receipt: Record<string, unknown> | null
  }>(`/api/batch-detail/lines/${lineId}/receipt`)
}
