<template>
  <section class="page batch-page" data-module="batch_detail">
    <header class="page-head">
      <div>
        <h2>入湖批次明细数据集</h2>
        <p class="page-desc">
          解析管道配置与待入湖样本，按来源文件、原始行号、去重键逐行核对清洗结果，
          有效行、重复行与脏行来源全部可定位，净数据版本与隔离原因一起落库。
        </p>
      </div>
      <div class="page-actions">
        <label class="parse-control">
          <span>入湖管道</span>
          <select v-model.number="pipelineId" data-testid="select-pipeline">
            <option v-for="p in pipelines" :key="p.id" :value="Number(p.id)">
              {{ p.id }} · {{ p['管道名称'] }}
            </option>
          </select>
        </label>
        <label class="parse-control">
          <span>版本口径</span>
          <select v-model="caliber" data-testid="select-caliber">
            <option v-for="item in options.calibers" :key="item.value" :value="item.value">
              {{ item.label }}
            </option>
          </select>
        </label>
        <button class="btn primary" data-testid="parse-batch" type="button" :disabled="parsing" @click="onParse">
          {{ parsing ? '解析中…' : '解析生成批次明细' }}
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="card in statCards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value" :class="card.tone">{{ card.value }}</strong>
      </article>
    </div>

    <div class="batch-layout">
      <div class="batch-list-panel">
        <div class="panel-head">
          <h3>入湖批次（主数据）</h3>
          <div class="filter-bar inline">
            <select v-model="statusFilter" data-testid="batch-status-filter" @change="reloadBatches">
              <option value="">全部状态</option>
              <option v-for="s in batchStatuses" :key="s" :value="s">{{ s }}</option>
            </select>
            <button class="btn ghost" type="button" @click="reloadBatches">刷新</button>
          </div>
        </div>
        <table class="data-table batch-table">
          <thead>
            <tr>
              <th>批次号</th>
              <th>口径</th>
              <th>采集时间</th>
              <th>明细版本</th>
              <th>有效</th>
              <th>重复</th>
              <th>脏行</th>
              <th>当前状态</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="batch in batches"
              :key="batch.id"
              :class="{ selected: selectedBatchId === batch.id }"
              :data-testid="`batch-row-${batch.id}`"
              @click="selectBatch(batch.id)"
            >
              <td>{{ batch.批次号 }}</td>
              <td>{{ batch.口径说明 }}</td>
              <td>{{ batch.采集时间 }}</td>
              <td class="mono">{{ batch.明细版本 }}</td>
              <td class="tone-valid">{{ batch.有效行数 }}</td>
              <td class="tone-duplicate">{{ batch.重复行数 }}</td>
              <td class="tone-dirty">{{ batch.脏行数 }}</td>
              <td><span class="status-pill" :data-status="batch.批次状态">{{ batch.批次状态 }}</span></td>
            </tr>
            <tr v-if="!batches.length">
              <td colspan="8" class="empty-state">暂无批次，先选择管道与口径后解析生成明细数据集</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="selectedBatch" class="batch-detail-panel">
        <div class="panel-head">
          <h3>批次明细 · {{ selectedBatch.批次号 }}</h3>
          <div class="row-actions">
            <button class="btn" data-testid="rerun-batch" type="button" @click="onRerun">批次重跑</button>
            <button class="btn" data-testid="gate-batch" type="button" @click="onQualityGate">质量门禁采样</button>
            <button class="btn primary" data-testid="confirm-batch" type="button" @click="onConfirm">核对确认</button>
          </div>
        </div>

        <dl class="batch-meta">
          <div><dt>管道名称</dt><dd>{{ selectedBatch.管道名称 }}</dd></div>
          <div><dt>目标湖区</dt><dd>{{ selectedBatch.目标湖区 }}</dd></div>
          <div><dt>来源文件</dt><dd>{{ selectedBatch.来源文件.join('、') }}</dd></div>
          <div><dt>采集时间</dt><dd>{{ selectedBatch.全部采集时间.join(' / ') }}</dd></div>
          <div><dt>口径</dt><dd>{{ selectedBatch.口径说明 }}</dd></div>
          <div><dt>明细版本 / 净数据版本</dt><dd class="mono">{{ selectedBatch.明细版本 }} / {{ selectedBatch.净数据版本 }}</dd></div>
          <div><dt>净数据路径</dt><dd class="mono small">{{ selectedBatch.净数据路径 }}</dd></div>
          <div><dt>校验和</dt><dd class="mono">{{ selectedBatch.校验和 }}</dd></div>
          <div><dt>重跑次数</dt><dd>{{ selectedBatch.重跑次数 }}</dd></div>
          <div><dt>消费凭据</dt><dd class="mono small">{{ selectedBatch.消费凭据 ? `${selectedBatch.消费凭据.receipt_no} @ ${selectedBatch.消费凭据.consumed_at}` : '—' }}</dd></div>
        </dl>

        <div v-if="selectedBatch.改动点.length" class="change-point" data-testid="change-points">
          <strong>补报改动点（不同采集时间）：</strong>
          <span v-for="point in selectedBatch.改动点" :key="point.去重键" class="change-tag">
            去重键 {{ point.去重键 }} · 行 {{ point.行号.join('/') }} · {{ point.采集时间.join(' 与 ') }}
          </span>
        </div>
        <div v-if="selectedBatch.回滚依据" class="rollback-basis">
          回滚依据：{{ JSON.stringify(selectedBatch.回滚依据) }}
        </div>

        <div class="result-tabs" data-testid="result-tabs">
          <button
            v-for="tab in resultTabs"
            :key="tab.value"
            type="button"
            class="tab-btn"
            :class="['tone-' + tab.tone, { active: resultFilter === tab.value }]"
            @click="setResultFilter(tab.value)"
          >
            {{ tab.label }} <em>{{ tab.count }}</em>
          </button>
        </div>

        <form class="filter-bar inline" @submit.prevent="reloadLines">
          <input v-model="keyword" placeholder="按去重键 / 来源文件定位行" data-testid="line-keyword" />
          <button class="btn" type="submit">定位</button>
          <button class="btn ghost" type="button" @click="resetLineFilter">重置</button>
        </form>

        <div class="table-scroll">
          <table class="data-table line-table">
            <thead>
              <tr>
                <th>来源文件</th>
                <th>源文件偏移</th>
                <th>原始行号</th>
                <th>样本序号</th>
                <th>去重键</th>
                <th>业务生效日</th>
                <th>采集时间</th>
                <th>清洗结果</th>
                <th>隔离原因 / 异常摘要</th>
                <th>隔离通道</th>
                <th>净数据版本</th>
                <th>校验指纹</th>
                <th>更新时间</th>
                <th>拒绝回执</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="line in lines"
                :key="line.id"
                :data-testid="`line-row-${line.id}`"
                :data-result="line.清洗结果"
              >
                <td class="small">{{ line.来源文件 }}<span v-if="line.补报改动" class="backfill-flag">补报</span></td>
                <td class="mono small">{{ line.源文件偏移 }}</td>
                <td>{{ line.原始行号 }}</td>
                <td>{{ line.样本序号 }}</td>
                <td class="mono">{{ line.去重键 || '—' }}</td>
                <td>{{ line.业务生效日 || '—' }}</td>
                <td class="small">{{ line.采集时间 }}</td>
                <td><span class="result-pill" :data-result="line.清洗结果">{{ line.清洗结果 }}</span></td>
                <td class="small reason-cell">{{ line.隔离原因 || line.异常摘要 || '—' }}</td>
                <td class="mono small">{{ line.隔离通道 || '—' }}</td>
                <td class="mono small">{{ line.净数据版本 }}</td>
                <td class="mono small">{{ line.校验指纹 }}</td>
                <td class="small">{{ line.更新时间 }}</td>
                <td>
                  <button v-if="line.拒绝回执ID" class="link" type="button" @click="openReceipt(line.id)">
                    {{ line.拒绝回执ID }}
                  </button>
                  <span v-else>—</span>
                </td>
              </tr>
              <tr v-if="!lines.length">
                <td colspan="14" class="empty-state">当前清洗结果下没有明细行</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="review-panel">
      <div class="panel-head">
        <h3>待复核清单（质量门禁 → 入湖管道跨模块核对）</h3>
        <button class="btn ghost" type="button" @click="reloadReviews">刷新跨模块状态</button>
      </div>
      <table class="data-table">
        <thead>
          <tr><th>批次号</th><th>明细版本</th><th>净数据版本</th><th>跨模块状态</th><th>批次状态快照</th><th>最近刷新时间</th><th>操作</th></tr>
        </thead>
        <tbody>
          <tr v-for="review in reviews" :key="review.id" :data-testid="`review-row-${review.id}`">
            <td>{{ review.批次号 }}</td>
            <td class="mono small">{{ review.明细版本 }}</td>
            <td class="mono small">{{ review.净数据版本 }}</td>
            <td>{{ review.跨模块状态 }}</td>
            <td>{{ review.批次状态快照 }}</td>
            <td class="small">{{ review.最近刷新时间 }}</td>
            <td><button class="link" type="button" @click="onRefreshReview(review.id)">再核对一次</button></td>
          </tr>
          <tr v-if="!reviews.length"><td colspan="7" class="empty-state">暂无待复核记录</td></tr>
        </tbody>
      </table>
    </div>

    <div v-if="receiptModal.open" class="modal-backdrop" data-testid="receipt-modal" @click.self="receiptModal.open = false">
      <div class="modal receipt-modal">
        <h3>采样拒绝回执</h3>
        <pre class="receipt-json">{{ receiptModal.content }}</pre>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="receiptModal.open = false">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span data-testid="line-count">共 {{ lines.length }} 条明细行（批次总行数 {{ selectedBatch ? selectedBatch.校验摘要.总来源行数 : 0 }}）</span>
      <span v-if="message" class="status-message" :class="messageTone">{{ message }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'
import {
  confirmBatch,
  lineReceipt,
  listBatchLines,
  listBatches,
  listReviews,
  loadBatchOptions,
  parseBatch,
  refreshReview,
  rerunBatch,
  runQualityGate,
  type BatchDetailOptions,
  type BatchLine,
  type BatchSummary,
  type ReviewItem,
} from '@/api/batch-detail'

type PipelineOption = { id: number; 管道名称: string }

const options = ref<BatchDetailOptions>({ calibers: [], clean_results: [] })
const pipelines = ref<PipelineOption[]>([])
const batches = ref<BatchSummary[]>([])
const lines = ref<BatchLine[]>([])
const reviews = ref<ReviewItem[]>([])
const selectedBatch = ref<BatchSummary | null>(null)
const pipelineId = ref(1)
const caliber = ref('arrival')
const parsing = ref(false)
const statusFilter = ref('')
const resultFilter = ref('')
const keyword = ref('')
const selectedBatchId = ref<number | null>(null)
const message = ref('')
const messageTone = ref('')
const receiptModal = ref<{ open: boolean; content: string }>({ open: false, content: '' })

const batchStatuses = ['解析中', '待核对', '已回执', '已确认', '已中断']

const statCards = computed(() => [
  { label: '批次总数', value: batches.value.length, tone: '' },
  { label: '待核对', value: batches.value.filter((b) => b.批次状态 === '待核对').length, tone: '' },
  { label: '已确认', value: batches.value.filter((b) => b.批次状态 === '已确认').length, tone: 'tone-valid' },
  { label: '已中断', value: batches.value.filter((b) => b.批次状态 === '已中断').length, tone: 'tone-dirty' },
])

const resultTabs = computed(() => {
  const countOf = (result: string) => linesAll.value.filter((line) => line.清洗结果 === result).length
  return [
    { value: '', label: '全部', count: linesAll.value.length, tone: 'all' },
    { value: '有效行', label: '有效行', count: countOf('有效行'), tone: 'valid' },
    { value: '重复行', label: '重复行', count: countOf('重复行'), tone: 'duplicate' },
    { value: '脏行-缺键行', label: '缺键行', count: countOf('脏行-缺键行'), tone: 'dirty' },
    { value: '脏行-格式异常行', label: '格式异常行', count: countOf('脏行-格式异常行'), tone: 'dirty' },
    { value: '脏行-空值行', label: '空值行', count: countOf('脏行-空值行'), tone: 'dirty' },
  ]
})

// 标签计数始终基于全量明细，而不是当前过滤结果
const linesAll = ref<BatchLine[]>([])

function notify(text: string, tone: 'ok' | 'error' = 'ok') {
  message.value = text
  messageTone.value = tone === 'ok' ? 'status-ok' : 'status-error'
}

async function loadPipelines() {
  const payload = await fetchJson<{ items: PipelineOption[] }>('/api/ingest_pipeline?size=200')
  pipelines.value = payload.items
  if (pipelines.value.length) pipelineId.value = Number(pipelines.value[0].id)
}

async function reloadBatches() {
  const payload = await listBatches(undefined, statusFilter.value || undefined)
  batches.value = payload.items
  if (selectedBatchId.value && !batches.value.some((b) => b.id === selectedBatchId.value)) {
    selectedBatchId.value = null
    selectedBatch.value = null
    lines.value = []
    linesAll.value = []
  }
}

async function selectBatch(batchId: number) {
  selectedBatchId.value = batchId
  await loadLines(true)
}

async function loadLines(fullReload = false) {
  if (selectedBatchId.value === null) return
  if (fullReload) {
    resultFilter.value = ''
    keyword.value = ''
  }
  const cleanResult = fullReload ? undefined : resultFilter.value || undefined
  const kw = fullReload ? undefined : keyword.value || undefined
  const payload = await listBatchLines(selectedBatchId.value, cleanResult, kw)
  if (!payload.ok) {
    notify(payload.error || '明细读取失败', 'error')
    return
  }
  selectedBatch.value = payload.batch
  lines.value = payload.items
  if (fullReload || (!resultFilter.value && !keyword.value)) {
    linesAll.value = payload.items
  }
}

async function reloadLines() {
  await loadLines(false)
}

function setResultFilter(value: string) {
  resultFilter.value = value
  void loadLines(false)
}

function resetLineFilter() {
  resultFilter.value = ''
  keyword.value = ''
  void loadLines(true)
}

async function onParse() {
  parsing.value = true
  message.value = ''
  try {
    const result = await parseBatch(pipelineId.value, caliber.value)
    if (!result.ok || !result.batch) {
      notify(result.error || '解析失败', 'error')
      return
    }
    notify(`批次 ${result.batch.批次号} 已生成明细数据集（${result.batch.明细版本}）`)
    await reloadBatches()
    await selectBatch(result.batch.id)
    await reloadReviews()
  } finally {
    parsing.value = false
  }
}

async function onRerun() {
  if (!selectedBatch.value) return
  const result = await rerunBatch(selectedBatch.value.id)
  if (!result.ok) {
    notify(result.error || '重跑失败', 'error')
  } else {
    notify(`批次重跑完成，重跑次数 ${result.batch?.重跑次数}，已确认行保持不变`)
  }
  await reloadBatches()
  await loadLines(true)
}

async function onQualityGate() {
  if (!selectedBatch.value) return
  const result = await runQualityGate(selectedBatch.value.id, 1)
  if (!result.ok || !result.receipt) {
    notify(result.error || '门禁拒绝接收', 'error')
    return
  }
  notify(`完整拒绝回执 ${result.receipt.回执编号}：采样 ${result.receipt.采样行数} 行，拦截 ${result.receipt.拦截行数} 行`)
  await reloadBatches()
  await loadLines(true)
  await reloadReviews()
}

async function onConfirm() {
  if (!selectedBatch.value) return
  const result = await confirmBatch(selectedBatch.value.id)
  if (!result.ok) {
    notify(result.error || '确认失败', 'error')
    return
  }
  notify('跨模块核对完成，明细行已冻结并回写待复核清单')
  await reloadBatches()
  await loadLines(true)
  await reloadReviews()
}

async function reloadReviews() {
  reviews.value = (await listReviews()).items
}

async function onRefreshReview(reviewId: number) {
  const result = await refreshReview(reviewId)
  if (result.ok) notify(`跨模块状态已刷新：${result.review?.跨模块状态}`)
  await reloadReviews()
}

async function openReceipt(lineId: number) {
  const payload = await lineReceipt(lineId)
  receiptModal.value = {
    open: true,
    content: JSON.stringify({ line: payload.line, receipt: payload.receipt }, null, 2),
  }
}

onMounted(async () => {
  options.value = await loadBatchOptions()
  await loadPipelines()
  await reloadBatches()
  await reloadReviews()
  if (batches.value.length) await selectBatch(batches.value[0].id)
})
</script>

<style scoped>
.batch-page { display: flex; flex-direction: column; gap: 12px; }
.parse-control { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.parse-control select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.batch-layout { display: grid; grid-template-columns: minmax(360px, 0.9fr) 1.6fr; gap: 12px; align-items: start; }
.batch-list-panel, .batch-detail-panel, .review-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.panel-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; gap: 8px; flex-wrap: wrap; }
.panel-head h3 { margin: 0; font-size: 15px; }
.filter-bar.inline { margin: 0; padding: 0; background: transparent; border: none; }
.batch-table tbody tr { cursor: pointer; }
.batch-table tr.selected { outline: 2px solid var(--brand); outline-offset: -2px; }
.mono { font-family: ui-monospace, monospace; }
.small { font-size: 12px; max-width: 220px; word-break: break-all; }
.tone-valid { color: #15803d; }
.tone-duplicate { color: #b45309; }
.tone-dirty { color: #b42318; }
.status-pill, .result-pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; white-space: nowrap; }
.status-pill[data-status='已确认'] { background: #dcfce7; color: #15803d; }
.status-pill[data-status='待核对'] { background: #fef9c3; color: #a16207; }
.status-pill[data-status='已回执'] { background: #dbeafe; color: #1d4ed8; }
.status-pill[data-status='已中断'] { background: #fee2e2; color: #b42318; }
.status-pill[data-status='解析中'] { background: #f1f5f9; color: #475569; }
.result-pill[data-result='有效行'] { background: #dcfce7; color: #15803d; }
.result-pill[data-result='重复行'] { background: #fef3c7; color: #b45309; }
.result-pill[data-result^='脏行'] { background: #fee2e2; color: #b42318; }
.batch-meta { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 16px; margin: 0 0 10px; }
.batch-meta div { display: flex; gap: 8px; min-width: 0; }
.batch-meta dt { color: var(--muted); font-size: 12px; flex-shrink: 0; width: 130px; }
.batch-meta dd { margin: 0; font-size: 13px; word-break: break-all; }
.change-point { background: #fffbeb; border: 1px solid #fde68a; border-radius: 6px; padding: 8px 10px; font-size: 12px; margin-bottom: 10px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
.change-tag { background: #fef3c7; border-radius: 4px; padding: 2px 6px; }
.rollback-basis { background: #fef2f2; border: 1px solid #fecaca; border-radius: 6px; padding: 8px; font-size: 12px; margin-bottom: 10px; word-break: break-all; }
.result-tabs { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 10px; }
.tab-btn { border: 1px solid var(--border); background: #fff; border-radius: 999px; padding: 4px 12px; cursor: pointer; font-size: 12px; }
.tab-btn em { font-style: normal; margin-left: 4px; font-weight: 700; }
.tab-btn.active { outline: 2px solid var(--brand); outline-offset: -2px; }
.table-scroll { overflow-x: auto; max-height: 520px; overflow-y: auto; }
.line-table { min-width: 1500px; }
.reason-cell { max-width: 260px; }
.backfill-flag { display: inline-block; margin-left: 4px; background: #ede9fe; color: #6d28d9; border-radius: 4px; padding: 0 4px; font-size: 10px; }
.review-panel { margin-top: 4px; }
.receipt-modal { width: min(760px, 92vw); }
.receipt-json { background: #0f172a; color: #e2e8f0; border-radius: 8px; padding: 12px; font-size: 12px; max-height: 60vh; overflow: auto; }
.status-message { font-size: 12px; }
.status-ok { color: #15803d; }
.status-error { color: #b42318; }
@media (max-width: 1100px) { .batch-layout { grid-template-columns: 1fr; } }
</style>
