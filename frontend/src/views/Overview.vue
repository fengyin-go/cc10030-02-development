<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "湖区分层", "created": 0, "pending": 0, "abnormal": 0}, {"name": "入湖管道", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema注册", "created": 0, "pending": 0, "abnormal": 0}, {"name": "分区策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "存储格式", "created": 0, "pending": 0, "abnormal": 0}, {"name": "留存策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "元数据目录", "created": 0, "pending": 0, "abnormal": 0}, {"name": "质量门禁", "created": 0, "pending": 0, "abnormal": 0}, {"name": "湖内去重", "created": 0, "pending": 0, "abnormal": 0}, {"name": "变更追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "操作审计", "created": 0, "pending": 0, "abnormal": 0}, {"name": "快照管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "过期清理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "冷热分层", "created": 0, "pending": 0, "abnormal": 0}, {"name": "跨区复制", "created": 0, "pending": 0, "abnormal": 0}, {"name": "模式演进", "created": 0, "pending": 0, "abnormal": 0}, {"name": "入湖监控", "created": 0, "pending": 0, "abnormal": 0}, {"name": "灾备策略", "created": 0, "pending": 0, "abnormal": 0}, {"name": "数据字典", "created": 0, "pending": 0, "abnormal": 0}, {"name": "访问管控", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
