import { createRouter, createWebHistory } from 'vue-router'

import BatchDetailView from '@/views/BatchDetailView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 入湖批次明细数据集有专用工作台（逐行核对 + 拒绝回执），需优先于动态路由注册
    { path: '/batch_detail', name: 'batch-detail', component: BatchDetailView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
