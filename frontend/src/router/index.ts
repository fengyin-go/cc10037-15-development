import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import BatchReplayView from '@/views/BatchReplayView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    // 批量回放是跨异常记录的组合视图，必须排在通配模块路由之前。
    { path: '/batch-replay', name: 'batch-replay', component: BatchReplayView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
