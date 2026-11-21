<template>
  <section class="page" data-module="batch_replay">
    <header class="page-head">
      <div>
        <h2>批量回放</h2>
        <p class="page-desc">
          多选异常记录归并历史分区，生成补偿分区清单；回刷规划只给具有有效源表和加工口径的日期生成执行批次。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="reload-exceptions" type="button" @click="loadExceptions">
          刷新异常
        </button>
        <button class="btn primary" data-testid="create-replay" type="button" :disabled="!selectedIds.length || busy" @click="create">
          生成补偿分区清单
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="replay-error">{{ errorMessage }}</p>
    <p v-if="notice" class="notice-text" data-testid="replay-notice">{{ notice }}</p>

    <div class="stat-row">
      <article v-for="card in statCards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>

    <div class="replay-layout">
      <section class="replay-panel">
        <h3>异常记录（可多选）</h3>
        <table class="data-table">
          <thead>
            <tr>
              <th>选择</th>
              <th>发生时间</th>
              <th>所属管线</th>
              <th>异常类型</th>
              <th>影响分区</th>
              <th>处理状态</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="row in exceptions"
              :key="String(row.id)"
              :data-testid="`exception-${row.id}`"
              :class="{ selected: selectedIds.includes(Number(row.id)) }"
            >
              <td>
                <input
                  type="checkbox"
                  :data-testid="`select-${row.id}`"
                  :checked="selectedIds.includes(Number(row.id))"
                  @change="toggle(Number(row.id))"
                />
              </td>
              <td>{{ row['发生时间'] }}</td>
              <td>{{ row['所属管线'] }}</td>
              <td>{{ row['异常类型'] }}</td>
              <td>{{ row['影响分区'] }}</td>
              <td>{{ row['处理状态'] ?? row.status }}</td>
            </tr>
            <tr v-if="!exceptions.length">
              <td colspan="6" class="empty-state">暂无可回放的异常记录</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="replay-panel">
        <h3>补偿分区清单</h3>
        <table class="data-table" data-testid="manifest-table">
          <thead>
            <tr>
              <th>目标表</th>
              <th>源表</th>
              <th>加工口径</th>
              <th>实际缺口</th>
              <th>重叠区间</th>
              <th>原异常引用</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in manifest" :key="item.target_table" :data-testid="`manifest-${item.target_table}`">
              <td>{{ item.target_table }}</td>
              <td>{{ item.source_table || '—' }}</td>
              <td>{{ item.processing_spec || '—' }}</td>
              <td>{{ item.actual_gap.join('、') || '—' }}</td>
              <td>{{ item.overlap_partitions.join('、') || '—' }}</td>
              <td>{{ item.exception_refs.join('、') || '—' }}</td>
            </tr>
            <tr v-if="!manifest.length">
              <td colspan="6" class="empty-state">选择异常后生成清单</td>
            </tr>
          </tbody>
        </table>
        <p v-if="manifest.length" class="hint-text">
          说明：重叠区间的交点只进入清单一次；缺少有效源表或加工口径的目标表不会生成批次。
          <span v-if="invalidTargets.length">当前无效目标：{{ invalidTargets.join('、') }}</span>
        </p>
      </section>
    </div>

    <section v-if="replayId" class="replay-panel" data-testid="replay-detail">
      <h3>回放请求 {{ replayCode }}</h3>
      <div class="page-actions replay-toolbar">
        <button class="btn primary" data-testid="plan-replay" type="button" :disabled="busy" @click="planBatches">
          规划回刷批次
        </button>
        <button class="btn" data-testid="retry-replay" type="button" :disabled="busy" @click="retry">
          补源失败重试
        </button>
        <button class="btn" data-testid="compare-replay" type="button" :disabled="busy" @click="compare">
          补齐前后对比
        </button>
      </div>

      <form class="filter-bar" @submit.prevent="supplement">
        <label class="filter-item">
          <span>补源表</span>
          <input v-model="sourceTable" data-testid="source-table" placeholder="如 dw.dws_sales_summary" />
        </label>
        <label class="filter-item">
          <span>补源分区</span>
          <input v-model="sourcePartitions" data-testid="source-partitions" placeholder="2026-11-04 或 2026-11-02~2026-11-04" />
        </label>
        <label class="filter-item">
          <span>本次失败分区（可选）</span>
          <input v-model="failedPartitions" data-testid="failed-partitions" placeholder="2026-11-04" />
        </label>
        <button class="btn" data-testid="supplement-source" type="submit" :disabled="busy">登记补源</button>
      </form>

      <table class="data-table" data-testid="plan-table">
        <thead>
          <tr>
            <th>执行批次</th>
            <th>目标表</th>
            <th>可执行分区</th>
            <th>阻塞分区</th>
            <th>依赖链</th>
            <th>状态</th>
            <th>写入</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in plan?.execution_batches ?? []" :key="batch.execution_batch_id">
            <td>{{ batch.execution_batch_id }}</td>
            <td>{{ batch.target_table }}</td>
            <td data-testid="executable-partitions">{{ batch.partitions.join('、') }}</td>
            <td>{{ batch.blocked_partitions.join('、') || '—' }}</td>
            <td>{{ formatChain(batch.dependency_chain) }}</td>
            <td>{{ batch.status }}</td>
            <td>
              <button
                class="link"
                type="button"
                :data-testid="`write-${batch.execution_batch_id}`"
                :disabled="batch.status === '已写入'"
                @click="write(batch.execution_batch_id)"
              >
                确认写入
              </button>
            </td>
          </tr>
          <tr v-if="!(plan?.execution_batches ?? []).length">
            <td colspan="7" class="empty-state">暂无执行批次，请先规划或补齐上游数据</td>
          </tr>
        </tbody>
      </table>

      <p class="hint-text">
        可执行分区：{{ plan?.executable_partitions.join('、') || '—' }}；阻塞分区：{{
          plan?.blocked_partitions.join('、') || '—'
        }}
      </p>
      <p v-if="replayDetail?.source_missing && Object.keys(replayDetail.source_missing).length" class="hint-text">
        仍缺失的源分区：
        <span v-for="(parts, table) in replayDetail.source_missing" :key="table">
          {{ table }} → {{ parts.join('、') }}
        </span>
      </p>
    </section>

    <section v-if="compareResult" class="replay-panel" data-testid="compare-panel">
      <h3>补齐上游前后对比</h3>
      <table class="data-table">
        <thead>
          <tr><th>阶段</th><th>可执行分区</th><th>阻塞分区</th></tr>
        </thead>
        <tbody>
          <tr>
            <td>首轮（补源前）</td>
            <td data-testid="before-partitions">{{ compareResult.before_executable_partitions.join('、') || '—' }}</td>
            <td>{{ compareResult.before_source_supplement?.blocked_partitions.join('、') || '—' }}</td>
          </tr>
          <tr>
            <td>当前（补源后）</td>
            <td data-testid="after-partitions">{{ compareResult.executable_partitions.join('、') || '—' }}</td>
            <td>{{ compareResult.blocked_partitions.join('、') || '—' }}</td>
          </tr>
        </tbody>
      </table>
      <h4>实际写过的目标表</h4>
      <table class="data-table" data-testid="write-table">
        <thead>
          <tr><th>目标表</th><th>已写入分区</th><th>写入笔数</th></tr>
        </thead>
        <tbody>
          <tr v-for="target in compareResult.targets" :key="target.target_table">
            <td>{{ target.target_table }}</td>
            <td>{{ target.partitions.join('、') }}</td>
            <td>{{ target.write_count }}</td>
          </tr>
          <tr v-if="!compareResult.targets.length">
            <td colspan="3" class="empty-state">还没有确认写入的分区</td>
          </tr>
        </tbody>
      </table>
      <p class="hint-text">待写入分区：{{ compareResult.awaiting_write_partitions.join('、') || '—' }}</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  compareReplay,
  confirmWrite,
  createReplay,
  getReplay,
  planReplay,
  retryReplay,
  supplementSource,
} from '@/api/batch-replay'
import type { CompareResult, ManifestItem, ReplayDetail, ReplayPlan } from '@/api/batch-replay'
import { listEntries } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const exceptions = ref<EntryRow[]>([])
const selectedIds = ref<number[]>([])
const manifest = ref<ManifestItem[]>([])
const replayId = ref<number | null>(null)
const replayCode = ref('')
const plan = ref<ReplayPlan | null>(null)
const replayDetail = ref<ReplayDetail | null>(null)
const compareResult = ref<CompareResult | null>(null)
const sourceTable = ref('')
const sourcePartitions = ref('')
const failedPartitions = ref('')
const errorMessage = ref('')
const notice = ref('')
const busy = ref(false)

const invalidTargets = computed(() =>
  manifest.value.filter((item) => !item.valid).map((item) => item.target_table),
)
const statCards = computed(() => [
  { label: '已选异常', value: selectedIds.value.length },
  { label: '目标表', value: manifest.value.length },
  {
    label: '实际缺口分区',
    value: manifest.value.reduce((sum, item) => sum + item.actual_gap.length, 0),
  },
  {
    label: '重叠交点',
    value: manifest.value.reduce((sum, item) => sum + item.overlap_partitions.length, 0),
  },
])

function toggle(id: number) {
  selectedIds.value = selectedIds.value.includes(id)
    ? selectedIds.value.filter((item) => item !== id)
    : [...selectedIds.value, id]
}

function formatChain(chain: string[][]): string {
  return chain.length ? chain.map(([from, to]) => `${from}→${to}`).join('，') : '—'
}

async function loadExceptions() {
  errorMessage.value = ''
  try {
    const payload = await listEntries('exception_capture', { page: '1', size: '200' })
    exceptions.value = payload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '异常记录读取失败'
  }
}

function applyDetail(detail: ReplayDetail) {
  replayId.value = detail.replay_id
  replayCode.value = detail.replay_code
  manifest.value = detail.manifest
  plan.value = detail.plan
  replayDetail.value = detail
  if (!sourceTable.value && detail.manifest.length) {
    sourceTable.value = detail.manifest[0].source_table
  }
}

async function create() {
  busy.value = true
  errorMessage.value = ''
  notice.value = ''
  compareResult.value = null
  try {
    const result = await createReplay(selectedIds.value)
    manifest.value = result.manifest
    replayId.value = result.replay_id
    replayCode.value = result.replay_code
    plan.value = null
    notice.value = result.message
    await refreshDetail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成清单失败'
  } finally {
    busy.value = false
  }
}

async function refreshDetail() {
  if (!replayId.value) return
  applyDetail(await getReplay(replayId.value))
}

async function planBatches() {
  if (!replayId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await planReplay(replayId.value)
    manifest.value = result.manifest
    plan.value = result.plan
    notice.value = result.message
    await refreshDetail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '规划失败'
  } finally {
    busy.value = false
  }
}

async function supplement() {
  if (!replayId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await supplementSource(replayId.value, {
      source_table: sourceTable.value,
      partitions: sourcePartitions.value,
      failed_partitions: failedPartitions.value,
    })
    manifest.value = result.manifest
    plan.value = result.plan
    notice.value = `${result.message}；本次释放：${result.released_target_partitions.join('、') || '无'}`
    await refreshDetail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补源登记失败'
  } finally {
    busy.value = false
  }
}

async function retry() {
  if (!replayId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await retryReplay(replayId.value)
    plan.value = result.plan
    notice.value = result.message
    await refreshDetail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重试规划失败'
  } finally {
    busy.value = false
  }
}

async function write(executionBatchId: string) {
  if (!replayId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await confirmWrite(replayId.value, { execution_batch_id: executionBatchId })
    notice.value = result.message
    await refreshDetail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '写入确认失败'
  } finally {
    busy.value = false
  }
}

async function compare() {
  if (!replayId.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    compareResult.value = await compareReplay(replayId.value)
    await refreshDetail()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '对比失败'
  } finally {
    busy.value = false
  }
}

onMounted(loadExceptions)
</script>

<style scoped>
.replay-layout { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.replay-panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-bottom: 12px; }
.replay-panel h3 { margin: 0 0 10px; font-size: 14px; }
.replay-panel h4 { margin: 12px 0 6px; font-size: 13px; }
.replay-toolbar { margin-bottom: 10px; }
.replay-panel tr.selected { background: #eff6ff; }
.notice-text { color: #0f766e; font-size: 13px; }
.hint-text { color: var(--muted); font-size: 12px; margin: 8px 0 0; }
</style>
