/** 批量回放接口封装：多选异常 → 补偿分区清单 → 回刷批次 → 补源 → 写入确认。 */
import { request } from './client'

export type ManifestItem = {
  target_table: string
  pipeline: string
  source_table: string
  processing_spec: string
  requested_partitions: string[]
  actual_gap: string[]
  exception_refs: number[]
  overlap_partitions: string[]
  missing_source_partitions: string[]
  already_written: string[]
  valid: boolean
  invalid_reason: string
}

export type ManifestSummary = {
  target_count: number
  exception_count: number
  requested_partitions: number
  actual_gap_partitions: number
  overlap_partitions: string[]
  invalid_targets: string[]
}

export type ExecutionBatch = {
  execution_batch_id: string
  target_table: string
  source_table: string
  processing_spec: string
  pipeline: string
  exception_refs: number[]
  partitions: string[]
  partition_count: number
  blocked_partitions: string[]
  depends_on: Record<string, string[]>
  dependency_chain: string[][]
  status: string
}

export type ReplayPlan = {
  batch_id: string
  execution_batches: ExecutionBatch[]
  executable_targets: { target_table: string; partitions: string[] }[]
  executable_partitions: string[]
  blocked_partitions: string[]
  blocked_details: { target_table: string; source_table: string; partition: string; reason: string }[]
  skipped_entries: { target_table: string; exception_refs: number[]; reason: string; partitions: string[] }[]
  not_planned: { target_table: string; reason: string; partitions: string[] }[]
  execution_batch_count: number
  executable_count: number
  blocked_count: number
  parallelism: number
}

export type WriteSummary = {
  written_targets: string[]
  targets: { target_table: string; partitions: string[]; write_count: number }[]
  written_partitions: string[]
  total_partitions: number
}

export type ReplayDetail = {
  ok: boolean
  replay_id: number
  replay_code: string
  status: string
  exception_ids: number[]
  manifest: ManifestItem[]
  summary: ManifestSummary
  first_plan: ReplayPlan | null
  plan: ReplayPlan | null
  writes: WriteSummary
  source_missing: Record<string, string[]>
  created_at: string
  updated_at: string
}

export type CompareResult = {
  ok: boolean
  replay_id: number
  replay_code: string
  before_source_supplement: ReplayPlan | null
  before_executable_partitions: string[]
  current_plan: ReplayPlan
  manifest: ManifestItem[]
  executable_partitions: string[]
  blocked_partitions: string[]
  written_targets: string[]
  written_partitions: string[]
  targets: WriteSummary['targets']
  awaiting_write_partitions: string[]
  still_missing_source: Record<string, string[]>
  summary: ManifestSummary
}

async function readJson<T>(response: Response): Promise<T> {
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    const detail = (payload as { detail?: string; message?: string }).detail
      ?? (payload as { message?: string }).message
      ?? '批量回放操作失败'
    throw new Error(detail)
  }
  return payload as T
}

export async function previewReplay(exceptionIds: number[]): Promise<{
  exception_ids: number[]
  missing_exception_ids: number[]
  manifest: ManifestItem[]
  summary: ManifestSummary
}> {
  const response = await request('/api/batch-replay/preview', {
    method: 'POST',
    body: JSON.stringify({ exception_ids: exceptionIds }),
  })
  return readJson(response)
}

export async function createReplay(exceptionIds: number[]): Promise<{
  ok: boolean
  message: string
  replay_id: number
  replay_code: string
  manifest: ManifestItem[]
  summary: ManifestSummary
  missing_exception_ids: number[]
}> {
  const response = await request('/api/batch-replay/replays', {
    method: 'POST',
    body: JSON.stringify({ exception_ids: exceptionIds }),
  })
  return readJson(response)
}

export async function planReplay(replayId: number, parallelism = 1): Promise<{
  message: string
  plan: ReplayPlan
  manifest: ManifestItem[]
  summary: ManifestSummary
}> {
  const response = await request(`/api/batch-replay/replays/${replayId}/plan`, {
    method: 'POST',
    body: JSON.stringify({ parallelism }),
  })
  return readJson(response)
}

export async function getReplay(replayId: number): Promise<ReplayDetail> {
  return readJson<ReplayDetail>(await request(`/api/batch-replay/replays/${replayId}`))
}

export async function supplementSource(
  replayId: number,
  payload: {
    source_table: string
    partitions: string
    failed_partitions?: string
    succeeded?: boolean
  },
): Promise<{
  message: string
  succeeded_partitions: string[]
  failed_partitions: string[]
  still_missing_partitions: string[]
  released_target_partitions: string[]
  plan: ReplayPlan
  manifest: ManifestItem[]
  summary: ManifestSummary
}> {
  const response = await request(`/api/batch-replay/replays/${replayId}/source-supplement`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return readJson(response)
}

export async function retryReplay(replayId: number): Promise<{
  message: string
  plan: ReplayPlan
  retry: {
    still_missing_source: Record<string, string[]>
    resumable_partitions: string[]
    blocked_partitions: string[]
  }
}> {
  const response = await request(`/api/batch-replay/replays/${replayId}/retry`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return readJson(response)
}

export async function confirmWrite(
  replayId: number,
  payload: { execution_batch_id?: string; claims?: { target_table: string; partitions: string[] }[] },
): Promise<{
  ok: boolean
  message: string
  claims: { target_table: string; partition: string }[]
  writes: WriteSummary
}> {
  const response = await request(`/api/batch-replay/replays/${replayId}/writes`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return readJson(response)
}

export async function compareReplay(replayId: number): Promise<CompareResult> {
  return readJson<CompareResult>(
    await request(`/api/batch-replay/replays/${replayId}/compare`),
  )
}
