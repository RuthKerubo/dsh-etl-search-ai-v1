<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import api from '../api.js'

const route = useRoute()
const router = useRouter()
const dataset = ref(null)
const loading = ref(true)
const error = ref('')

onMounted(async () => {
  try {
    const { data } = await api.get(`/datasets/${route.params.id}`)
    dataset.value = data
  } catch (e) {
    error.value = e.response?.data?.detail || 'Failed to load dataset'
  } finally {
    loading.value = false
  }
})

const compliance = computed(() => dataset.value?.iso_compliance ?? null)
const scoreColor = computed(() => {
  const s = compliance.value?.score ?? 0
  if (s === 100) return 'text-green-600'
  if (s >= 60)  return 'text-amber-600'
  return 'text-red-600'
})
const scoreBg = computed(() => {
  const s = compliance.value?.score ?? 0
  if (s === 100) return 'bg-green-50 border-green-200'
  if (s >= 60)  return 'bg-amber-50 border-amber-200'
  return 'bg-red-50 border-red-200'
})

function fmtDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleDateString('en-GB', { year: 'numeric', month: 'short', day: 'numeric' })
}

function askAboutDataset() {
  const q = `Tell me about the dataset: ${dataset.value.title}`
  router.push({ path: '/chat', query: { q } })
}
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-8">
    <button @click="$router.back()"
      class="flex items-center gap-1 text-sm text-indigo-600 hover:text-indigo-800 mb-5 transition-colors">
      <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" />
      </svg>
      Back
    </button>

    <div v-if="loading" class="flex items-center justify-center py-20 gap-3 text-gray-400">
      <svg class="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
        <path class="opacity-75" fill="currentColor"
          d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
      </svg>
      Loading dataset...
    </div>

    <div v-else-if="error" class="p-4 bg-red-50 text-red-700 rounded-lg text-sm">{{ error }}</div>

    <div v-else-if="dataset" class="space-y-4">

      <!-- Title card -->
      <div class="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
        <div class="flex items-start justify-between gap-4 flex-wrap">
          <h1 class="text-xl font-bold text-gray-900 flex-1">{{ dataset.title }}</h1>
          <!-- Ask button -->
          <button @click="askAboutDataset"
            class="shrink-0 flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white text-sm font-medium rounded-lg hover:bg-indigo-700 transition-colors">
            <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
            </svg>
            Ask about this dataset
          </button>
        </div>
        <p class="mt-1 text-xs text-gray-400 font-mono">{{ dataset.identifier }}</p>
      </div>

      <!-- ISO Compliance card -->
      <div v-if="compliance" class="bg-white rounded-xl border shadow-sm p-5" :class="scoreBg">
        <div class="flex items-center justify-between flex-wrap gap-3">
          <div class="flex items-center gap-3">
            <!-- Score ring -->
            <div class="flex flex-col items-center justify-center w-14 h-14 rounded-full border-2 font-bold text-lg"
              :class="[scoreColor, scoreBg]">
              {{ compliance.score }}
            </div>
            <div>
              <div class="font-semibold text-sm text-gray-800">ISO 19115 Compliance</div>
              <div class="text-xs mt-0.5" :class="scoreColor">
                {{ compliance.compliant ? 'Fully compliant — all required fields present' : 'Partially compliant' }}
              </div>
            </div>
          </div>
          <svg v-if="compliance.compliant" class="w-7 h-7 text-green-500 shrink-0" fill="none" stroke="currentColor"
            viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
        </div>

        <!-- Missing fields -->
        <div v-if="compliance.missing_required?.length || compliance.missing_recommended?.length"
          class="mt-3 pt-3 border-t border-gray-200 space-y-1">
          <p v-for="w in compliance.missing_required" :key="'r-'+w"
            class="text-xs text-red-600">Required: {{ w }} is missing</p>
          <p v-for="w in compliance.missing_recommended" :key="'rec-'+w"
            class="text-xs text-amber-600">Recommended: {{ w }} is missing</p>
        </div>
      </div>

      <!-- Main metadata -->
      <div class="bg-white rounded-xl border border-gray-200 shadow-sm p-6 space-y-6">

        <!-- Abstract -->
        <section>
          <h2 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Abstract</h2>
          <p class="text-sm text-gray-700 leading-relaxed whitespace-pre-line">{{ dataset.abstract }}</p>
        </section>

        <!-- Keywords -->
        <section v-if="dataset.keywords?.length">
          <h2 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Keywords</h2>
          <div class="flex flex-wrap gap-2">
            <span v-for="kw in dataset.keywords" :key="kw"
              class="px-3 py-1 bg-indigo-50 text-indigo-700 text-xs rounded-full font-medium">{{ kw }}</span>
          </div>
        </section>

        <!-- Topic categories -->
        <section v-if="dataset.topic_categories?.length">
          <h2 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Topic Categories</h2>
          <div class="flex flex-wrap gap-2">
            <span v-for="tc in dataset.topic_categories" :key="tc"
              class="px-3 py-1 bg-emerald-50 text-emerald-700 text-xs rounded-full font-medium">{{ tc }}</span>
          </div>
        </section>

        <!-- Temporal + Bounding Box side by side -->
        <div class="grid sm:grid-cols-2 gap-6">

          <!-- Temporal extent -->
          <section v-if="dataset.temporal_extent">
            <h2 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Temporal Extent</h2>
            <div class="flex items-center gap-3 text-sm text-gray-700">
              <div class="flex flex-col items-center">
                <span class="text-xs text-gray-400 mb-1">Start</span>
                <span class="font-medium">{{ fmtDate(dataset.temporal_extent.start) }}</span>
              </div>
              <div class="flex-1 border-t-2 border-dashed border-indigo-200 relative">
                <span class="absolute -top-3 left-1/2 -translate-x-1/2 text-xs text-indigo-400">span</span>
              </div>
              <div class="flex flex-col items-center">
                <span class="text-xs text-gray-400 mb-1">End</span>
                <span class="font-medium">{{ fmtDate(dataset.temporal_extent.end) }}</span>
              </div>
            </div>
          </section>

          <!-- Bounding box -->
          <section v-if="dataset.bounding_box">
            <h2 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Bounding Box</h2>
            <!-- Compass-style grid -->
            <div class="grid grid-cols-3 grid-rows-3 gap-1 text-xs text-center w-fit">
              <div></div>
              <div class="bg-indigo-50 rounded px-2 py-1 font-medium text-indigo-700">
                N {{ dataset.bounding_box.north }}°
              </div>
              <div></div>
              <div class="bg-indigo-50 rounded px-2 py-1 font-medium text-indigo-700">
                W {{ dataset.bounding_box.west }}°
              </div>
              <div class="bg-slate-100 rounded px-2 py-1 text-slate-400 text-[10px] leading-tight">
                lat/lon
              </div>
              <div class="bg-indigo-50 rounded px-2 py-1 font-medium text-indigo-700">
                E {{ dataset.bounding_box.east }}°
              </div>
              <div></div>
              <div class="bg-indigo-50 rounded px-2 py-1 font-medium text-indigo-700">
                S {{ dataset.bounding_box.south }}°
              </div>
              <div></div>
            </div>
          </section>
        </div>

        <!-- Lineage -->
        <section v-if="dataset.lineage">
          <h2 class="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Lineage</h2>
          <p class="text-sm text-gray-600 leading-relaxed whitespace-pre-line">{{ dataset.lineage }}</p>
        </section>

        <!-- Source / import info -->
        <div v-if="dataset.source" class="pt-4 border-t border-gray-100 flex flex-wrap gap-4 text-xs text-gray-400">
          <span>Source: <span class="font-medium text-gray-600">{{ dataset.source }}</span></span>
          <span v-if="dataset.imported_at">
            Imported: <span class="font-medium text-gray-600">{{ fmtDate(dataset.imported_at) }}</span>
          </span>
        </div>
      </div>
    </div>
  </div>
</template>
