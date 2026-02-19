<script setup>
import { ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import api from '../api.js'

const route = useRoute()
const router = useRouter()

const results = ref([])
const meta = ref(null)
const loading = ref(false)
const error = ref('')
const searchQuery = ref(route.query.q || '')

async function doSearch(q) {
  if (!q) return
  loading.value = true
  error.value = ''
  try {
    const { data } = await api.get('/search', { params: { q, limit: 20 } })
    results.value = data.results
    meta.value = {
      total: data.total,
      mode: data.mode,
      duration_ms: data.duration_ms,
      semantic_results: data.semantic_results,
      keyword_results: data.keyword_results,
    }
  } catch (e) {
    error.value = e.response?.data?.detail || 'Search failed'
  } finally {
    loading.value = false
  }
}

watch(() => route.query.q, (q) => {
  searchQuery.value = q || ''
  doSearch(q)
}, { immediate: true })

function newSearch() {
  if (searchQuery.value.trim()) {
    router.push({ path: '/search', query: { q: searchQuery.value.trim() } })
  }
}

// Score → green shade: high score = dark green, low = light
function scoreBadgeClass(score) {
  if (score >= 0.75) return 'bg-green-100 text-green-800'
  if (score >= 0.45) return 'bg-emerald-50 text-emerald-700'
  return 'bg-gray-100 text-gray-500'
}
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-6">

    <!-- Search bar -->
    <form @submit.prevent="newSearch"
      class="flex mb-5 shadow-sm rounded-xl overflow-hidden border border-green-200">
      <input v-model="searchQuery" type="text" placeholder="Search datasets..."
        class="flex-1 px-4 py-3 text-sm border-0 focus:ring-0 focus:outline-none bg-white" />
      <button type="submit"
        class="bg-green-600 text-white px-5 py-3 text-sm font-medium hover:bg-green-700 transition-colors shrink-0">
        Search
      </button>
    </form>

    <!-- Loading -->
    <div v-if="loading" class="flex items-center justify-center py-20 gap-3 text-gray-400">
      <svg class="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
        <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
        <path class="opacity-75" fill="currentColor"
          d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
      </svg>
      Searching...
    </div>

    <!-- Error -->
    <div v-else-if="error" class="p-4 bg-red-50 text-red-700 rounded-lg text-sm">{{ error }}</div>

    <template v-else-if="meta">

      <!-- Meta bar -->
      <div class="flex items-center justify-between mb-4 text-sm text-gray-500">
        <span>
          <span class="font-semibold text-gray-800">{{ meta.total }}</span> results
          for "<span class="text-green-700">{{ route.query.q }}</span>"
        </span>
        <div class="flex items-center gap-2">
          <span class="px-2 py-0.5 rounded-full text-xs font-medium"
            :class="{
              'bg-green-100 text-green-700':   meta.mode === 'hybrid',
              'bg-emerald-100 text-emerald-700': meta.mode === 'semantic',
              'bg-gray-100 text-gray-600':       meta.mode === 'keyword',
            }">
            {{ meta.mode }}
          </span>
          <span class="text-gray-400">{{ meta.duration_ms }}ms</span>
        </div>
      </div>

      <!-- No results -->
      <div v-if="results.length === 0" class="text-center py-16 text-gray-400">
        <svg class="w-12 h-12 mx-auto mb-3 opacity-40" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
            d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        No results found for "{{ route.query.q }}"
      </div>

      <!-- Results -->
      <div v-else class="space-y-3">
        <RouterLink v-for="r in results" :key="r.identifier" :to="`/datasets/${r.identifier}`"
          class="block bg-white rounded-xl border border-green-100 p-4 shadow-sm hover:shadow-md hover:border-green-300 transition-all group">

          <!-- Title row -->
          <div class="flex items-start justify-between gap-3 mb-2">
            <h2
              class="text-base font-semibold text-emerald-900 group-hover:text-green-700 transition-colors leading-snug">
              {{ r.title }}
            </h2>
            <!-- Score pill — shade varies with score -->
            <span class="shrink-0 text-xs font-mono px-2 py-0.5 rounded-full" :class="scoreBadgeClass(r.score)">
              {{ (r.score * 100).toFixed(1) }}%
            </span>
          </div>

          <!-- Abstract -->
          <p class="text-sm text-gray-600 line-clamp-2 mb-3">{{ r.abstract }}</p>

          <!-- Footer -->
          <div class="flex flex-wrap items-center gap-2">
            <span v-for="kw in r.keywords.slice(0, 4)" :key="kw"
              class="px-2 py-0.5 bg-green-50 text-green-700 text-xs rounded-full">{{ kw }}</span>

            <span class="flex-1" />

            <!-- Semantic badge -->
            <span v-if="r.from_semantic"
              class="flex items-center gap-1 px-2 py-0.5 bg-green-100 text-green-700 text-xs rounded-full font-medium">
              <svg class="w-3 h-3" fill="currentColor" viewBox="0 0 20 20">
                <path d="M9 9a2 2 0 114 0 2 2 0 01-4 0z" />
                <path fill-rule="evenodd" clip-rule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-13a4 4 0 00-3.446 6.032l-2.261 2.26a1 1 0 101.414 1.415l2.261-2.261A4 4 0 1011 5z" />
              </svg>
              semantic
            </span>
            <!-- Keyword badge -->
            <span v-if="r.from_keyword"
              class="flex items-center gap-1 px-2 py-0.5 bg-emerald-50 text-emerald-600 text-xs rounded-full font-medium">
              <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                  d="M7 20l4-16m2 16l4-16M6 9h14M4 15h14" />
              </svg>
              keyword
            </span>
            <!-- Access level badge (only shown for non-public datasets) -->
            <span v-if="r.access_level && r.access_level !== 'public'"
              class="px-2 py-0.5 text-xs rounded-full font-medium"
              :class="{
                'bg-amber-100 text-amber-700': r.access_level === 'restricted',
                'bg-red-100 text-red-700': r.access_level === 'admin_only',
              }">
              {{ r.access_level === 'restricted' ? 'Restricted' : 'Admin Only' }}
            </span>
          </div>
        </RouterLink>
      </div>
    </template>
  </div>
</template>
