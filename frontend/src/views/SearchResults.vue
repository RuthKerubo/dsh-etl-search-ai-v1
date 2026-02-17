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
    meta.value = { total: data.total, mode: data.mode, duration_ms: data.duration_ms }
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
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-8">
    <form @submit.prevent="newSearch" class="flex mb-6 shadow rounded-lg overflow-hidden">
      <input v-model="searchQuery" type="text" placeholder="Search datasets..."
        class="flex-1 px-4 py-3 border-0 focus:ring-0 focus:outline-none" />
      <button type="submit" class="bg-indigo-600 text-white px-6 py-3 hover:bg-indigo-700">Search</button>
    </form>

    <div v-if="loading" class="text-center py-12 text-gray-500">Searching...</div>

    <div v-else-if="error" class="p-4 bg-red-50 text-red-700 rounded">{{ error }}</div>

    <template v-else-if="meta">
      <div class="flex items-center justify-between mb-4 text-sm text-gray-500">
        <span>{{ meta.total }} results</span>
        <span>{{ meta.mode }} search &middot; {{ meta.duration_ms }}ms</span>
      </div>

      <div v-if="results.length === 0" class="text-center py-12 text-gray-500">
        No results found for "{{ route.query.q }}"
      </div>

      <div v-else class="space-y-4">
        <RouterLink v-for="r in results" :key="r.identifier" :to="`/datasets/${r.identifier}`"
          class="block p-4 bg-white rounded-lg shadow hover:shadow-md transition-shadow">
          <div class="flex items-start justify-between">
            <h2 class="text-lg font-semibold text-gray-900">{{ r.title }}</h2>
            <span class="ml-2 text-sm font-mono text-indigo-600 whitespace-nowrap">
              {{ (r.score * 100).toFixed(1) }}%
            </span>
          </div>
          <p class="mt-1 text-sm text-gray-600 line-clamp-2">{{ r.abstract }}</p>
          <div class="mt-2 flex flex-wrap gap-1">
            <span v-for="kw in r.keywords" :key="kw"
              class="px-2 py-0.5 bg-indigo-50 text-indigo-700 text-xs rounded-full">{{ kw }}</span>
          </div>
        </RouterLink>
      </div>
    </template>
  </div>
</template>
