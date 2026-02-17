<script setup>
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import api from '../api.js'

const route = useRoute()
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
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-8">
    <RouterLink to="/" class="text-sm text-indigo-600 hover:underline mb-4 inline-block">&larr; Back to search</RouterLink>

    <div v-if="loading" class="text-center py-12 text-gray-500">Loading...</div>
    <div v-else-if="error" class="p-4 bg-red-50 text-red-700 rounded">{{ error }}</div>

    <div v-else-if="dataset" class="bg-white rounded-lg shadow p-6">
      <h1 class="text-2xl font-bold text-gray-900 mb-4">{{ dataset.title }}</h1>

      <!-- ISO 19115 Compliance Badge -->
      <div v-if="dataset.iso_compliance" class="mb-6">
        <div v-if="dataset.iso_compliance.compliant"
          class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-green-100 text-green-800 text-sm font-medium rounded-full">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" />
          </svg>
          ISO 19115 Compliant
        </div>
        <div v-else>
          <span class="inline-flex items-center gap-1.5 px-3 py-1.5 bg-yellow-100 text-yellow-800 text-sm font-medium rounded-full">
            Score: {{ dataset.iso_compliance.score }}%
          </span>
          <ul class="mt-2 text-sm text-yellow-700 list-disc list-inside">
            <li v-for="field in dataset.iso_compliance.missing_required" :key="'req-'+field">
              Missing required: {{ field }}
            </li>
            <li v-for="field in dataset.iso_compliance.missing_recommended" :key="'rec-'+field">
              Missing recommended: {{ field }}
            </li>
          </ul>
        </div>
      </div>

      <section class="mb-6">
        <h2 class="text-sm font-semibold text-gray-500 uppercase mb-2">Abstract</h2>
        <p class="text-gray-700 whitespace-pre-line">{{ dataset.abstract }}</p>
      </section>

      <section v-if="dataset.keywords?.length" class="mb-6">
        <h2 class="text-sm font-semibold text-gray-500 uppercase mb-2">Keywords</h2>
        <div class="flex flex-wrap gap-2">
          <span v-for="kw in dataset.keywords" :key="kw"
            class="px-3 py-1 bg-indigo-50 text-indigo-700 text-sm rounded-full">{{ kw }}</span>
        </div>
      </section>

      <section v-if="dataset.topic_categories?.length" class="mb-6">
        <h2 class="text-sm font-semibold text-gray-500 uppercase mb-2">Topic Categories</h2>
        <div class="flex flex-wrap gap-2">
          <span v-for="tc in dataset.topic_categories" :key="tc"
            class="px-3 py-1 bg-green-50 text-green-700 text-sm rounded-full">{{ tc }}</span>
        </div>
      </section>

      <section v-if="dataset.lineage" class="mb-6">
        <h2 class="text-sm font-semibold text-gray-500 uppercase mb-2">Lineage</h2>
        <p class="text-gray-700 whitespace-pre-line">{{ dataset.lineage }}</p>
      </section>

      <section v-if="dataset.bounding_box" class="mb-6">
        <h2 class="text-sm font-semibold text-gray-500 uppercase mb-2">Bounding Box</h2>
        <div class="grid grid-cols-2 gap-2 text-sm text-gray-700 max-w-xs">
          <div>West: {{ dataset.bounding_box.west }}</div>
          <div>East: {{ dataset.bounding_box.east }}</div>
          <div>South: {{ dataset.bounding_box.south }}</div>
          <div>North: {{ dataset.bounding_box.north }}</div>
        </div>
      </section>

      <section v-if="dataset.temporal_extent" class="mb-6">
        <h2 class="text-sm font-semibold text-gray-500 uppercase mb-2">Temporal Extent</h2>
        <p class="text-sm text-gray-700">
          {{ dataset.temporal_extent.start || '—' }} to {{ dataset.temporal_extent.end || '—' }}
        </p>
      </section>

      <div class="mt-4 pt-4 border-t text-xs text-gray-400">
        ID: {{ dataset.identifier }}
      </div>
    </div>
  </div>
</template>
