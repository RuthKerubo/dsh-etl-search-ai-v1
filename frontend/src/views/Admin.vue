<script setup>
import { ref, onMounted } from 'vue'
import api from '../api.js'

const pendingCount = ref(0)

onMounted(async () => {
  try {
    const { data } = await api.get('/admin/pending', { params: { page_size: 1 } })
    pendingCount.value = data.total
  } catch {
    // ignore
  }
})
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-8">
    <h1 class="text-2xl font-bold text-gray-900 mb-6">Admin Dashboard</h1>

    <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">
      <RouterLink to="/admin/upload"
        class="block p-6 bg-white rounded-lg shadow hover:shadow-md transition-shadow">
        <h2 class="text-lg font-semibold text-gray-900">Upload PDF</h2>
        <p class="mt-1 text-sm text-gray-500">Upload a new PDF document for metadata extraction</p>
      </RouterLink>

      <RouterLink to="/admin/pending"
        class="block p-6 bg-white rounded-lg shadow hover:shadow-md transition-shadow">
        <div class="flex items-center justify-between">
          <h2 class="text-lg font-semibold text-gray-900">Pending Datasets</h2>
          <span v-if="pendingCount > 0"
            class="px-2.5 py-0.5 bg-amber-100 text-amber-800 text-sm font-medium rounded-full">
            {{ pendingCount }}
          </span>
        </div>
        <p class="mt-1 text-sm text-gray-500">Review, edit, and approve or reject pending datasets</p>
      </RouterLink>
    </div>
  </div>
</template>
