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
    <h1 class="text-2xl font-bold text-emerald-900 mb-6">Admin Dashboard</h1>

    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">

      <!-- Bulk Import -->
      <RouterLink to="/admin/bulk-upload"
        class="block p-6 bg-white rounded-xl border border-green-100 shadow-sm hover:shadow-md hover:border-green-300 transition-all group">
        <div class="flex items-center gap-3 mb-2">
          <div class="w-9 h-9 rounded-lg bg-green-50 flex items-center justify-center shrink-0">
            <svg class="w-5 h-5 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
            </svg>
          </div>
          <h2 class="text-base font-semibold text-emerald-900 group-hover:text-green-700 transition-colors">
            Bulk Import
          </h2>
        </div>
        <p class="text-sm text-gray-500">
          Import datasets from JSON, CSV, XLSX, or PDF files directly into the catalogue.
        </p>
        <div class="mt-3 flex gap-1.5 flex-wrap">
          <span v-for="ext in ['.json', '.csv', '.xlsx', '.pdf']" :key="ext"
            class="px-1.5 py-0.5 bg-gray-100 text-gray-500 text-xs rounded font-mono">{{ ext }}</span>
        </div>
      </RouterLink>

      <!-- PDF Upload (pending review) -->
      <RouterLink to="/admin/upload"
        class="block p-6 bg-white rounded-xl border border-green-100 shadow-sm hover:shadow-md hover:border-green-300 transition-all group">
        <div class="flex items-center gap-3 mb-2">
          <div class="w-9 h-9 rounded-lg bg-red-50 flex items-center justify-center shrink-0">
            <svg class="w-5 h-5 text-red-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
            </svg>
          </div>
          <h2 class="text-base font-semibold text-emerald-900 group-hover:text-green-700 transition-colors">
            PDF Upload
          </h2>
        </div>
        <p class="text-sm text-gray-500">
          Upload a PDF for AI metadata extraction, then review and approve before publishing.
        </p>
        <div class="mt-3">
          <span class="px-2 py-0.5 bg-amber-50 text-amber-700 text-xs rounded-full font-medium">
            Requires review
          </span>
        </div>
      </RouterLink>

      <!-- Pending Queue -->
      <RouterLink to="/admin/pending"
        class="block p-6 bg-white rounded-xl border border-green-100 shadow-sm hover:shadow-md hover:border-green-300 transition-all group">
        <div class="flex items-center gap-3 mb-2">
          <div class="w-9 h-9 rounded-lg bg-amber-50 flex items-center justify-center shrink-0">
            <svg class="w-5 h-5 text-amber-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
            </svg>
          </div>
          <div class="flex items-center gap-2 flex-1 min-w-0">
            <h2 class="text-base font-semibold text-emerald-900 group-hover:text-green-700 transition-colors">
              Pending Review
            </h2>
            <span v-if="pendingCount > 0"
              class="px-2 py-0.5 bg-amber-100 text-amber-800 text-xs font-medium rounded-full shrink-0">
              {{ pendingCount }}
            </span>
          </div>
        </div>
        <p class="text-sm text-gray-500">
          Review, edit, and approve or reject datasets extracted from uploaded PDFs.
        </p>
      </RouterLink>

    </div>
  </div>
</template>
