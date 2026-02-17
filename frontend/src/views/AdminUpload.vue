<script setup>
import { ref } from 'vue'
import api from '../api.js'

const file = ref(null)
const uploading = ref(false)
const error = ref('')
const result = ref(null)

function onFileChange(e) {
  file.value = e.target.files[0] || null
  result.value = null
  error.value = ''
}

async function upload() {
  if (!file.value) return
  uploading.value = true
  error.value = ''
  result.value = null

  const formData = new FormData()
  formData.append('file', file.value)

  try {
    const { data } = await api.post('/admin/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    })
    result.value = data
    file.value = null
  } catch (e) {
    error.value = e.response?.data?.detail || 'Upload failed'
  } finally {
    uploading.value = false
  }
}
</script>

<template>
  <div class="max-w-2xl mx-auto px-4 py-8">
    <RouterLink to="/admin" class="text-sm text-indigo-600 hover:underline mb-4 inline-block">&larr; Admin</RouterLink>
    <h1 class="text-2xl font-bold text-gray-900 mb-6">Upload PDF</h1>

    <div class="bg-white rounded-lg shadow p-6">
      <div v-if="error" class="mb-4 p-3 bg-red-50 text-red-700 rounded text-sm">{{ error }}</div>

      <div class="mb-4">
        <label class="block text-sm font-medium text-gray-700 mb-2">Select PDF file</label>
        <input type="file" accept=".pdf" @change="onFileChange"
          class="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:text-sm file:font-medium file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100" />
      </div>

      <button @click="upload" :disabled="!file || uploading"
        class="bg-indigo-600 text-white px-6 py-2 rounded hover:bg-indigo-700 disabled:opacity-50">
        {{ uploading ? 'Uploading...' : 'Upload' }}
      </button>
    </div>

    <div v-if="result" class="mt-6 bg-green-50 rounded-lg shadow p-6">
      <h2 class="text-lg font-semibold text-green-800 mb-3">Extracted Metadata</h2>
      <dl class="space-y-2 text-sm">
        <div>
          <dt class="font-medium text-gray-700">Title</dt>
          <dd class="text-gray-900">{{ result.title || '—' }}</dd>
        </div>
        <div>
          <dt class="font-medium text-gray-700">Abstract</dt>
          <dd class="text-gray-900">{{ result.abstract || '—' }}</dd>
        </div>
        <div>
          <dt class="font-medium text-gray-700">Keywords</dt>
          <dd class="flex flex-wrap gap-1 mt-1">
            <span v-for="kw in result.keywords" :key="kw"
              class="px-2 py-0.5 bg-indigo-50 text-indigo-700 text-xs rounded-full">{{ kw }}</span>
            <span v-if="!result.keywords?.length" class="text-gray-400">None</span>
          </dd>
        </div>
        <div>
          <dt class="font-medium text-gray-700">Topic Categories</dt>
          <dd class="flex flex-wrap gap-1 mt-1">
            <span v-for="tc in result.topic_categories" :key="tc"
              class="px-2 py-0.5 bg-green-100 text-green-700 text-xs rounded-full">{{ tc }}</span>
            <span v-if="!result.topic_categories?.length" class="text-gray-400">None</span>
          </dd>
        </div>
        <div>
          <dt class="font-medium text-gray-700">Filename</dt>
          <dd class="text-gray-900">{{ result.filename }}</dd>
        </div>
      </dl>
      <p class="mt-4 text-sm text-gray-500">
        This document is now in the <RouterLink to="/admin/pending" class="text-indigo-600 hover:underline">pending queue</RouterLink>.
      </p>
    </div>
  </div>
</template>
