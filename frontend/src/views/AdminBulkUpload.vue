<script setup>
import { ref } from 'vue'
import api from '../api.js'

const file = ref(null)
const source = ref('manual_upload')
const uploading = ref(false)
const error = ref('')
const result = ref(null)

const ALLOWED = ['.json', '.csv', '.xlsx', '.xls', '.pdf']

function onFileChange(e) {
  const selected = e.target.files[0] || null
  error.value = ''
  result.value = null

  if (selected) {
    const lower = selected.name.toLowerCase()
    const ok = ALLOWED.some(ext => lower.endsWith(ext))
    if (!ok) {
      error.value = `Unsupported file type. Allowed: ${ALLOWED.join(', ')}`
      file.value = null
      e.target.value = ''
      return
    }
  }
  file.value = selected
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function fileExt(name) {
  if (!name) return ''
  const m = name.match(/\.[^.]+$/)
  return m ? m[0].toLowerCase() : ''
}

function extColor(name) {
  const ext = fileExt(name)
  if (ext === '.json') return 'bg-blue-100 text-blue-700'
  if (ext === '.csv') return 'bg-green-100 text-green-700'
  if (ext === '.xlsx' || ext === '.xls') return 'bg-emerald-100 text-emerald-700'
  if (ext === '.pdf') return 'bg-red-100 text-red-700'
  return 'bg-gray-100 text-gray-600'
}

async function upload() {
  if (!file.value) return
  uploading.value = true
  error.value = ''
  result.value = null

  const formData = new FormData()
  formData.append('file', file.value)

  try {
    const { data } = await api.post('/admin/bulk-upload', formData, {
      params: { source: source.value },
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
    <RouterLink to="/admin" class="text-sm text-green-700 hover:underline mb-4 inline-block">
      &larr; Admin
    </RouterLink>
    <h1 class="text-2xl font-bold text-emerald-900 mb-1">Bulk Import Datasets</h1>
    <p class="text-sm text-gray-500 mb-6">
      Import datasets directly from a structured file. Records go straight to the catalogue
      (no review step). For PDF uploads requiring manual review, use
      <RouterLink to="/admin/upload" class="text-green-700 hover:underline">PDF Upload</RouterLink>.
    </p>

    <!-- Supported formats -->
    <div class="mb-6 grid grid-cols-2 sm:grid-cols-4 gap-3 text-center text-xs">
      <div v-for="fmt in [
        { ext: '.json', label: 'JSON', hint: 'Single object or array' },
        { ext: '.csv',  label: 'CSV',  hint: 'Header row required' },
        { ext: '.xlsx', label: 'XLSX', hint: 'First row = headers' },
        { ext: '.pdf',  label: 'PDF',  hint: 'Single dataset, text extracted' },
      ]" :key="fmt.ext"
        class="rounded-lg border border-green-100 bg-white p-3">
        <div class="font-mono font-bold text-emerald-700 text-base">{{ fmt.ext }}</div>
        <div class="font-medium text-gray-700 mt-0.5">{{ fmt.label }}</div>
        <div class="text-gray-400 mt-0.5 leading-tight">{{ fmt.hint }}</div>
      </div>
    </div>

    <div class="bg-white rounded-xl border border-green-100 shadow-sm p-6 space-y-5">

      <!-- Error -->
      <div v-if="error" class="p-3 bg-red-50 text-red-700 rounded-lg text-sm">{{ error }}</div>

      <!-- File picker -->
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-2">
          Select file
          <span class="font-normal text-gray-400">(max 10 MB)</span>
        </label>
        <input type="file"
          :accept="ALLOWED.join(',')"
          @change="onFileChange"
          class="block w-full text-sm text-gray-500
            file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0
            file:text-sm file:font-medium file:bg-green-50 file:text-green-700
            hover:file:bg-green-100 cursor-pointer" />

        <!-- File preview chip -->
        <div v-if="file" class="mt-2 flex items-center gap-2">
          <span class="px-2 py-0.5 rounded text-xs font-mono font-bold" :class="extColor(file.name)">
            {{ fileExt(file.name) }}
          </span>
          <span class="text-sm text-gray-700 truncate">{{ file.name }}</span>
          <span class="text-xs text-gray-400 shrink-0">{{ formatBytes(file.size) }}</span>
        </div>
      </div>

      <!-- Source label -->
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">
          Source label
          <span class="font-normal text-gray-400">(stored on each dataset for filtering)</span>
        </label>
        <input v-model="source" type="text" placeholder="e.g. manual_upload, partner_data"
          class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-green-300" />
      </div>

      <!-- Upload button -->
      <button @click="upload" :disabled="!file || uploading"
        class="w-full bg-green-600 text-white py-2.5 rounded-lg text-sm font-medium
               hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors
               flex items-center justify-center gap-2">
        <svg v-if="uploading" class="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
          <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" />
          <path class="opacity-75" fill="currentColor"
            d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
        </svg>
        {{ uploading ? 'Importing…' : 'Import Datasets' }}
      </button>
    </div>

    <!-- Result -->
    <div v-if="result" class="mt-6 rounded-xl border shadow-sm p-6 space-y-3"
      :class="result.success ? 'bg-green-50 border-green-200' : 'bg-amber-50 border-amber-200'">

      <div class="flex items-center gap-3">
        <!-- Success / partial icon -->
        <svg v-if="result.success" class="w-6 h-6 text-green-600 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
        <svg v-else class="w-6 h-6 text-amber-500 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
            d="M12 9v2m0 4h.01M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" />
        </svg>
        <p class="font-semibold" :class="result.success ? 'text-green-800' : 'text-amber-800'">
          {{ result.message }}
        </p>
      </div>

      <div class="text-sm" :class="result.success ? 'text-green-700' : 'text-amber-700'">
        {{ result.datasets_created }} dataset{{ result.datasets_created !== 1 ? 's' : '' }} added to the catalogue.
      </div>

      <!-- Errors list -->
      <div v-if="result.errors?.length" class="mt-2 space-y-1">
        <p class="text-xs font-semibold text-red-700">Errors (first {{ result.errors.length }}):</p>
        <p v-for="(e, i) in result.errors" :key="i" class="text-xs text-red-600">{{ e }}</p>
      </div>
    </div>

    <!-- Format help -->
    <div class="mt-8 text-sm text-gray-500 space-y-2">
      <p class="font-medium text-gray-700">Tips</p>
      <ul class="list-disc list-inside space-y-1 text-xs">
        <li>JSON objects must have at least a <code class="bg-gray-100 px-1 rounded">title</code> field.</li>
        <li>CSV/XLSX: use a <code class="bg-gray-100 px-1 rounded">keywords</code> column with comma-separated values.</li>
        <li>Set <code class="bg-gray-100 px-1 rounded">access_level</code> to <em>public</em>, <em>restricted</em>, or <em>admin_only</em> (defaults to public).</li>
        <li>Embeddings and ISO 19115 compliance are computed automatically on import.</li>
      </ul>
    </div>
  </div>
</template>
