<script setup>
import { ref, onMounted } from 'vue'
import api from '../api.js'

const items = ref([])
const total = ref(0)
const loading = ref(true)
const error = ref('')
const editing = ref(null) // pending id being edited
const editForm = ref({})
const saving = ref(false)
const actionError = ref('')
const complianceResults = ref({}) // keyed by pending id
const checkingCompliance = ref({}) // keyed by pending id

async function checkCompliance(id) {
  checkingCompliance.value[id] = true
  try {
    const { data } = await api.get(`/admin/pending/${id}/compliance`)
    complianceResults.value[id] = data
  } catch (e) {
    complianceResults.value[id] = { error: e.response?.data?.detail || 'Check failed' }
  } finally {
    checkingCompliance.value[id] = false
  }
}

async function loadPending() {
  loading.value = true
  error.value = ''
  try {
    const { data } = await api.get('/admin/pending')
    items.value = data.items
    total.value = data.total
  } catch (e) {
    error.value = e.response?.data?.detail || 'Failed to load pending datasets'
  } finally {
    loading.value = false
  }
}

function startEdit(item) {
  editing.value = item.id
  editForm.value = {
    title: item.title || '',
    abstract: item.abstract || '',
    keywords: (item.keywords || []).join(', '),
    topic_categories: (item.topic_categories || []).join(', '),
    lineage: item.lineage || '',
  }
  actionError.value = ''
}

function cancelEdit() {
  editing.value = null
  editForm.value = {}
}

async function saveEdit(id) {
  saving.value = true
  actionError.value = ''
  try {
    const body = {
      title: editForm.value.title || null,
      abstract: editForm.value.abstract || null,
      keywords: editForm.value.keywords ? editForm.value.keywords.split(',').map(k => k.trim()).filter(Boolean) : null,
      topic_categories: editForm.value.topic_categories ? editForm.value.topic_categories.split(',').map(k => k.trim()).filter(Boolean) : null,
      lineage: editForm.value.lineage || null,
    }
    await api.put(`/admin/pending/${id}`, body)
    editing.value = null
    await loadPending()
  } catch (e) {
    actionError.value = e.response?.data?.detail || 'Save failed'
  } finally {
    saving.value = false
  }
}

async function approve(id) {
  if (!confirm('Approve this dataset?')) return
  actionError.value = ''
  try {
    await api.post(`/admin/approve/${id}`)
    await loadPending()
  } catch (e) {
    actionError.value = e.response?.data?.detail || 'Approve failed'
  }
}

async function reject(id) {
  if (!confirm('Reject and delete this dataset?')) return
  actionError.value = ''
  try {
    await api.delete(`/admin/reject/${id}`)
    await loadPending()
  } catch (e) {
    actionError.value = e.response?.data?.detail || 'Reject failed'
  }
}

onMounted(loadPending)
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-8">
    <RouterLink to="/admin" class="text-sm text-indigo-600 hover:underline mb-4 inline-block">&larr; Admin</RouterLink>
    <h1 class="text-2xl font-bold text-gray-900 mb-2">Pending Datasets</h1>
    <p class="text-sm text-gray-500 mb-6">{{ total }} pending</p>

    <div v-if="actionError" class="mb-4 p-3 bg-red-50 text-red-700 rounded text-sm">{{ actionError }}</div>

    <div v-if="loading" class="text-center py-12 text-gray-500">Loading...</div>
    <div v-else-if="error" class="p-4 bg-red-50 text-red-700 rounded">{{ error }}</div>
    <div v-else-if="items.length === 0" class="text-center py-12 text-gray-500">No pending datasets</div>

    <div v-else class="space-y-4">
      <div v-for="item in items" :key="item.id" class="bg-white rounded-lg shadow p-5">
        <!-- View mode -->
        <template v-if="editing !== item.id">
          <div class="flex items-start justify-between">
            <div class="flex-1">
              <h2 class="text-lg font-semibold text-gray-900">{{ item.title || 'Untitled' }}</h2>
              <p class="text-xs text-gray-400 mt-0.5">{{ item.filename }} &middot; {{ item.uploaded_by }} &middot; {{ new Date(item.uploaded_at).toLocaleDateString() }}</p>
            </div>
          </div>
          <p class="mt-2 text-sm text-gray-600 line-clamp-3">{{ item.abstract || 'No abstract' }}</p>
          <div v-if="item.keywords?.length" class="mt-2 flex flex-wrap gap-1">
            <span v-for="kw in item.keywords" :key="kw"
              class="px-2 py-0.5 bg-indigo-50 text-indigo-700 text-xs rounded-full">{{ kw }}</span>
          </div>
          <div class="mt-4 flex gap-2">
            <button @click="startEdit(item)"
              class="px-3 py-1.5 text-sm bg-gray-100 text-gray-700 rounded hover:bg-gray-200">Edit</button>
            <button @click="approve(item.id)"
              class="px-3 py-1.5 text-sm bg-green-600 text-white rounded hover:bg-green-700">Approve</button>
            <button @click="reject(item.id)"
              class="px-3 py-1.5 text-sm bg-red-600 text-white rounded hover:bg-red-700">Reject</button>
            <button @click="checkCompliance(item.id)" :disabled="checkingCompliance[item.id]"
              class="px-3 py-1.5 text-sm bg-indigo-100 text-indigo-700 rounded hover:bg-indigo-200 disabled:opacity-50">
              {{ checkingCompliance[item.id] ? 'Checking...' : 'Check Compliance' }}
            </button>
          </div>
          <!-- Compliance results -->
          <div v-if="complianceResults[item.id]" class="mt-3 p-3 rounded text-sm"
            :class="complianceResults[item.id].error ? 'bg-red-50 text-red-700' :
                     complianceResults[item.id].compliant ? 'bg-green-50 text-green-700' : 'bg-yellow-50 text-yellow-700'">
            <template v-if="complianceResults[item.id].error">
              {{ complianceResults[item.id].error }}
            </template>
            <template v-else>
              <div class="font-medium">
                <span v-if="complianceResults[item.id].compliant">ISO 19115 Compliant</span>
                <span v-else>Score: {{ complianceResults[item.id].score }}%</span>
              </div>
              <ul v-if="complianceResults[item.id].warnings?.length" class="mt-1 list-disc list-inside text-xs">
                <li v-for="(w, i) in complianceResults[item.id].warnings" :key="i">{{ w }}</li>
              </ul>
            </template>
          </div>
        </template>

        <!-- Edit mode -->
        <template v-else>
          <div class="space-y-3">
            <div>
              <label class="block text-xs font-medium text-gray-500 mb-1">Title</label>
              <input v-model="editForm.title" class="w-full px-3 py-2 border rounded text-sm" />
            </div>
            <div>
              <label class="block text-xs font-medium text-gray-500 mb-1">Abstract</label>
              <textarea v-model="editForm.abstract" rows="3" class="w-full px-3 py-2 border rounded text-sm"></textarea>
            </div>
            <div>
              <label class="block text-xs font-medium text-gray-500 mb-1">Keywords (comma-separated)</label>
              <input v-model="editForm.keywords" class="w-full px-3 py-2 border rounded text-sm" />
            </div>
            <div>
              <label class="block text-xs font-medium text-gray-500 mb-1">Topic Categories (comma-separated)</label>
              <input v-model="editForm.topic_categories" class="w-full px-3 py-2 border rounded text-sm" />
            </div>
            <div>
              <label class="block text-xs font-medium text-gray-500 mb-1">Lineage</label>
              <textarea v-model="editForm.lineage" rows="2" class="w-full px-3 py-2 border rounded text-sm"></textarea>
            </div>
            <div class="flex gap-2">
              <button @click="saveEdit(item.id)" :disabled="saving"
                class="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50">
                {{ saving ? 'Saving...' : 'Save' }}
              </button>
              <button @click="cancelEdit"
                class="px-3 py-1.5 text-sm bg-gray-100 text-gray-700 rounded hover:bg-gray-200">Cancel</button>
            </div>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>
