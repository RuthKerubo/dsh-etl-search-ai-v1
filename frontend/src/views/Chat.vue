<script setup>
import { ref, nextTick, onMounted, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth.js'
import api from '../api.js'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()

const messages = ref([])
const input = ref('')
const loading = ref(false)
const messagesEnd = ref(null)

onMounted(() => {
  if (!auth.isLoggedIn) {
    router.push({ path: '/login', query: { redirect: '/chat' } })
    return
  }
  const prefill = route.query.q
  if (prefill) {
    input.value = String(prefill)
  }
})

const canSend = computed(() => input.value.trim().length >= 3 && !loading.value)

async function send() {
  const q = input.value.trim()
  if (!canSend.value) return

  messages.value.push({ role: 'user', content: q })
  input.value = ''
  loading.value = true
  await scrollToBottom()

  try {
    const { data } = await api.post('/rag', { question: q, use_llm: false, top_k: 5 })
    messages.value.push({
      role: 'assistant',
      content: data.answer,
      sources: data.sources ?? [],
      generated: data.generated,
    })
  } catch (e) {
    const detail = e.response?.data?.detail
    messages.value.push({
      role: 'assistant',
      content: detail
        ? `Error: ${detail}`
        : 'Sorry, I could not retrieve an answer. Please check you are logged in and try again.',
      sources: [],
      generated: false,
      isError: true,
    })
  } finally {
    loading.value = false
    await scrollToBottom()
  }
}

async function scrollToBottom() {
  await nextTick()
  messagesEnd.value?.scrollIntoView({ behavior: 'smooth' })
}

function handleKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault()
    send()
  }
}

function scoreColor(score) {
  if (score >= 0.8) return 'bg-green-100 text-green-700'
  if (score >= 0.5) return 'bg-amber-100 text-amber-700'
  return 'bg-gray-100 text-gray-500'
}
</script>

<template>
  <div class="flex flex-col h-[calc(100vh-4rem)]">

    <!-- Header -->
    <div class="bg-white border-b border-gray-200 px-4 py-3">
      <div class="max-w-3xl mx-auto flex items-center gap-3">
        <div class="w-8 h-8 rounded-full bg-indigo-100 flex items-center justify-center shrink-0">
          <svg class="w-4 h-4 text-indigo-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z" />
          </svg>
        </div>
        <div>
          <h1 class="text-sm font-semibold text-gray-900">Dataset Q&amp;A</h1>
          <p class="text-xs text-gray-400">Ask questions about UK environmental datasets</p>
        </div>
      </div>
    </div>

    <!-- Messages -->
    <div class="flex-1 overflow-y-auto px-4 py-6">
      <div class="max-w-3xl mx-auto space-y-4">

        <!-- Empty state -->
        <div v-if="messages.length === 0" class="flex flex-col items-center justify-center h-48 text-center gap-3">
          <div class="w-12 h-12 rounded-full bg-indigo-50 flex items-center justify-center">
            <svg class="w-6 h-6 text-indigo-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5"
                d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
          </div>
          <p class="text-sm text-gray-500 max-w-sm">
            Ask questions about datasets — topics, methods, coverage, availability.
            Answers are sourced directly from the dataset catalogue.
          </p>
          <div class="flex flex-wrap justify-center gap-2 mt-1">
            <button v-for="ex in [
              'What datasets cover river water quality?',
              'Which datasets monitor soil carbon?',
              'Tell me about butterfly population trends',
            ]" :key="ex"
              @click="input = ex"
              class="text-xs px-3 py-1.5 bg-indigo-50 text-indigo-600 rounded-full hover:bg-indigo-100 transition-colors">
              {{ ex }}
            </button>
          </div>
        </div>

        <!-- Message bubbles -->
        <div v-for="(msg, i) in messages" :key="i">

          <!-- User -->
          <div v-if="msg.role === 'user'" class="flex justify-end">
            <div class="max-w-[75%] bg-indigo-600 text-white text-sm rounded-2xl rounded-tr-sm px-4 py-2.5 leading-relaxed">
              {{ msg.content }}
            </div>
          </div>

          <!-- Assistant -->
          <div v-else class="flex items-start gap-3">
            <div class="w-7 h-7 rounded-full bg-indigo-100 flex items-center justify-center shrink-0 mt-0.5">
              <svg class="w-3.5 h-3.5 text-indigo-600" fill="currentColor" viewBox="0 0 20 20">
                <path d="M2 5a2 2 0 012-2h7a2 2 0 012 2v4a2 2 0 01-2 2H9l-3 3v-3H4a2 2 0 01-2-2V5z" />
                <path d="M15 7v2a4 4 0 01-4 4H9.828l-1.766 1.767c.28.149.599.233.938.233h2l3 3v-3h2a2 2 0 002-2V9a2 2 0 00-2-2h-1z" />
              </svg>
            </div>
            <div class="flex-1 min-w-0">
              <div class="text-sm text-gray-800 leading-relaxed whitespace-pre-line"
                :class="msg.isError ? 'text-red-600' : ''">
                {{ msg.content }}
              </div>

              <!-- Sources -->
              <div v-if="msg.sources?.length" class="mt-3 space-y-1.5">
                <p class="text-xs font-semibold text-gray-400 uppercase tracking-wide">Sources</p>
                <RouterLink v-for="src in msg.sources" :key="src.id"
                  :to="`/datasets/${src.id}`"
                  class="flex items-center justify-between gap-3 p-2 rounded-lg border border-gray-100 bg-gray-50 hover:bg-indigo-50 hover:border-indigo-100 transition-colors group">
                  <span class="text-xs text-gray-700 group-hover:text-indigo-700 truncate">{{ src.title }}</span>
                  <span class="shrink-0 text-xs px-1.5 py-0.5 rounded font-medium" :class="scoreColor(src.relevance_score)">
                    {{ (src.relevance_score * 100).toFixed(0) }}%
                  </span>
                </RouterLink>
              </div>
            </div>
          </div>
        </div>

        <!-- Typing indicator -->
        <div v-if="loading" class="flex items-start gap-3">
          <div class="w-7 h-7 rounded-full bg-indigo-100 flex items-center justify-center shrink-0">
            <svg class="w-3.5 h-3.5 text-indigo-600" fill="currentColor" viewBox="0 0 20 20">
              <path d="M2 5a2 2 0 012-2h7a2 2 0 012 2v4a2 2 0 01-2 2H9l-3 3v-3H4a2 2 0 01-2-2V5z" />
              <path d="M15 7v2a4 4 0 01-4 4H9.828l-1.766 1.767c.28.149.599.233.938.233h2l3 3v-3h2a2 2 0 002-2V9a2 2 0 00-2-2h-1z" />
            </svg>
          </div>
          <div class="flex gap-1 mt-2">
            <span class="w-2 h-2 bg-indigo-300 rounded-full animate-bounce" style="animation-delay:0ms"></span>
            <span class="w-2 h-2 bg-indigo-300 rounded-full animate-bounce" style="animation-delay:150ms"></span>
            <span class="w-2 h-2 bg-indigo-300 rounded-full animate-bounce" style="animation-delay:300ms"></span>
          </div>
        </div>

        <div ref="messagesEnd" />
      </div>
    </div>

    <!-- Input bar -->
    <div class="bg-white border-t border-gray-200 px-4 py-3">
      <div class="max-w-3xl mx-auto flex gap-3 items-end">
        <textarea v-model="input" @keydown="handleKey" rows="1"
          placeholder="Ask about datasets..."
          class="flex-1 resize-none px-4 py-2.5 text-sm border border-gray-200 rounded-xl focus:outline-none focus:ring-2 focus:ring-indigo-300 focus:border-transparent leading-relaxed" />
        <button @click="send" :disabled="!canSend"
          class="shrink-0 w-10 h-10 flex items-center justify-center rounded-xl transition-colors"
          :class="canSend
            ? 'bg-indigo-600 text-white hover:bg-indigo-700'
            : 'bg-gray-100 text-gray-300 cursor-not-allowed'">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8" />
          </svg>
        </button>
      </div>
      <p class="text-center text-xs text-gray-300 mt-2">Enter to send &middot; Shift+Enter for new line</p>
    </div>
  </div>
</template>
