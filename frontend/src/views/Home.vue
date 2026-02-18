<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()
const query = ref('')

function search() {
  if (query.value.trim()) {
    router.push({ path: '/search', query: { q: query.value.trim() } })
  }
}

const features = [
  {
    title: 'Hybrid Search',
    description: 'Combines semantic vector similarity with keyword matching using Reciprocal Rank Fusion for best-in-class relevance.',
    icon: 'M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z',
    color: 'green',
  },
  {
    title: 'RAG Q&A',
    description: 'Ask natural language questions about datasets. Retrieval Augmented Generation surfaces relevant context from the catalogue.',
    icon: 'M8 10h.01M12 10h.01M16 10h.01M9 16H5a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v8a2 2 0 01-2 2h-5l-5 5v-5z',
    color: 'emerald',
  },
  {
    title: 'ISO 19115 Compliance',
    description: 'Every dataset is automatically scored against the ISO 19115 geographic metadata standard with field-level reporting.',
    icon: 'M9 12l2 2 4-4M7.835 4.697a3.42 3.42 0 001.946-.806 3.42 3.42 0 014.438 0 3.42 3.42 0 001.946.806 3.42 3.42 0 013.138 3.138 3.42 3.42 0 00.806 1.946 3.42 3.42 0 010 4.438 3.42 3.42 0 00-.806 1.946 3.42 3.42 0 01-3.138 3.138 3.42 3.42 0 00-1.946.806 3.42 3.42 0 01-4.438 0 3.42 3.42 0 00-1.946-.806 3.42 3.42 0 01-3.138-3.138 3.42 3.42 0 00-.806-1.946 3.42 3.42 0 010-4.438 3.42 3.42 0 00.806-1.946 3.42 3.42 0 013.138-3.138z',
    color: 'teal',
  },
  {
    title: 'Document Upload',
    description: 'Admins can ingest PDFs, CSV, and JSON files directly. Embeddings are generated automatically on upload.',
    icon: 'M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12',
    color: 'lime',
  },
]

const colorMap = {
  green:   { bg: 'bg-green-50',   icon: 'text-green-600',   border: 'border-green-100'   },
  emerald: { bg: 'bg-emerald-50', icon: 'text-emerald-600', border: 'border-emerald-100' },
  teal:    { bg: 'bg-teal-50',    icon: 'text-teal-600',    border: 'border-teal-100'    },
  lime:    { bg: 'bg-lime-50',    icon: 'text-lime-600',    border: 'border-lime-100'    },
}

const steps = [
  { n: '1', title: 'Enter your query', body: 'Type a topic, species, location, or any environmental keyword into the search box.' },
  { n: '2', title: 'Hybrid ranking', body: 'Your query is embedded into a vector and matched semantically alongside keyword scoring — results are fused via RRF.' },
  { n: '3', title: 'Explore results', body: 'Browse ISO 19115-compliant dataset cards, drill into metadata, or ask follow-up questions via the Chat interface.' },
]
</script>

<template>
  <!-- Hero -->
  <section class="bg-gradient-to-br from-green-800 via-green-700 to-emerald-700 text-white">
    <div class="max-w-4xl mx-auto px-4 py-20 text-center">
      <span class="inline-flex items-center gap-2 mb-5 px-3 py-1 bg-white/20 rounded-full text-sm font-medium tracking-wide">
        <!-- Leaf icon -->
        <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
            d="M21 3C21 3 11 5 7 12C3 19 3 21 3 21C3 21 13 19 17 12C21 5 21 3 21 3Z" />
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 21L10 14" />
        </svg>
        AI-Powered Environmental Data Discovery
      </span>

      <h1 class="text-4xl sm:text-5xl font-bold mb-4 leading-tight">
        DSH Environmental<br>Dataset Search
      </h1>
      <p class="text-green-100 text-lg max-w-2xl mx-auto mb-10">
        Discover 200+ UK environmental datasets using AI-powered semantic search.
        ISO 19115 compliant metadata with hybrid vector + keyword search.
      </p>

      <form @submit.prevent="search" class="flex max-w-2xl mx-auto shadow-xl rounded-xl overflow-hidden">
        <input v-model="query" type="text"
          placeholder="e.g. river water quality, soil carbon, pollinator abundance..."
          class="flex-1 px-5 py-4 text-gray-900 text-base focus:outline-none" autofocus />
        <button type="submit"
          class="bg-green-500 text-white px-6 py-4 font-semibold hover:bg-green-600 transition-colors shrink-0">
          Search
        </button>
      </form>

      <!-- Quick searches -->
      <div class="mt-4 flex flex-wrap justify-center gap-2">
        <span class="text-green-200 text-sm">Try:</span>
        <button v-for="ex in ['climate projections', 'pollinator abundance', 'peatland carbon flux', 'groundwater levels']"
          :key="ex"
          @click="query = ex; search()"
          class="text-sm text-green-100 underline underline-offset-2 hover:text-white transition-colors">
          {{ ex }}
        </button>
      </div>
    </div>
  </section>

  <!-- Stats -->
  <section class="bg-white border-b border-green-100">
    <div class="max-w-4xl mx-auto px-4 py-8 grid grid-cols-2 sm:grid-cols-4 gap-6 text-center">
      <div>
        <div class="text-3xl font-bold text-green-600">200+</div>
        <div class="text-sm text-gray-500 mt-1">Datasets</div>
      </div>
      <div>
        <div class="text-3xl font-bold text-green-600">3</div>
        <div class="text-sm text-gray-500 mt-1">Data Sources</div>
      </div>
      <div>
        <div class="text-3xl font-bold text-green-600">2</div>
        <div class="text-sm text-gray-500 mt-1">Search Modes</div>
      </div>
      <div>
        <div class="text-3xl font-bold text-green-600">384</div>
        <div class="text-sm text-gray-500 mt-1">Vector Dimensions</div>
      </div>
    </div>
  </section>

  <!-- Feature cards -->
  <section class="max-w-5xl mx-auto px-4 py-16">
    <h2 class="text-2xl font-bold text-emerald-900 text-center mb-10">Platform Features</h2>
    <div class="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
      <div v-for="f in features" :key="f.title"
        class="bg-white rounded-xl border p-5 flex flex-col gap-3 shadow-sm hover:shadow-md transition-shadow"
        :class="colorMap[f.color].border">
        <div class="w-10 h-10 rounded-lg flex items-center justify-center" :class="colorMap[f.color].bg">
          <svg class="w-5 h-5" :class="colorMap[f.color].icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" :d="f.icon" />
          </svg>
        </div>
        <div>
          <h3 class="font-semibold text-emerald-900 text-sm mb-1">{{ f.title }}</h3>
          <p class="text-xs text-gray-500 leading-relaxed">{{ f.description }}</p>
        </div>
      </div>
    </div>
  </section>

  <!-- How it works -->
  <section class="bg-white border-t border-green-100">
    <div class="max-w-4xl mx-auto px-4 py-16">
      <h2 class="text-2xl font-bold text-emerald-900 text-center mb-12">How It Works</h2>
      <div class="grid sm:grid-cols-3 gap-8">
        <div v-for="s in steps" :key="s.n" class="text-center">
          <div
            class="w-10 h-10 rounded-full bg-green-600 text-white font-bold text-lg flex items-center justify-center mx-auto mb-4">
            {{ s.n }}
          </div>
          <h3 class="font-semibold text-emerald-900 mb-2">{{ s.title }}</h3>
          <p class="text-sm text-gray-500 leading-relaxed">{{ s.body }}</p>
        </div>
      </div>
    </div>
  </section>
</template>
