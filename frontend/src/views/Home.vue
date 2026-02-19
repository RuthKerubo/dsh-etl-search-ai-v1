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
</script>

<template>
  <!-- Hero -->
  <section class="bg-gradient-to-br from-green-700 to-emerald-700 text-white">
    <div class="max-w-4xl mx-auto px-4 py-16 text-center">
      <h1 class="text-3xl sm:text-4xl font-bold mb-4">
        Environmental Dataset Search
      </h1>
      <p class="text-green-100 max-w-2xl mx-auto mb-8">
        Search 200+ UK environmental datasets from the UKCEH catalogue. 
        Hybrid retrieval combines keyword matching with semantic similarity.
      </p>

      <form @submit.prevent="search" class="flex max-w-xl mx-auto shadow-lg rounded-lg overflow-hidden">
        <input v-model="query" type="text"
          placeholder="Search datasets..."
          class="flex-1 px-4 py-3 text-gray-900 focus:outline-none" autofocus />
        <button type="submit"
          class="bg-green-600 text-white px-6 py-3 font-medium hover:bg-green-500 transition-colors">
          Search
        </button>
      </form>

      <div class="mt-4 flex flex-wrap justify-center gap-2 text-sm">
        <span class="text-green-200">Try:</span>
        <button v-for="ex in ['climate change', 'river water quality', 'soil carbon', 'land cover']"
          :key="ex"
          @click="query = ex; search()"
          class="text-green-100 underline underline-offset-2 hover:text-white">
          {{ ex }}
        </button>
      </div>
    </div>
  </section>

  <!-- Stats -->
  <section class="bg-white border-b border-gray-200">
    <div class="max-w-4xl mx-auto px-4 py-6 grid grid-cols-4 gap-4 text-center text-sm">
      <div>
        <div class="text-2xl font-bold text-green-700">200+</div>
        <div class="text-gray-500">Datasets</div>
      </div>
      <div>
        <div class="text-2xl font-bold text-green-700">3</div>
        <div class="text-gray-500">Sources</div>
      </div>
      <div>
        <div class="text-2xl font-bold text-green-700">Hybrid</div>
        <div class="text-gray-500">Search</div>
      </div>
      <div>
        <div class="text-2xl font-bold text-green-700">ISO 19115</div>
        <div class="text-gray-500">Compliant</div>
      </div>
    </div>
  </section>

  <!-- Features -->
  <section class="max-w-4xl mx-auto px-4 py-12">
    <div class="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="font-semibold text-emerald-900 text-sm mb-2">Hybrid Search</h3>
        <p class="text-xs text-gray-600 leading-relaxed">
          Combines keyword matching with vector similarity. Results merged using Reciprocal Rank Fusion.
        </p>
      </div>
      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="font-semibold text-emerald-900 text-sm mb-2">RAG Chat</h3>
        <p class="text-xs text-gray-600 leading-relaxed">
          Ask questions in natural language. Answers are grounded in dataset metadata with citations.
        </p>
      </div>
      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="font-semibold text-emerald-900 text-sm mb-2">ISO 19115</h3>
        <p class="text-xs text-gray-600 leading-relaxed">
          Metadata validated against the geographic information standard. Compliance score shown per dataset.
        </p>
      </div>
      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="font-semibold text-emerald-900 text-sm mb-2">Document Upload</h3>
        <p class="text-xs text-gray-600 leading-relaxed">
          Admins can upload PDFs and structured files. Embeddings generated automatically.
        </p>
      </div>
    </div>
  </section>

  <!-- How it works -->
  <section class="bg-gray-50 border-t border-gray-200">
    <div class="max-w-3xl mx-auto px-4 py-12">
      <h2 class="text-lg font-semibold text-emerald-900 text-center mb-8">How It Works</h2>
      <div class="grid sm:grid-cols-3 gap-6 text-center text-sm">
        <div>
          <div class="w-8 h-8 rounded-full bg-green-600 text-white font-bold flex items-center justify-center mx-auto mb-3">1</div>
          <h3 class="font-medium text-gray-900 mb-1">Enter query</h3>
          <p class="text-gray-500">Type a topic, location, or keyword</p>
        </div>
        <div>
          <div class="w-8 h-8 rounded-full bg-green-600 text-white font-bold flex items-center justify-center mx-auto mb-3">2</div>
          <h3 class="font-medium text-gray-900 mb-1">Hybrid ranking</h3>
          <p class="text-gray-500">Vector and keyword results combined via RRF</p>
        </div>
        <div>
          <div class="w-8 h-8 rounded-full bg-green-600 text-white font-bold flex items-center justify-center mx-auto mb-3">3</div>
          <h3 class="font-medium text-gray-900 mb-1">Explore results</h3>
          <p class="text-gray-500">View metadata or ask follow-up questions</p>
        </div>
      </div>
    </div>
  </section>
</template>