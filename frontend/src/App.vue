<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()
const searchQuery = ref('')

function handleSearch() {
  if (searchQuery.value.trim()) {
    router.push({ path: '/search', query: { q: searchQuery.value.trim() } })
  }
}
</script>

<template>
  <div class="max-w-4xl mx-auto px-4 py-16">
    
    <!-- Header -->
    <div class="text-center mb-12">
      <h1 class="text-3xl font-bold text-emerald-900 mb-4">
        Environmental Dataset Search
      </h1>
      <p class="text-gray-600 max-w-2xl mx-auto">
        Search 200+ UK environmental datasets from the UKCEH Environmental Data Centre. 
        The platform combines keyword matching with semantic similarity to find relevant records.
      </p>
    </div>

    <!-- Search Box -->
    <div class="max-w-xl mx-auto mb-16">
      <form @submit.prevent="handleSearch" class="flex gap-2">
        <input
          v-model="searchQuery"
          type="text"
          placeholder="Search datasets..."
          class="flex-1 px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-green-500 focus:border-transparent"
        />
        <button
          type="submit"
          class="px-6 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors font-medium"
        >
          Search
        </button>
      </form>
      
      <!-- Example queries -->
      <div class="mt-4 text-sm text-gray-500">
        <span>Try: </span>
        <button @click="searchQuery = 'climate change'; handleSearch()" class="text-green-700 hover:underline">climate change</button>,
        <button @click="searchQuery = 'river water quality'; handleSearch()" class="text-green-700 hover:underline ml-1">river water quality</button>,
        <button @click="searchQuery = 'soil carbon'; handleSearch()" class="text-green-700 hover:underline ml-1">soil carbon</button>
      </div>
    </div>

    <!-- Brief Description -->
    <div class="grid md:grid-cols-3 gap-6 text-sm">
      <div class="bg-white p-5 rounded-lg border border-gray-200">
        <h3 class="font-semibold text-emerald-900 mb-2">Hybrid Search</h3>
        <p class="text-gray-600">
          Combines keyword matching with vector similarity search. 
          Results are merged using Reciprocal Rank Fusion.
        </p>
      </div>
      
      <div class="bg-white p-5 rounded-lg border border-gray-200">
        <h3 class="font-semibold text-emerald-900 mb-2">ISO 19115 Metadata</h3>
        <p class="text-gray-600">
          Dataset records follow the ISO 19115 geographic metadata standard 
          for interoperability with other catalogues.
        </p>
      </div>
      
      <div class="bg-white p-5 rounded-lg border border-gray-200">
        <h3 class="font-semibold text-emerald-900 mb-2">UKCEH Data</h3>
        <p class="text-gray-600">
          Metadata sourced from the UK Centre for Ecology & Hydrology 
          Environmental Data Centre catalogue.
        </p>
      </div>
    </div>

  </div>
</template>