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
  <div class="flex items-center justify-center min-h-[calc(100vh-4rem)]">
    <div class="w-full max-w-2xl px-4 text-center">
      <h1 class="text-4xl font-bold text-gray-900 mb-2">Dataset Search</h1>
      <p class="text-lg text-gray-500 mb-8">Search environmental datasets using hybrid semantic + keyword search</p>

      <form @submit.prevent="search" class="flex shadow-lg rounded-lg overflow-hidden">
        <input v-model="query" type="text" placeholder="Search datasets..."
          class="flex-1 px-6 py-4 text-lg border-0 focus:ring-0 focus:outline-none" autofocus />
        <button type="submit"
          class="bg-indigo-600 text-white px-8 py-4 text-lg font-medium hover:bg-indigo-700">
          Search
        </button>
      </form>
    </div>
  </div>
</template>
