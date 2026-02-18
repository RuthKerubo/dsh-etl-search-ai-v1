<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../stores/auth.js'

const auth = useAuthStore()
const router = useRouter()

const email = ref('')
const password = ref('')
const role = ref('researcher')
const error = ref('')
const loading = ref(false)

async function submit() {
  error.value = ''
  loading.value = true
  try {
    await auth.register(email.value, password.value, role.value)
    router.push('/')
  } catch (e) {
    error.value = e.response?.data?.detail || 'Registration failed'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <div class="flex items-center justify-center min-h-[calc(100vh-4rem)] bg-green-50">
    <div class="w-full max-w-md p-8 bg-white rounded-xl shadow-sm border border-green-100">

      <!-- Logo mark -->
      <div class="flex justify-center mb-6">
        <div class="w-12 h-12 rounded-full bg-green-100 flex items-center justify-center">
          <svg class="w-6 h-6 text-green-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
              d="M21 3C21 3 11 5 7 12C3 19 3 21 3 21C3 21 13 19 17 12C21 5 21 3 21 3Z" />
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 21L10 14" />
          </svg>
        </div>
      </div>

      <h1 class="text-2xl font-bold text-emerald-900 text-center mb-1">Create account</h1>
      <p class="text-sm text-gray-500 text-center mb-6">Join to access Dataset Q&amp;A and search features</p>

      <div v-if="error" class="mb-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm border border-red-100">
        {{ error }}
      </div>

      <form @submit.prevent="submit" class="space-y-4">
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Email</label>
          <input v-model="email" type="email" required
            class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-green-300 focus:border-green-400 focus:outline-none transition-colors" />
        </div>
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Password</label>
          <input v-model="password" type="password" required minlength="8"
            class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-green-300 focus:border-green-400 focus:outline-none transition-colors" />
          <p class="text-xs text-gray-400 mt-1">Minimum 8 characters</p>
        </div>
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Role</label>
          <select v-model="role"
            class="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:ring-2 focus:ring-green-300 focus:border-green-400 focus:outline-none transition-colors bg-white">
            <option value="researcher">Researcher</option>
            <option value="admin">Admin</option>
          </select>
        </div>
        <button type="submit" :disabled="loading"
          class="w-full bg-green-600 text-white py-2.5 rounded-lg font-medium hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
          {{ loading ? 'Creating account...' : 'Create account' }}
        </button>
      </form>

      <p class="mt-5 text-center text-sm text-gray-500">
        Already have an account?
        <RouterLink to="/login" class="text-green-700 hover:text-green-900 font-medium hover:underline">
          Sign in
        </RouterLink>
      </p>
    </div>
  </div>
</template>
