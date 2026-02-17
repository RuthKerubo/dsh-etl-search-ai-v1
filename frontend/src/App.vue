<script setup>
import { RouterLink, RouterView } from 'vue-router'
import { useAuthStore } from './stores/auth.js'

const auth = useAuthStore()
</script>

<template>
  <div class="min-h-screen flex flex-col">
    <nav class="bg-white shadow">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between h-16 items-center">
          <div class="flex items-center space-x-8">
            <RouterLink to="/" class="text-xl font-bold text-indigo-600">DSH Search</RouterLink>
            <RouterLink to="/" class="text-gray-600 hover:text-gray-900">Search</RouterLink>
            <RouterLink to="/datasets" class="text-gray-600 hover:text-gray-900"
              v-if="false">Browse</RouterLink>
            <RouterLink to="/admin" class="text-gray-600 hover:text-gray-900"
              v-if="auth.isAdmin">Admin</RouterLink>
          </div>
          <div class="flex items-center space-x-4">
            <template v-if="auth.isLoggedIn">
              <span class="text-sm text-gray-500">{{ auth.user?.email }}</span>
              <button @click="auth.logout(); $router.push('/')"
                class="text-sm text-red-600 hover:text-red-800">Logout</button>
            </template>
            <template v-else>
              <RouterLink to="/login" class="text-sm text-indigo-600 hover:text-indigo-800">Login</RouterLink>
              <RouterLink to="/register"
                class="text-sm bg-indigo-600 text-white px-3 py-1.5 rounded hover:bg-indigo-700">Register</RouterLink>
            </template>
          </div>
        </div>
      </div>
    </nav>
    <main class="flex-1">
      <RouterView />
    </main>
  </div>
</template>
