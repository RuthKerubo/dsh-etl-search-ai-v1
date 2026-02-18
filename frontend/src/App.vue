<script setup>
import { RouterLink, RouterView } from 'vue-router'
import { useAuthStore } from './stores/auth.js'

const auth = useAuthStore()
</script>

<template>
  <div class="min-h-screen flex flex-col bg-slate-50">
    <nav class="bg-white border-b border-gray-200 sticky top-0 z-10">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between h-16 items-center">

          <!-- Logo + nav links -->
          <div class="flex items-center gap-6">
            <RouterLink to="/" class="flex items-center gap-2 text-indigo-600 font-bold text-lg shrink-0">
              <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                  d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              DSH Search
            </RouterLink>
            <div class="hidden sm:flex items-center gap-5">
              <RouterLink to="/" class="text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors"
                active-class="text-indigo-600">Search</RouterLink>
              <RouterLink v-if="auth.isLoggedIn" to="/chat"
                class="text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors"
                active-class="text-indigo-600">Chat</RouterLink>
              <RouterLink to="/about" class="text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors"
                active-class="text-indigo-600">About</RouterLink>
              <RouterLink v-if="auth.isAdmin" to="/admin"
                class="text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors"
                active-class="text-indigo-600">Admin</RouterLink>
            </div>
          </div>

          <!-- Auth -->
          <div class="flex items-center gap-4">
            <template v-if="auth.isLoggedIn">
              <div class="hidden sm:flex flex-col items-end leading-tight">
                <span class="text-sm text-gray-700 truncate max-w-[180px]">{{ auth.user?.email }}</span>
                <span class="text-xs font-semibold px-1.5 py-0.5 rounded mt-0.5"
                  :class="auth.isAdmin
                    ? 'bg-red-100 text-red-700'
                    : 'bg-blue-100 text-blue-700'">
                  {{ auth.isAdmin ? 'Admin' : 'Researcher' }}
                </span>
              </div>
              <button @click="auth.logout(); $router.push('/')"
                class="text-sm text-gray-400 hover:text-red-600 transition-colors">
                Logout
              </button>
            </template>
            <template v-else>
              <RouterLink to="/login"
                class="text-sm font-medium text-indigo-600 hover:text-indigo-800 transition-colors">
                Login
              </RouterLink>
              <RouterLink to="/register"
                class="text-sm bg-indigo-600 text-white px-3 py-1.5 rounded-lg hover:bg-indigo-700 transition-colors font-medium">
                Register
              </RouterLink>
            </template>
          </div>
        </div>
      </div>
    </nav>

    <main class="flex-1">
      <RouterView />
    </main>

    <footer class="bg-white border-t border-gray-100 py-4 text-center text-xs text-gray-400">
      DSH Environmental Dataset Search &middot; ISO 19115 Compliant &middot; Powered by UKCEH data
    </footer>
  </div>
</template>
