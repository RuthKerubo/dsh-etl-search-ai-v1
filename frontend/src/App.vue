<script setup>
import { RouterLink, RouterView } from 'vue-router'
import { useAuthStore } from './stores/auth.js'

const auth = useAuthStore()
</script>

<template>
  <div class="min-h-screen flex flex-col bg-green-50">
    <nav class="bg-white border-b border-green-100 sticky top-0 z-10">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between h-16 items-center">

          <!-- Logo + nav links -->
          <div class="flex items-center gap-6">
            <RouterLink to="/" class="text-emerald-900 font-bold text-lg shrink-0">
              DSH Search
            </RouterLink>
            <div class="hidden sm:flex items-center gap-5">
              <RouterLink to="/" 
                class="text-sm font-medium text-gray-500 hover:text-emerald-700 transition-colors"
                active-class="text-green-600">
                Search
              </RouterLink>
              <RouterLink v-if="auth.isLoggedIn" to="/chat"
                class="text-sm font-medium text-gray-500 hover:text-emerald-700 transition-colors"
                active-class="text-green-600">
                Chat
              </RouterLink>
              <RouterLink to="/about"
                class="text-sm font-medium text-gray-500 hover:text-emerald-700 transition-colors"
                active-class="text-green-600">
                About
              </RouterLink>
              <RouterLink v-if="auth.isAdmin" to="/admin"
                class="text-sm font-medium text-gray-500 hover:text-emerald-700 transition-colors"
                active-class="text-green-600">
                Admin
              </RouterLink>
            </div>
          </div>

          <!-- Auth section -->
          <div class="flex items-center gap-4">
            <template v-if="auth.isLoggedIn">
              <div class="hidden sm:flex flex-col items-end leading-tight">
                <span class="text-sm text-gray-700 truncate max-w-[180px]">{{ auth.user?.email }}</span>
                <span class="text-xs font-semibold px-1.5 py-0.5 rounded mt-0.5"
                  :class="auth.isAdmin ? 'bg-red-100 text-red-700' : 'bg-green-100 text-green-700'">
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
                class="text-sm font-medium text-green-700 hover:text-green-900 transition-colors">
                Login
              </RouterLink>
              <RouterLink to="/register"
                class="text-sm bg-green-600 text-white px-3 py-1.5 rounded-lg hover:bg-green-700 transition-colors font-medium">
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

    <footer class="bg-white border-t border-green-100 py-4 text-center text-xs text-gray-400">
      DSH Environmental Dataset Search · ISO 19115 Compliant · Powered by UKCEH data
    </footer>
  </div>
</template>