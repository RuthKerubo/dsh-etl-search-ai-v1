import { createRouter, createWebHistory } from 'vue-router'
import Home from './views/Home.vue'
import Login from './views/Login.vue'
import Register from './views/Register.vue'
import SearchResults from './views/SearchResults.vue'
import DatasetDetail from './views/DatasetDetail.vue'
import Admin from './views/Admin.vue'
import AdminUpload from './views/AdminUpload.vue'
import AdminPending from './views/AdminPending.vue'

const routes = [
  { path: '/', component: Home },
  { path: '/login', component: Login },
  { path: '/register', component: Register },
  { path: '/search', component: SearchResults },
  { path: '/datasets/:id', component: DatasetDetail, props: true },
  { path: '/admin', component: Admin, meta: { requiresAdmin: true } },
  { path: '/admin/upload', component: AdminUpload, meta: { requiresAdmin: true } },
  { path: '/admin/pending', component: AdminPending, meta: { requiresAdmin: true } },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

router.beforeEach((to) => {
  if (to.meta.requiresAdmin) {
    const token = localStorage.getItem('token')
    const user = JSON.parse(localStorage.getItem('user') || 'null')
    if (!token || user?.role !== 'admin') {
      return '/login'
    }
  }
})

export default router
