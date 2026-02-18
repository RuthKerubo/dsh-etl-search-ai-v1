<script setup>
const stack = [
  { name: 'FastAPI',               desc: 'Async Python API framework',                     color: 'bg-teal-50   text-teal-800   border-teal-200'   },
  { name: 'MongoDB Atlas',         desc: 'Vector + document database',                     color: 'bg-green-50  text-green-800  border-green-200'  },
  { name: 'sentence-transformers', desc: 'Local embedding model (all-MiniLM-L6-v2)',       color: 'bg-emerald-50 text-emerald-800 border-emerald-200' },
  { name: 'Vue 3',                 desc: 'Reactive frontend framework',                    color: 'bg-lime-50   text-lime-800   border-lime-200'   },
  { name: 'Tailwind CSS',          desc: 'Utility-first styling',                          color: 'bg-cyan-50   text-cyan-800   border-cyan-200'   },
  { name: 'Docker',                desc: 'Containerised deployment',                       color: 'bg-sky-50    text-sky-800    border-sky-200'    },
]

const isoFields = [
  { field: 'title',            req: true,  desc: 'Human-readable dataset name' },
  { field: 'abstract',         req: true,  desc: 'Summary of dataset content and purpose' },
  { field: 'identifier',       req: true,  desc: 'Unique persistent identifier' },
  { field: 'keywords',         req: false, desc: 'Controlled vocabulary terms' },
  { field: 'topic_categories', req: false, desc: 'ISO topic category codes (e.g. biota, inlandWaters)' },
  { field: 'lineage',          req: false, desc: 'Data provenance and quality information' },
  { field: 'bounding_box',     req: false, desc: 'Geographic extent (W/E/S/N coordinates)' },
  { field: 'temporal_extent',  req: false, desc: 'Start and end dates of data coverage' },
]
</script>

<template>
  <div class="max-w-3xl mx-auto px-4 py-12 space-y-12">

    <!-- What is DSH Search -->
    <section>
      <div class="flex items-center gap-3 mb-4">
        <svg class="w-7 h-7 text-green-600 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
            d="M21 3C21 3 11 5 7 12C3 19 3 21 3 21C3 21 13 19 17 12C21 5 21 3 21 3Z" />
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 21L10 14" />
        </svg>
        <h1 class="text-3xl font-bold text-emerald-900">About DSH Environmental Search</h1>
      </div>
      <p class="text-gray-600 leading-relaxed mb-4">
        The DSH Environmental Dataset Search Platform is an AI-powered catalogue for discovering
        UK environmental datasets. It was built as part of a UKCEH Research Software Engineer
        coding exercise and demonstrates modern search techniques applied to real scientific metadata.
      </p>
      <p class="text-gray-600 leading-relaxed">
        The platform ingests metadata from the
        <a href="https://catalogue.ceh.ac.uk" target="_blank" rel="noopener"
          class="text-green-700 hover:text-green-900 underline underline-offset-2">
          UKCEH Environmental Data Centre
        </a>
        catalogue, validates it against the ISO 19115 geographic metadata standard, generates
        semantic embeddings, and exposes a hybrid search API combining vector similarity with
        full-text keyword matching.
      </p>
    </section>

    <!-- ISO 19115 -->
    <section>
      <h2 class="text-xl font-bold text-emerald-900 mb-2">ISO 19115 Metadata Standard</h2>
      <p class="text-gray-600 leading-relaxed mb-5">
        ISO 19115 is the international standard for geographic information metadata. It defines
        the structure, content, and semantics of metadata records for spatial datasets.
        Compliance ensures interoperability across data portals and catalogues.
      </p>
      <div class="overflow-hidden rounded-xl border border-green-100">
        <table class="min-w-full text-sm">
          <thead class="bg-green-50 text-xs text-emerald-700 uppercase tracking-wide">
            <tr>
              <th class="px-4 py-2 text-left font-semibold">Field</th>
              <th class="px-4 py-2 text-left font-semibold">Status</th>
              <th class="px-4 py-2 text-left font-semibold">Description</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-green-50 bg-white">
            <tr v-for="f in isoFields" :key="f.field" class="hover:bg-green-50/50 transition-colors">
              <td class="px-4 py-2 font-mono text-emerald-900">{{ f.field }}</td>
              <td class="px-4 py-2">
                <span v-if="f.req"
                  class="px-2 py-0.5 bg-red-50 text-red-600 text-xs font-medium rounded-full">Required</span>
                <span v-else
                  class="px-2 py-0.5 bg-amber-50 text-amber-600 text-xs font-medium rounded-full">Recommended</span>
              </td>
              <td class="px-4 py-2 text-gray-600">{{ f.desc }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <!-- Technology Stack -->
    <section>
      <h2 class="text-xl font-bold text-emerald-900 mb-4">Technology Stack</h2>
      <div class="grid sm:grid-cols-2 gap-3">
        <div v-for="s in stack" :key="s.name"
          class="flex items-start gap-3 p-4 rounded-xl border" :class="s.color">
          <div class="flex-1 min-w-0">
            <div class="font-semibold text-sm">{{ s.name }}</div>
            <div class="text-xs mt-0.5 opacity-70">{{ s.desc }}</div>
          </div>
        </div>
      </div>
    </section>

    <!-- Data Sources -->
    <section>
      <h2 class="text-xl font-bold text-emerald-900 mb-3">Data Sources</h2>
      <div class="space-y-3">
        <div class="p-4 rounded-xl border border-green-100 bg-white">
          <div class="font-semibold text-emerald-900 text-sm mb-1">UKCEH Environmental Data Centre</div>
          <p class="text-xs text-gray-500 leading-relaxed">
            Primary data source. Metadata for 200+ datasets is fetched from the CEH
            GEMINI-format catalogue via OAI-PMH, covering ecology, hydrology, atmospheric science,
            and soil science.
          </p>
        </div>
        <div class="p-4 rounded-xl border border-green-100 bg-white">
          <div class="font-semibold text-emerald-900 text-sm mb-1">Sample UK Environmental Datasets</div>
          <p class="text-xs text-gray-500 leading-relaxed">
            Curated sample records covering river water quality, land cover, peatland carbon flux,
            butterfly monitoring, air quality, and more — used for development and demonstration.
          </p>
        </div>
      </div>
    </section>

    <!-- How hybrid search works -->
    <section>
      <h2 class="text-xl font-bold text-emerald-900 mb-3">How Hybrid Search Works</h2>
      <div class="space-y-3 text-sm text-gray-600 leading-relaxed">
        <p>
          <span class="font-semibold text-green-700">Semantic search</span> — your query is encoded
          into a 384-dimensional embedding by the
          <span class="font-mono text-xs bg-green-50 text-green-800 px-1 py-0.5 rounded border border-green-100">
            all-MiniLM-L6-v2
          </span>
          model. MongoDB Atlas $vectorSearch finds the nearest dataset embeddings by cosine similarity.
        </p>
        <p>
          <span class="font-semibold text-emerald-900">Keyword search</span> — a standard text index
          over
          <span class="font-mono text-xs bg-green-50 text-green-800 px-1 py-0.5 rounded border border-green-100">title</span>
          and
          <span class="font-mono text-xs bg-green-50 text-green-800 px-1 py-0.5 rounded border border-green-100">abstract</span>
          is queried in parallel.
        </p>
        <p>
          <span class="font-semibold text-green-700">Reciprocal Rank Fusion (RRF)</span> — both ranked
          lists are merged using RRF, which combines rankings without requiring comparable score scales.
          This consistently outperforms single-mode retrieval.
        </p>
      </div>
    </section>

  </div>
</template>
