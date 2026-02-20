<template>
  <div class="max-w-3xl mx-auto px-4 py-12">
    
    <!-- Overview -->
    <section class="mb-10">
      <h1 class="text-2xl font-bold text-emerald-900 mb-4">About</h1>
      <p class="text-gray-600 leading-relaxed mb-4">
        The DSH Environmental Dataset Search Platform is a research-oriented retrieval system 
        for discovering UK environmental datasets. It demonstrates modern search techniques 
        including hybrid retrieval and semantic similarity applied to scientific metadata.
      </p>
      <p class="text-gray-600 leading-relaxed">
        The system combines two retrieval methods: keyword search over text 
        fields and vector similarity search over embeddings. Results from 
        both are merged using Reciprocal Rank Fusion (RRF).
      </p>
    </section>

    <!-- How Search Works -->
    <section class="mb-10">
      <h2 class="text-lg font-semibold text-emerald-900 mb-3">How Search Works</h2>
      <div class="space-y-3 text-sm text-gray-600">
        <p>
          <span class="font-medium text-gray-800">Keyword search</span> — 
          queries the title and abstract fields using MongoDB text search.
        </p>
        <p>
          <span class="font-medium text-gray-800">Semantic search</span> — 
          encodes the query into a 384-dimensional vector and finds similar 
          dataset embeddings using cosine similarity.
        </p>
        <p>
          <span class="font-medium text-gray-800">Rank fusion</span> — 
          both result lists are combined using RRF, which ranks documents 
          by their positions in each list without requiring score normalisation.
        </p>
      </div>
    </section>

    <!-- Metadata Standard -->
    <section class="mb-10">
      <h2 class="text-lg font-semibold text-emerald-900 mb-3">Metadata Standard</h2>
      <p class="text-gray-600 text-sm mb-4">
        Records are validated against ISO 19115, the international standard 
        for geographic information metadata.
      </p>
      <div class="grid sm:grid-cols-2 gap-3 text-sm">
        <div class="bg-green-50 border border-green-100 rounded-lg p-3">
          <p class="font-medium text-green-800 mb-1">Required</p>
          <p class="text-gray-600">Title, Abstract, Identifier</p>
        </div>
        <div class="bg-green-50 border border-green-100 rounded-lg p-3">
          <p class="font-medium text-green-800 mb-1">Recommended</p>
          <p class="text-gray-600">Keywords, Bounding Box, Temporal Extent</p>
        </div>
      </div>
    </section>

    <!-- Technology -->
    <section class="mb-10">
      <h2 class="text-lg font-semibold text-emerald-900 mb-3">Technology</h2>
      <div class="grid sm:grid-cols-2 gap-2 text-sm">
        <div class="px-3 py-2 bg-gray-50 rounded border border-gray-200">
          <span class="text-gray-800">FastAPI</span>
          <span class="text-gray-500 ml-2">Backend API</span>
        </div>
        <div class="px-3 py-2 bg-gray-50 rounded border border-gray-200">
          <span class="text-gray-800">MongoDB Atlas</span>
          <span class="text-gray-500 ml-2">Database + vector search</span>
        </div>
        <div class="px-3 py-2 bg-gray-50 rounded border border-gray-200">
          <span class="text-gray-800">sentence-transformers</span>
          <span class="text-gray-500 ml-2">Embeddings</span>
        </div>
        <div class="px-3 py-2 bg-gray-50 rounded border border-gray-200">
          <span class="text-gray-800">Vue 3</span>
          <span class="text-gray-500 ml-2">Frontend</span>
        </div>
      </div>
    </section>

    <!-- Data Source -->
    <section>
      <h2 class="text-lg font-semibold text-emerald-900 mb-3">Data Source</h2>
      <p class="text-gray-600 text-sm">
        Dataset metadata is harvested from the 
        <a href="https://catalogue.ceh.ac.uk" target="_blank" rel="noopener" 
           class="text-green-700 hover:underline">UKCEH Environmental Data Centre</a> 
        catalogue. The platform currently indexes approximately 200 records 
        covering ecology, hydrology, atmospheric science, and soil science.
      </p>
    </section>

  </div>
</template>

<script setup>
// Static page
</script>