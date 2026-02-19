# Architecture

## Overview

The platform has four main components:

- **Frontend** — Vue 3 single-page application
- **Backend** — FastAPI REST API
- **Database** — MongoDB Atlas with vector search
- **ETL** — Standalone ingestion pipeline

These are loosely coupled. The ETL pipeline populates the database independently of the API.

---

## Search

The search system combines two retrieval methods:

1. **Keyword search** — MongoDB text index on title and abstract
2. **Vector search** — Cosine similarity over 384-dimensional embeddings

Results from both methods are merged using Reciprocal Rank Fusion (RRF).

### Reciprocal Rank Fusion

RRF combines ranked lists without requiring score normalisation:
```
RRF_score(d) = Σ 1/(k + rank_i(d))
```

Where `k` is a smoothing constant (default 60) and `rank_i(d)` is the rank of document `d` in retrieval method `i`.

This approach is robust when one method underperforms and avoids issues with incompatible score scales.

---

## Embeddings

Embeddings are generated using `sentence-transformers/all-MiniLM-L6-v2`:

- 384 dimensions
- Generated locally (no external API)
- Stored in MongoDB alongside metadata
- Indexed using MongoDB Atlas `$vectorSearch`

---

## RAG Pipeline

The retrieval-augmented generation system:

1. Retrieves top-k datasets via hybrid search
2. Builds context from dataset metadata
3. Passes context to language model (Ollama) or returns structured summary
4. Returns answer with source citations

Context is filtered by user access level before generation.

---

## Access Control

- JWT authentication with 7-day expiry
- Two roles: `admin` and `researcher`
- Datasets have access levels: `public`, `restricted`, `admin_only`
- Search results are filtered based on user role
- RAG context excludes datasets the user cannot access

---

## ETL Pipeline

The ETL module:

- Fetches metadata from UKCEH catalogue
- Parses JSON and XML formats
- Validates against ISO 19115 schema
- Generates embeddings
- Stores records in MongoDB

It runs independently via CLI and can be re-executed for updates.

---

## Deployment

- Docker Compose for local development
- Nginx reverse proxy in production
- HTTPS via Let's Encrypt
- Hosted on Linode