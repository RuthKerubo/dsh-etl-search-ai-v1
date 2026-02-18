# DSH Environmental Dataset Search

Live Demo: https://dsh-etl-search.duckdns.org

---

## Overview

DSH Environmental Dataset Search is a research-oriented platform for discovering and exploring UK environmental datasets sourced from the UK Centre for Ecology & Hydrology (UKCEH) Environmental Data Centre.

The system supports structured dataset discovery through hybrid retrieval (lexical + dense vector search) and provides a retrieval-augmented generation (RAG) interface for grounded question answering over dataset metadata.

The platform is designed as a reproducible research software system rather than a product-oriented application.

---

## Functional Components

- Hybrid lexical + semantic retrieval
- Reciprocal Rank Fusion (RRF) ranking aggregation
- Retrieval-Augmented Generation (RAG) with citations
- ISO 19115 core metadata validation
- ETL pipeline for automated dataset ingestion
- Role-based access control (Admin / Researcher)
- Admin document upload and metadata extraction

---

## System Summary

High-level workflow:

1. Datasets are ingested via ETL pipeline.
2. Metadata is stored in MongoDB.
3. Embeddings are generated locally and stored alongside metadata.
4. Queries trigger:
   - Lexical search
   - Vector similarity search
   - Reciprocal Rank Fusion (RRF)
5. Results returned via REST API.
6. Optional RAG pipeline generates grounded responses.

Detailed architectural decisions are documented in `ARCHITECTURE.md`.

---

## Technology Stack (Summary)

- FastAPI (Python 3.10)
- MongoDB Atlas with `$vectorSearch`
- Sentence Transformers (`all-MiniLM-L6-v2`)
- Vue 3 frontend
- Docker-based deployment

See `STACK.md` for full justification.

---

## Quick Start

### Backend

```bash
git clone https://github.com/RuthKerubo/dsh-etl-search-ai-v1.git
cd dsh-etl-search-ai-v1

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Configure MONGODB_URI and JWT_SECRET

uvicorn api.main:app --reload
