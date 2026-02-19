# DSH Environmental Dataset Search

Search platform for UK environmental datasets from the UKCEH Environmental Data Centre.

**Live demo:** https://dsh-etl-search.duckdns.org

---

## What it does

- Indexes 200+ environmental dataset metadata records
- Hybrid search combining keyword and semantic similarity
- Reciprocal Rank Fusion for result ranking
- RAG interface for question answering
- ISO 19115 metadata validation

---

## Technology

- FastAPI (Python 3.10)
- MongoDB Atlas with vector search
- sentence-transformers (all-MiniLM-L6-v2)
- Vue 3 + Tailwind CSS
- Docker

---

## Quick Start
```bash
git clone https://github.com/RuthKerubo/dsh-etl-search-ai-v1.git
cd dsh-etl-search-ai-v1

cp .env.example .env
# Configure MONGODB_URI and JWT_SECRET

docker-compose up --build
```

API docs at http://localhost:8000/docs

---

## Project Structure
```
api/           # FastAPI application
etl/           # Data ingestion pipeline
frontend/      # Vue 3 application
tests/         # Test suite
```

---

## Documentation

- [Architecture](ARCHITECTURE.md)
- [Instructions](INSTRUCTIONS.md)

---

## Data Source

Metadata from [UKCEH Environmental Data Centre](https://catalogue.ceh.ac.uk)