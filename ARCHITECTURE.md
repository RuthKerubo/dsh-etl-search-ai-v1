# Architecture

## System Overview

The platform follows a modular architecture:

- Vue 3 frontend (client interface)
- FastAPI backend (API + retrieval logic)
- MongoDB Atlas (metadata + vector storage)
- ETL pipeline (standalone ingestion module)

The system separates ingestion, retrieval, and presentation layers.

---

## Retrieval Architecture

The platform implements a hybrid retrieval approach combining:

1. Lexical keyword search
2. Dense vector similarity search

Each method retrieves and ranks results independently.

These rankings are then combined using Reciprocal Rank Fusion (RRF).

---

## Reciprocal Rank Fusion (RRF)

Reciprocal Rank Fusion aggregates multiple ranked result lists without requiring score normalization.

For each retrieval system:

- Results are independently ranked.
- A score is assigned based on rank position.

The RRF score is defined as:

RRF_score = Σ (1 / (k + rank_i))

Where:

- rank_i = rank position in retrieval method i
- k = smoothing constant (commonly 60)

### Why RRF?

RRF was selected because:

- It does not require similarity score normalization.
- It is robust to score distribution differences.
- It improves stability when one retrieval method underperforms.
- It is computationally inexpensive.

This approach improves recall compared to lexical search alone and improves precision compared to embedding-only search.

---

## Embedding Pipeline

Embeddings are generated using:

- Sentence Transformers
- all-MiniLM-L6-v2 (384 dimensions)

Embeddings are:

- Generated locally
- Stored alongside dataset metadata
- Indexed using MongoDB Atlas `$vectorSearch`

This avoids external API dependency and ensures reproducibility.

---

## ETL Design

The ETL pipeline is implemented as a standalone CLI module.

Responsibilities:

- Fetch datasets from UKCEH catalogue
- Parse metadata (JSON/XML)
- Store structured records
- Generate embeddings
- Support reprocessing

Separation of ETL from API ensures:

- Deterministic ingestion
- Batch processing capability
- Reduced coupling between ingestion and serving layers

---

## RAG Pipeline

The RAG system:

1. Retrieves top-k datasets via hybrid search
2. Constructs context from metadata fields
3. Passes context to language model
4. Returns generated answer with citation references

The system prioritizes grounded responses using retrieved metadata rather than free-form generation.

---

## Security Model

- JWT-based authentication
- Role-based access control
- Admin-only upload routes
- Environment-based secret configuration

---

## Deployment Model

- Dockerized services
- Nginx reverse proxy
- HTTPS via Let's Encrypt
- Hosted on Linode

The deployment configuration mirrors local development through Docker Compose.
