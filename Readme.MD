# DSH Environmental Dataset Search

**Live Demo**: https://dsh-etl-search.duckdns.org

## Overview

AI-powered search platform for UK environmental datasets from the UKCEH Environmental Data Centre. Combines hybrid semantic + keyword search, RAG Q&A, and ISO 19115 metadata compliance.

## Features

- **Hybrid Search** - Combines vector embeddings + keyword matching via Reciprocal Rank Fusion
- **RAG Q&A** - Ask questions about datasets, get answers with source citations
- **ISO 19115 Compliance** - Metadata validation and scoring
- **Document Upload** - PDF extraction with auto-metadata generation (admin only)
- **User Roles** - Admin and Researcher access levels with JWT auth

## Tech Stack

### Backend
- FastAPI, Python 3.10
- Motor (async MongoDB driver)
- sentence-transformers/all-MiniLM-L6-v2 (384-dimension embeddings)
- PyJWT for authentication

### Frontend
- Vue 3, Vue Router, Pinia
- Tailwind CSS

### Database
- MongoDB Atlas with $vectorSearch index

### Deployment
- Docker, Docker Compose
- Linode, Nginx, Let's Encrypt SSL

## Architecture

```
┌──────────────────────────────────────────────────────┐
│              Vue 3 Frontend                          │
│         https://dsh-etl-search.duckdns.org          │
│                                                      │
│  Home · Search · Dataset Detail · Chat (RAG)         │
│  Login · Register · Admin Portal                     │
└──────────────────────────┬───────────────────────────┘
                           │ HTTPS
                           ▼
┌──────────────────────────────────────────────────────┐
│              FastAPI Backend (:8000)                 │
│                                                      │
│  /api/auth        JWT authentication                 │
│  /api/search      Hybrid search (RRF fusion)         │
│  /api/datasets    Dataset CRUD                       │
│  /api/rag         RAG Q&A pipeline                   │
│  /api/admin       Upload & moderation (admin only)   │
└──────┬───────────────────────────┬───────────────────┘
       │                           │
       ▼                           ▼
┌─────────────────┐     ┌──────────────────────────┐
│  MongoDB Atlas  │     │  sentence-transformers   │
│                 │     │  all-MiniLM-L6-v2        │
│  datasets       │     │  384-dimension vectors   │
│  + embeddings   │     │  (runs locally, no API)  │
│  users          │     └──────────────────────────┘
│  $vectorSearch  │
└─────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- Node.js 18+
- MongoDB Atlas account with a cluster
- Docker (for deployment)

### Local Development

```bash
# Clone repository
git clone https://github.com/RuthKerubo/dsh-etl-search-ai-v1.git
cd dsh-etl-search-ai-v1

# Backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env and add your MONGODB_URI and JWT_SECRET
uvicorn api.main:app --reload

# Frontend (separate terminal)
cd frontend
npm install
npm run dev
```

- Backend API docs: http://localhost:8000/docs
- Frontend: http://localhost:5173

### Docker Deployment

```bash
cp .env.example .env
# Edit .env with your MONGODB_URI and JWT_SECRET
./deploy.sh
```

## ETL Pipeline

```bash
# Fetch and store datasets from UKCEH catalogue
python -m etl.cli run

# Generate embeddings (stores in MongoDB)
python -m etl.cli embed

# Check status
python -m etl.cli status

# Test search
python -m etl.cli search "rainfall river catchment"
```

## API Endpoints

| Endpoint | Method | Description | Auth |
|----------|--------|-------------|------|
| `/health` | GET | Health check | None |
| `/api/auth/register` | POST | Register user | None |
| `/api/auth/login` | POST | Login, returns JWT | None |
| `/api/search` | GET | Hybrid search (`?q=`) | Optional |
| `/api/datasets` | GET | List datasets (paginated) | None |
| `/api/datasets/{id}` | GET | Dataset detail | None |
| `/api/rag` | POST | RAG Q&A with citations | Researcher+ |
| `/api/admin/upload` | POST | Upload PDF dataset | Admin |
| `/api/admin/pending` | GET | List pending uploads | Admin |

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `MONGODB_URI` | MongoDB Atlas connection string | Yes |
| `MONGODB_DATABASE` | Database name | Yes (default: `dsh_etl_search`) |
| `JWT_SECRET` | Secret key for JWT signing | Yes |
| `CACHE_DIR` | API response cache directory | No (default: `data/cache`) |
| `EMBEDDING_MODEL` | Sentence transformer model | No (default: `all-MiniLM-L6-v2`) |

## Project Structure

```
dsh-etl-search-ai-v1/
├── api/                        # FastAPI application
│   ├── main.py                 # App entry point
│   ├── dependencies.py         # Dependency injection
│   ├── auth/                   # JWT auth service
│   ├── routers/                # Route handlers
│   │   ├── auth.py
│   │   ├── search.py
│   │   ├── datasets.py
│   │   ├── rag.py
│   │   ├── admin.py
│   │   └── upload.py
│   └── schemas/                # Pydantic request/response models
├── etl/                        # ETL pipeline
│   ├── cli.py                  # CLI commands
│   ├── client/                 # UKCEH API client
│   ├── embeddings/             # sentence-transformers service
│   ├── extraction/             # PDF metadata extraction
│   ├── models/                 # Domain models (Dataset, User)
│   ├── parsers/                # JSON/XML parsers
│   ├── pipeline/               # ETL orchestration
│   ├── rag/                    # RAG pipeline (retriever, context, generator)
│   └── repository/             # MongoDB data access
├── frontend/                   # Vue 3 application
│   └── src/
│       ├── views/              # Pages (Home, Search, Chat, Admin, etc.)
│       ├── stores/             # Pinia state (auth)
│       ├── router.js
│       └── api.js
├── tests/                      # Test suite
├── Dockerfile
├── docker-compose.yml
├── deploy.sh
└── requirements.txt
```

## Data Sources

- **UKCEH Environmental Data Centre** - 200+ environmental datasets (automated ETL)
- **Admin uploads** - PDF documents processed via the admin portal

## License

MIT

## Author

Ruth Kerubo — [GitHub](https://github.com/RuthKerubo)
