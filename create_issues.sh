#!/bin/bash
# =============================================================================
# DSH ETL Search AI v1 - GitHub Issues
# Architecture: MongoDB Atlas + SentenceTransformers + FastAPI (async)
# =============================================================================

# --- #1 Project Setup and Structure ---
gh issue create --title "Project Setup and Structure" --label "chore,infrastructure" --body "## Description
Set up the project structure, virtual environment, and dependencies.

**Branch:** \`feature/project-setup\`

## Tasks
- [x] Initialise Git repository
- [x] Create project structure (/etl, /api, /tests)
- [x] Set up Python virtual environment
- [x] Create requirements.txt with core dependencies
- [x] Add .gitignore and .env.example
- [x] Configure python-dotenv for environment management

## Acceptance Criteria
- Project structure matches architecture design
- pip install -r requirements.txt works
- .env.example documents all required environment variables"

# --- #2 Domain Models (Pydantic) ---
gh issue create --title "Domain Models - Pydantic Implementation" --label "feature,etl" --body "## Description
Implement Pydantic v2 models for the unified domain representation. These models are stored directly as MongoDB documents (no ORM layer needed).

**Branch:** \`feature/domain-models\`
**Depends on:** #1

## Tasks
- [x] Create BoundingBox model
- [x] Create TemporalExtent model
- [x] Create DistributionInfo model
- [x] Create ResponsibleParty model with role enum
- [x] Create RelatedDocument and SupportingDocument models
- [x] Create DatasetMetadata composite model
- [x] Create User and UserPreferences models
- [x] Create factory function create_minimal_dataset()

## Acceptance Criteria
- All models use Pydantic v2 with validation
- Models serialise via model_dump(mode='json') for MongoDB storage
- Models deserialise via model_validate() from MongoDB documents"

# --- #3 MongoDB Connection Manager ---
gh issue create --title "MongoDB Atlas Connection Manager" --label "feature,database" --body "## Description
Create async MongoDB connection manager using motor (async driver) to replace SQLite/SQLAlchemy.

**Branch:** \`feature/mongodb-connection\`
**Depends on:** #1

## Tasks
- [x] Create MongoDBConfig dataclass (reads MONGODB_URI from env)
- [x] Create MongoDBConnection class wrapping AsyncIOMotorClient
- [x] Implement connect()/close() lifecycle methods
- [x] Add collection accessors (datasets, users, search_history)
- [x] Implement create_indexes() for text and unique indexes
- [x] Add get_database() convenience function
- [x] Remove old SQLAlchemy SessionFactory and UnitOfWork

## Acceptance Criteria
- Connects to MongoDB Atlas via connection string
- Creates text index on title + abstract
- Creates unique index on identifier
- Ping test confirms connection"

# --- #4 MongoDB Dataset Repository ---
gh issue create --title "Async MongoDB Dataset Repository" --label "feature,database" --body "## Description
Rewrite the dataset repository for async MongoDB, replacing SQLAlchemy ORM.

**Branch:** \`feature/mongodb-repository\`
**Depends on:** #2, #3

## Tasks
- [x] Rewrite DatasetRepository with AsyncIOMotorCollection
- [x] Implement async get() using find_one()
- [x] Implement async save() using replace_one(upsert=True)
- [x] Implement async search() using \$regex on title/abstract
- [x] Implement async get_paged() with skip/limit + count_documents
- [x] Implement async save_many() using bulk_write with upserts
- [x] Implement get_all_for_embedding() with minimal projection
- [x] Handle ObjectId to string conversion for _id fields
- [x] Simplify base.py to only PagedResult and BulkOperationResult
- [x] Delete old ORM models (orm.py) and converters (converters.py)
- [x] Delete old session.py (SQLAlchemy)

## Acceptance Criteria
- All methods are async
- Dataset identifier used as MongoDB _id
- Pydantic model_dump()/model_validate() for serialisation
- ObjectId handled gracefully (str() cast)"

# --- #5 CEH Catalogue Client ---
gh issue create --title "CEH Catalogue Client" --label "feature,etl" --body "## Description
Async HTTP client for fetching metadata from the CEH EIDC Catalogue API.

**Branch:** \`feature/ceh-client\`
**Depends on:** #1

## Tasks
- [x] Implement CEHCatalogueClient with httpx
- [x] Fetch JSON and XML formats per dataset ID
- [x] Batch fetching with rate limiting
- [x] Local file caching for fetched documents
- [x] Structured DatasetFetchResult response

## Acceptance Criteria
- Can fetch all metadata formats for a given dataset ID
- Rate limiting prevents API throttling
- Caching avoids re-downloading"

# --- #6 Metadata Parsers ---
gh issue create --title "Metadata Parsers (ISO 19115, CEH JSON, JSON-LD, RDF)" --label "feature,etl" --body "## Description
Implement parsers for all CEH metadata formats with a registry for automatic format detection.

**Branch:** \`feature/metadata-parsers\`
**Depends on:** #2

## Tasks
- [x] Create abstract MetadataParser base class
- [x] Implement ISO19115Parser (XML with namespace handling)
- [x] Implement CEHJSONParser for catalogue JSON format
- [x] Implement JSONLDParser for Schema.org format
- [x] Implement RDFParser using rdflib for Turtle format
- [x] Create ParserRegistry with auto-detection
- [x] Extract all fields: identifier, title, abstract, keywords, bbox, temporal, parties, distributions

## Acceptance Criteria
- Parses real CEH documents in all formats
- Registry auto-detects format from content type
- Handles missing optional fields gracefully"

# --- #7 ETL Pipeline Orchestrator ---
gh issue create --title "ETL Pipeline Orchestrator" --label "feature,etl" --body "## Description
Pipeline orchestrator that wires fetch -> parse -> store with batching and progress reporting. Uses async MongoDB repository.

**Branch:** \`feature/etl-pipeline\`
**Depends on:** #4, #5, #6

## Tasks
- [x] Create ETLPipeline class with async run()
- [x] Batch commits every N datasets
- [x] Structured PipelineResult with success/failure tracking
- [x] Console progress bar with ProgressCallback
- [x] ResumableETLPipeline with checkpoint support
- [x] CLI integration (cmd_run, cmd_status)
- [x] Update pipeline to use DatasetRepository directly (not SessionFactory)

## Acceptance Criteria
- Can process 200+ dataset IDs
- Failed datasets logged but don't stop pipeline
- Progress visible in console
- Checkpoint allows resuming interrupted runs"

# --- #8 SentenceTransformers Embedding Service ---
gh issue create --title "SentenceTransformers Embedding Service" --label "feature,embeddings" --body "## Description
Free local embedding service using sentence-transformers, replacing Cohere API.

**Branch:** \`feature/sentence-transformers\`
**Depends on:** #1

## Tasks
- [x] Create abstract EmbeddingService base class
- [x] Implement SentenceTransformerService
- [x] Load all-MiniLM-L6-v2 model (384 dimensions)
- [x] Async embed_query() for single text
- [x] Async embed_batch() for bulk embedding
- [x] Delete old Cohere embedding service

## Acceptance Criteria
- No API key required (runs locally)
- Embeddings are 384-dimensional
- Batch processing with progress bar
- Model loads on first use"

# --- #9 MongoDB Atlas Vector Store ---
gh issue create --title "MongoDB Atlas Vector Store" --label "feature,embeddings,database" --body "## Description
Vector store using MongoDB Atlas \$vectorSearch, replacing ChromaDB. Stores embeddings directly on dataset documents.

**Branch:** \`feature/mongodb-vector-store\`
**Depends on:** #4, #8

## Tasks
- [x] Rewrite VectorStore for MongoDB Atlas
- [x] Store embeddings as 'embedding' field on dataset documents
- [x] Implement add_datasets() with bulk_write UpdateOne operations
- [x] Implement search() using \$vectorSearch aggregation pipeline
- [x] Implement get_indexed_ids() and get_stats()
- [x] Handle ObjectId to string conversion in search results
- [x] Console progress callback for indexing

## Acceptance Criteria
- Embeddings stored on same document (no separate collection)
- \$vectorSearch returns ranked results with cosine similarity
- Requires Atlas Vector Search index named 'vector_index'
- 384 dimensions, cosine similarity"

# --- #10 Hybrid Search Service ---
gh issue create --title "Hybrid Search Service (Semantic + Keyword)" --label "feature,search" --body "## Description
Combines semantic (vector) and keyword (MongoDB regex) search using Reciprocal Rank Fusion (RRF).

**Branch:** \`feature/hybrid-search\`
**Depends on:** #4, #9

## Tasks
- [x] Implement HybridSearchService with async methods
- [x] Auto-detect query type (ID, title, short, normal)
- [x] Parallel semantic + keyword search via asyncio.create_task
- [x] RRF merge with configurable weights
- [x] Exact match boosting for title and keywords
- [x] Semantic-only and keyword-only convenience methods
- [x] Graceful fallback to keyword-only when embeddings unavailable

## Acceptance Criteria
- Hybrid mode returns better results than either method alone
- Short queries weight keyword higher
- Exact ID queries bypass search entirely
- All repository calls are awaited (fully async)"

# --- #11 FastAPI Application Setup ---
gh issue create --title "FastAPI Application with Async MongoDB" --label "feature,api" --body "## Description
FastAPI application with async lifespan, dependency injection, and CORS for the MongoDB + SentenceTransformers stack.

**Branch:** \`feature/fastapi-setup\`
**Depends on:** #3, #4, #8, #9, #10

## Tasks
- [x] Create FastAPI app with async lifespan handler
- [x] Implement async init_dependencies() / shutdown_dependencies()
- [x] Create dependency getters with Annotated types
- [x] Configure CORS for frontend development
- [x] GET /health - async health check with service status
- [x] GET /api/datasets - async paginated dataset listing
- [x] GET /api/datasets/{id} - async single dataset retrieval
- [x] GET /api/search - hybrid search with mode selection
- [x] Pydantic response schemas for all endpoints
- [x] Auto-detect search mode (hybrid vs keyword-only)

## Acceptance Criteria
- uvicorn api.main:app starts successfully
- /health returns MongoDB + embedding service status
- /api/datasets returns paginated results
- /api/search performs hybrid search with fallback"

# --- #12 Document Upload Endpoint ---
gh issue create --title "Document Upload Endpoint (PDF/CSV/JSON)" --label "feature,api" --body "## Description
POST /api/upload endpoint for uploading documents with automatic text extraction and embedding.

**Branch:** \`feature/document-upload\`
**Depends on:** #11, #8

## Tasks
- [x] Create POST /api/upload endpoint with FastAPI UploadFile
- [x] PDF text extraction using pypdf
- [x] CSV parsing with header/row summary
- [x] JSON content extraction (title/abstract or structure summary)
- [x] Auto-generate dataset identifier
- [x] Store document in MongoDB via DatasetRepository
- [x] Generate and store embedding on upload
- [x] File size validation (10 MB max)
- [x] File type validation (.pdf, .csv, .json)
- [x] Add python-multipart to requirements.txt

## Acceptance Criteria
- curl -X POST /api/upload -F 'file=@doc.pdf' works
- Uploaded document is searchable immediately
- Embedding generated automatically if service available
- Returns identifier, title, abstract, embed status"

# --- #13 RAG Endpoint ---
gh issue create --title "RAG Endpoint (Retrieval Augmented Generation)" --label "feature,api" --body "## Description
POST /api/rag endpoint that retrieves relevant documents via vector search and returns context-based answers.

**Branch:** \`feature/rag-endpoint\`
**Depends on:** #9, #11

## Tasks
- [x] Create RAGRequest Pydantic model (question + top_k)
- [x] Create POST /api/rag endpoint accepting JSON body
- [x] Retrieve top-k documents via VectorStore.search()
- [x] Build RAGContextDocument response objects
- [x] Template-based answer generation (LLM integration ready)
- [x] Handle missing vector store gracefully (503)

## Acceptance Criteria
- curl -X POST /api/rag -d '{\"question\": \"...\"}' works
- Returns context documents with relevance scores
- Answer summarises most relevant documents
- Ready for LLM integration (replace _generate_answer())"

# --- #14 Dependencies and Environment Update ---
gh issue create --title "Dependencies and Environment Configuration" --label "chore,infrastructure" --body "## Description
Update requirements.txt and .env.example for the MongoDB + SentenceTransformers stack.

**Branch:** \`feature/dependencies-update\`
**Depends on:** #1

## Tasks
- [x] Add motor, pymongo for async MongoDB
- [x] Add sentence-transformers, torch for local embeddings
- [x] Add pypdf for PDF processing
- [x] Add python-multipart for file uploads
- [x] Remove sqlalchemy, chromadb, cohere references
- [x] Update .env.example with MONGODB_URI, MONGODB_DATABASE
- [x] Remove old DATABASE_PATH, CHROMA_PATH, COHERE_API_KEY vars

## Acceptance Criteria
- pip install -r requirements.txt installs all needed packages
- .env.example documents all environment variables
- No references to old stack (SQLite, ChromaDB, Cohere)"

# --- #15 CLI Tool ---
gh issue create --title "CLI Tool for ETL Operations" --label "feature,etl" --body "## Description
Command-line interface for running ETL pipeline, managing embeddings, and searching datasets.

**Branch:** \`feature/cli\`
**Depends on:** #3, #4, #7, #8, #9

## Tasks
- [x] cmd_init - Create MongoDB indexes
- [x] cmd_run - Run ETL pipeline
- [x] cmd_embed - Generate embeddings for all datasets
- [x] cmd_search - Search datasets from terminal
- [x] cmd_status - Show database and embedding statistics
- [x] All commands use async MongoDB connection
- [x] Remove all SQLAlchemy/Cohere/ChromaDB references

## Acceptance Criteria
- python -m etl.cli init creates indexes
- python -m etl.cli status shows dataset and embedding counts
- python -m etl.cli search 'query' returns results"

# --- #16 Test Infrastructure ---
gh issue create --title "Test Infrastructure Setup" --label "chore,testing" --body "## Description
Set up pytest with async support for MongoDB-based testing.

**Depends on:** #1

## Tasks
- [ ] Configure pytest with pytest-asyncio
- [ ] Create conftest.py with MongoDB test fixtures
- [ ] Add fixture for test MongoDB database
- [ ] Create sample dataset fixtures
- [ ] Unit tests for domain models
- [ ] Unit tests for repository operations
- [ ] Integration tests for search pipeline

## Acceptance Criteria
- pytest runs from project root
- Async tests work with pytest-asyncio
- Tests isolated (separate test database)"

# --- #17 Frontend Setup ---
gh issue create --title "Frontend Setup (Svelte/Vue)" --label "feature,frontend" --body "## Description
Set up frontend project for search UI.

**Depends on:** #11

## Tasks
- [ ] Create frontend project with Vite
- [ ] Configure Tailwind CSS
- [ ] Set up API client for /api endpoints
- [ ] Create basic layout with search bar

## Acceptance Criteria
- Frontend dev server starts
- Can call API endpoints
- Basic search UI functional"

# --- #18 Search UI Component ---
gh issue create --title "Search UI Component" --label "feature,frontend" --body "## Description
Search input and results display component.

**Depends on:** #17

## Tasks
- [ ] Create search input component
- [ ] Display search results as cards
- [ ] Show title, abstract, keywords, relevance score
- [ ] Loading and error states
- [ ] Mode selector (hybrid/keyword/semantic)

## Acceptance Criteria
- Search returns and displays results
- Results show relevance scoring
- Responsive design"

# --- #19 LLM Integration for RAG ---
gh issue create --title "LLM Integration for RAG Answers" --label "feature,api" --body "## Description
Integrate an LLM (Claude API or Ollama) to generate natural language answers from retrieved context.

**Depends on:** #13

## Tasks
- [ ] Create abstract LLMService interface
- [ ] Implement Claude API integration (or Ollama)
- [ ] Replace template-based _generate_answer() with LLM call
- [ ] Build context prompt from retrieved documents
- [ ] Stream responses (optional)

## Acceptance Criteria
- RAG endpoint returns natural language answers
- Answers cite source documents
- Configurable LLM provider"

# --- #20 Documentation ---
gh issue create --title "Documentation" --label "documentation" --body "## Description
Complete project documentation.

## Tasks
- [ ] README with setup instructions
- [ ] Architecture documentation
- [ ] API endpoint documentation (or auto-generated /docs)
- [ ] Atlas Vector Search index setup guide
- [ ] Sample queries and curl examples

## Acceptance Criteria
- New developer can set up project from README
- API endpoints documented with examples"

# --- #21 Final Testing and Cleanup ---
gh issue create --title "Final Testing and Cleanup" --label "chore,testing" --body "## Description
Final quality assurance before submission.

## Tasks
- [ ] Run full test suite
- [ ] Manual E2E testing of all endpoints
- [ ] Code cleanup and formatting
- [ ] Verify all 200 datasets processed and searchable
- [ ] Verify vector search returns relevant results
- [ ] Check for security issues (no credentials in code)

## Acceptance Criteria
- All tests pass
- All endpoints respond correctly
- 200 datasets with embeddings in MongoDB Atlas"

echo "All 21 issues created!"
