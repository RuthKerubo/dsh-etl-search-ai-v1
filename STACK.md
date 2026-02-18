# Technology Stack Rationale

## Backend: FastAPI

FastAPI was selected because:

- Native async support
- Strong typing via Pydantic
- Automatic OpenAPI documentation
- Lightweight and suitable for research services

---

## Database: MongoDB Atlas

MongoDB was chosen because:

- Flexible schema for heterogeneous metadata
- Native support for `$vectorSearch`
- Co-location of metadata and embeddings
- Reduced infrastructure complexity

This avoids maintaining a separate vector database (e.g., Chroma or Pinecone).

---

## Embeddings: all-MiniLM-L6-v2

Selected because:

- 384-dimensional embeddings (low memory footprint)
- Strong semantic performance relative to size
- CPU-friendly inference
- No external API dependency

This supports local reproducibility and cost control.

---

## Frontend: Vue 3

Chosen for:

- Lightweight reactive architecture
- Simplicity for moderate UI complexity
- Clear separation of views and state management

The frontend is intentionally minimal and functional.

---

## Deployment: Docker + Nginx

Docker provides:

- Environment consistency
- Reproducible builds
- Isolation of services

Nginx provides:

- Reverse proxy routing
- HTTPS termination
- Static asset handling

---

## Infrastructure Hosting: Linode

Linode was selected for:

- Simplicity
- Cost efficiency
- Direct control over server configuration

---

## Design Philosophy

The stack prioritizes:

- Reproducibility
- Simplicity
- Minimal external dependency
- Moderate-scale dataset support
- Maintainability over complexity
