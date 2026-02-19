# Running the Project

## Docker
```bash
docker-compose up --build
```

Access the API at http://localhost:8000/docs

---

## Local Development

### Backend
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Set MONGODB_URI and JWT_SECRET

uvicorn api.main:app --reload
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

---

## Testing
```bash
pytest tests/
```

---

## API Examples
```bash
# Health check
curl http://localhost:8000/health

# Search
curl "http://localhost:8000/api/search?q=climate"

# Dataset detail
curl "http://localhost:8000/api/datasets/{identifier}"
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `MONGODB_URI` | MongoDB Atlas connection string |
| `JWT_SECRET` | Secret key for JWT signing |
| `DEBUG` | Enable debug mode (optional) |