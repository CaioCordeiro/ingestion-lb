# Data Pipeline Microservices

This project implements a scalable data ingestion and processing pipeline using microservices architecture. It supports multiple ingestion types (text, CSV/tabular, JSON, files), processes raw data, and indexes processed results into Elasticsearch. RabbitMQ is used as the message broker for decoupling ingestion and processing.

---

## Project Overview

The pipeline consists of the following components:

- **API Gateway**: Unified REST API for all ingestion types.
- **Ingestion Services**: Specialized microservices for:
  - Text (`ingestion_text`)
  - Tabular/CSV (`ingestion_tabular`)
  - JSON (`ingestion_json`)
  - File (PDF/DOCX) (`ingestion_file`)
- **Processing Service**: Consumes raw data from RabbitMQ, processes/cleans it, and indexes into Elasticsearch.
- **RabbitMQ**: Message broker for decoupling ingestion and processing.
- **Elasticsearch**: Search engine for storing and querying processed data.

All services are orchestrated via Docker Compose.

---

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)

---

## Environment Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd data-pipeline
   ```

2. **Environment variables:**  
   Most configuration is set via sensible defaults in `docker-compose.yml`.  
   To override, create a `.env` file or export variables before running Docker Compose.

---

## Building and Running the Services

To build and start all services (API Gateway, ingestion services, processing, RabbitMQ, Elasticsearch):

```bash
docker-compose up --build
```

To stop the stack:
```bash
docker-compose down
```

**Useful URLs:**
- API Gateway: [http://localhost:8000/docs](http://localhost:8000/docs) (Swagger UI)
- RabbitMQ Management: [http://localhost:15672](http://localhost:15672) (user/pass: guest/guest)
- Elasticsearch: [http://localhost:9200](http://localhost:9200)

---

## Test Examples

### 1. Ingest Text
```bash
curl -X POST http://localhost:8000/ingest/text \
     -H 'Content-Type: application/json' \
     -d '{"text": "This is a test document for the pipeline."}'
```

### 2. Ingest CSV
```bash
curl -X POST http://localhost:8000/ingest/csv \
     -F 'file=@/path/to/your/file.csv'
```

### 3. Ingest JSON
```bash
curl -X POST http://localhost:8000/ingest/json \
     -H 'Content-Type: application/json' \
     -d '{"title": "Sample", "body": "This is a test", "tags": ["pipeline", "test"]}'
```

### 4. Ingest File (PDF or DOCX)
```bash
curl -X POST http://localhost:8000/ingest/file \
     -F 'file=@/path/to/your/file.pdf'
```

### 5. Health Check
```bash
curl http://localhost:8000/health
```

---

## Querying Results in Elasticsearch

After ingestion and processing, data is indexed in Elasticsearch (`processed_texts` index).

Example query:
```bash
curl 'http://localhost:9200/processed_texts/_search?pretty'
```

---

## Troubleshooting & Tips
- **Port conflicts:** Make sure ports 8000, 15672, 5672, 9200, 9300 are available.
- **Docker not running:** Ensure Docker Desktop or your Docker daemon is running.
- **Logs:** View logs for a service with e.g.  
  `docker-compose logs api_gateway`
- **RabbitMQ UI:** Use [http://localhost:15672](http://localhost:15672) (guest/guest) to monitor queues.
- **Elasticsearch UI:** Use [http://localhost:9200](http://localhost:9200) or [Kibana](https://www.elastic.co/kibana) if you add it.

---

## Local Development (Optional)

You can run a single service locally for debugging:
```bash
cd data-pipeline/ingestion_text
uvicorn main:app --reload --port 8001
```
Make sure dependencies are installed (`pip install -r ../requirements.txt`) and RabbitMQ/Elasticsearch are running (via Docker Compose).

---
