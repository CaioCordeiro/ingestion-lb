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
3. **Pre-commit**
```bash
make install-pre-commit
```
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
## Unit Tests
### Data Pipeline Tests
This directory contains comprehensive unit and integration tests for the data pipeline services.
### Test Structure
tests/
├── init.py # Test package initialization
├── conftest.py # Pytest configuration and shared fixtures
├── requirements.txt # Test dependencies
├── README.md # This file
├── mock_files/ # Mock files for testing ~
│ ├── sample.pdf # Sample PDF for file ingestion tests
│ ├── sample.csv # Sample CSV for tabular ingestion tests
│ ├── sample.json # Sample JSON for JSON ingestion tests
│ ├── empty.csv # Empty CSV for error testing
│ └── invalid.json # Invalid JSON for error testing
├── test_ingestion_text.py # Tests for text ingestion service
├── test_ingestion_tabular.py # Tests for tabular ingestion service
├── test_ingestion_file.py # Tests for file ingestion service
├── test_ingestion_json.py # Tests for JSON ingestion service
├── test_processing_service.py # Tests for processing service
├── test_api_gateway.py # Tests for API gateway
├── test_shared_modules.py # Tests for shared modules
└── test_integration.py # Integration tests
## Running Tests
### Prerequisites
Install test dependencies:
```bash
make install-test-deps
# or
pip install -r tests/requirements.txt
```
### Run All Tests
```bash
make test
# or
python -m pytest tests/ -v
```
### Run Specific Test Categories
#### Unit Tests Only:
```bash
make test-unit
# or
python -m pytest tests/ -v -m "not integration"
```
#### Integration Tests Only:
```bash
make test-integration
# or
python -m pytest tests/test_integration.py -v
```
#### Specific Service Tests:
```bash
make test-service SERVICE=ingestion_text
# or
python -m pytest tests/test_ingestion_text.py -v
```
#### Specific Test File:
```bash
make test-file FILE=test_processing_service.py
# or
python -m pytest tests/test_processing_service.py -v
```
### Coverage Reports
Generate Coverage Report:
```bash
make test-coverage
# or
python -m pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing
```
The HTML coverage report will be generated in htmlcov/index.html.
### Watch Mode
Run tests in watch mode (re-runs tests when files change):
```bash
make test-watch
```
### Test Categories
#### Unit Tests
Each service has comprehensive unit tests covering:
**Happy Path Scenarios:** Normal operation with valid inputs
**Error Handling:** Invalid inputs, missing data, malformed requests
**Edge Cases:** Empty data, special characters, boundary conditions
**Infrastructure Failures:** Database/queue connection failures
**Business Logic:** Text processing, data transformation, validation
#### Integration Tests
Integration tests cover end-to-end scenarios:
**Complete Pipeline Flow:** From ingestion to processing to storage
**Error Propagation:** How errors flow through the system
**Correlation Tracking:** Message correlation across services
**Concurrent Processing:** Multiple messages processed simultaneously
**Data Quality:** Text processing consistency and quality
#### Mock Strategy
All infrastructure components are mocked to focus on business logic:
**RabbitMQ:** Mocked using unittest.mock
**Elasticsearch:** Mocked using unittest.mock
**File System:** Mock files provided in mock_files/
**HTTP Clients:** Mocked using unittest.mock
**External Libraries:** PDF/DOCX processing libraries mocked
### Coverage Goals
Target coverage metrics:
**Overall Coverage:** >90%
**Business Logic:** >95%
**Error Handling:** >85%
**Integration Paths:** >80%
## Running Tests in CI/CD
For continuous integration, use:
```bash
# Install dependencies
pip install -r tests/requirements.txt
# Run tests with coverage
python -m pytest tests/ -v --cov=. --cov-report=xml --cov-report=term
# Generate coverage badge (if using coverage badge tools)
coverage-badge -o coverage.svg
```
