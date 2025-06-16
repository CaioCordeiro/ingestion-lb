"""Tests for tabular ingestion service."""

import io
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from ingestion_tabular.main import app


class TestTabularIngestionService:
    """Test class for tabular ingestion service."""

    @pytest.fixture
    def client(self):
        """Test client fixture."""
        return TestClient(app)

    @pytest.fixture
    def mock_producer(self):
        """Mock RabbitMQ producer."""
        with patch("ingestion_tabular.main.rabbitmq_producer") as mock:
            mock.connect.return_value = True
            mock.publish_message.return_value = True
            yield mock

    @pytest.fixture
    def sample_csv_content(self):
        """Sample CSV content."""
        return "name,age,city\nJohn,30,NYC\nJane,25,LA"

    @pytest.fixture
    def sample_csv_file(self, sample_csv_content):
        """Sample CSV file."""
        return io.BytesIO(sample_csv_content.encode())

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "ingestion_tabular"}

    def test_ingest_csv_success(self, client, mock_producer, sample_csv_content):
        """Test successful CSV ingestion."""
        files = {"file": ("test.csv", sample_csv_content, "text/csv")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "success"
        assert "2 rows processed" in response_data["message"]
        assert response_data["filename"] == "test.csv"

        # Should call publish_message twice (once per row)
        assert mock_producer.publish_message.call_count == 2

    def test_ingest_csv_with_correlation_id(self, client, mock_producer, sample_csv_content):
        """Test CSV ingestion with correlation ID."""
        files = {"file": ("test.csv", sample_csv_content, "text/csv")}
        correlation_id = "test-correlation-123"

        response = client.post("/ingest", files=files, headers={"X-Correlation-ID": correlation_id})

        assert response.status_code == 200
        assert response.json()["correlation_id"] == correlation_id

    def test_ingest_invalid_file_format(self, client):
        """Test ingestion with invalid file format."""
        files = {"file": ("test.txt", "some text", "text/plain")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 400
        assert "Invalid file format" in response.json()["detail"]

    def test_ingest_empty_csv(self, client, mock_producer):
        """Test ingestion with empty CSV."""
        files = {"file": ("empty.csv", "", "text/csv")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 400
        assert "empty" in response.json()["detail"].lower()

    def test_ingest_malformed_csv(self, client, mock_producer):
        """Test ingestion with malformed CSV."""
        malformed_csv = "name,age\nJohn,30,extra_field\nJane"
        files = {"file": ("malformed.csv", malformed_csv, "text/csv")}

        response = client.post("/ingest", files=files)

        # Should still process successfully, pandas is forgiving
        assert response.status_code == 200

    def test_ingest_csv_producer_failure(self, client, mock_producer, sample_csv_content):
        """Test handling of producer failure."""
        mock_producer.publish_message.return_value = False
        files = {"file": ("test.csv", sample_csv_content, "text/csv")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert "0 rows processed, 2 rows failed" in response_data["message"]

    def test_ingest_csv_mixed_success_failure(self, client, mock_producer, sample_csv_content):
        """Test handling of mixed success/failure."""
        # Mock producer to fail on second call
        mock_producer.publish_message.side_effect = [True, False]
        files = {"file": ("test.csv", sample_csv_content, "text/csv")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert "1 rows processed, 1 rows failed" in response_data["message"]

    def test_ingest_csv_with_special_characters(self, client, mock_producer):
        """Test CSV with special characters."""
        csv_content = "name,description\nJohn,\"Hello, world!\"\nJane,Test with 'quotes'"
        files = {"file": ("special.csv", csv_content, "text/csv")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        assert mock_producer.publish_message.call_count == 2

    def test_ingest_csv_producer_exception(self, client, mock_producer, sample_csv_content):
        """Test handling of producer exception."""
        mock_producer.publish_message.side_effect = Exception("Connection error")
        files = {"file": ("test.csv", sample_csv_content, "text/csv")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert "0 rows processed, 2 rows failed" in response_data["message"]
