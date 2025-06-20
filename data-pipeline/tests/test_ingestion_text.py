"""Tests for text ingestion service."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from ingestion_text.main import app


class TestTextIngestionService:
    """Test class for text ingestion service."""

    @pytest.fixture
    def client(self):
        """Test client fixture."""
        return TestClient(app)

    @pytest.fixture
    def mock_producer(self):
        """Mock RabbitMQ producer."""
        with patch("ingestion_text.main.rabbitmq_producer") as mock:
            mock.connect.return_value = True
            mock.publish_message.return_value = True
            yield mock

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "ingestion_text"}

    def test_ingest_text_success(self, client, mock_producer):
        """Test successful text ingestion."""
        test_data = {"text": "This is a test message"}

        response = client.post("/ingest", json=test_data)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "success"
        assert response_data["message"] == "Text ingested successfully"
        assert "correlation_id" in response_data

        # Verify producer was called
        mock_producer.publish_message.assert_called_once()

    def test_ingest_text_with_correlation_id(self, client, mock_producer):
        """Test text ingestion with provided correlation ID."""
        test_data = {"text": "Test message with correlation ID"}
        correlation_id = "test-correlation-123"

        response = client.post(
            "/ingest", json=test_data, headers={"X-Correlation-ID": correlation_id}
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["correlation_id"] == correlation_id

    def test_ingest_text_empty_text(self, client, mock_producer):
        """Test ingestion with empty text."""
        test_data = {"text": ""}

        response = client.post("/ingest", json=test_data)

        assert response.status_code == 200
        # Should still succeed but with empty text
        mock_producer.publish_message.assert_called_once()

    def test_ingest_text_producer_failure(self, client, mock_producer):
        """Test handling of producer failure."""
        mock_producer.publish_message.return_value = False
        test_data = {"text": "Test message"}

        response = client.post("/ingest", json=test_data)

        assert response.status_code == 500
        assert "Failed to process text ingestion" in response.json()["detail"]

    def test_ingest_text_invalid_json(self, client):
        """Test handling of invalid JSON."""
        response = client.post("/ingest", data="invalid json")

        assert response.status_code == 422  # Unprocessable Entity

    @patch("ingestion_text.main.rabbitmq_producer")
    def test_startup_event(self, mock_producer):
        """Test startup event."""
        with TestClient(app):
            mock_producer.connect.assert_called_once()

    def test_ingest_text_producer_exception(self, client, mock_producer):
        """Test handling of producer exception."""
        mock_producer.publish_message.side_effect = Exception("Connection error")
        test_data = {"text": "Test message"}

        response = client.post("/ingest", json=test_data)

        assert response.status_code == 500
        assert "Connection error" in response.json()["detail"]
