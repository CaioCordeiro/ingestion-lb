"""Tests for JSON ingestion service."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from ingestion_json.main import app, extract_text_from_json


class TestJSONIngestionService:
    """Test class for JSON ingestion service."""

    @pytest.fixture
    def client(self):
        """Test client fixture."""
        return TestClient(app)

    @pytest.fixture
    def mock_producer(self):
        """Mock RabbitMQ producer."""
        with patch("ingestion_json.main.rabbitmq_producer") as mock:
            mock.connect.return_value = True
            mock.publish_message.return_value = True
            yield mock

    @pytest.fixture
    def sample_json_data(self):
        """Sample JSON data."""
        return {
            "name": "John Doe",
            "age": 30,
            "profile": {
                "bio": "Software engineer",
                "skills": ["Python", "JavaScript"],
                "location": "New York",
            },
        }

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "ingestion_json"}

    def test_ingest_json_success(self, client, mock_producer, sample_json_data):
        """Test successful JSON ingestion."""
        response = client.post("/ingest", json=sample_json_data)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "success"
        assert response_data["message"] == "JSON ingested successfully"
        assert "extracted_fields" in response_data

        mock_producer.publish_message.assert_called_once()

    def test_ingest_json_with_correlation_id(self, client, mock_producer, sample_json_data):
        """Test JSON ingestion with correlation ID."""
        correlation_id = "test-correlation-123"

        response = client.post(
            "/ingest",
            json=sample_json_data,
            headers={"X-Correlation-ID": correlation_id},
        )

        assert response.status_code == 200
        assert response.json()["correlation_id"] == correlation_id

    def test_ingest_json_empty_data(self, client, mock_producer):
        """Test ingestion with empty JSON data."""
        empty_data = {}

        response = client.post("/ingest", json=empty_data)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "warning"
        assert "No text content found" in response_data["message"]

    def test_ingest_json_only_numbers(self, client, mock_producer):
        """Test ingestion with JSON containing only numbers."""
        numeric_data = {"count": 42, "price": 19.99, "active": True}

        response = client.post("/ingest", json=numeric_data)

        assert response.status_code == 200
        response_data = response.json()
        # Should extract field names as text
        assert response_data["status"] == "success"

    def test_ingest_json_nested_structure(self, client, mock_producer):
        """Test ingestion with deeply nested JSON."""
        nested_data = {"level1": {"level2": {"level3": {"message": "Deep nested message"}}}}

        response = client.post("/ingest", json=nested_data)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "success"

    def test_ingest_json_array(self, client, mock_producer):
        """Test ingestion with JSON array."""
        array_data = [
            {"name": "Item 1", "description": "First item"},
            {"name": "Item 2", "description": "Second item"},
        ]

        response = client.post("/ingest", json=array_data)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "success"

    def test_ingest_json_producer_failure(self, client, mock_producer, sample_json_data):
        """Test handling of producer failure."""
        mock_producer.publish_message.return_value = False

        response = client.post("/ingest", json=sample_json_data)

        assert response.status_code == 500
        assert "Failed to process JSON ingestion" in response.json()["detail"]

    def test_ingest_invalid_json(self, client):
        """Test handling of invalid JSON."""
        response = client.post("/ingest", data="invalid json")

        assert response.status_code == 400

    def test_ingest_json_producer_exception(self, client, mock_producer, sample_json_data):
        """Test handling of producer exception."""
        mock_producer.publish_message.side_effect = Exception("Connection error")

        response = client.post("/ingest", json=sample_json_data)

        assert response.status_code == 500
        assert "Connection error" in response.json()["detail"]

    def test_extract_text_from_json_dict(self):
        """Test text extraction from dictionary."""
        data = {"name": "John", "age": 30, "bio": "Engineer"}
        result = extract_text_from_json(data)

        assert "name" in result
        assert "John" in result
        assert "bio" in result
        assert "Engineer" in result
        # Age should not be included as it's not a string
        assert "30" not in result

    def test_extract_text_from_json_list(self):
        """Test text extraction from list."""
        data = ["item1", "item2", {"key": "value"}]
        result = extract_text_from_json(data)

        assert "item1" in result
        assert "item2" in result
        assert "key" in result
        assert "value" in result

    def test_extract_text_from_json_nested(self):
        """Test text extraction from nested structure."""
        data = {"user": {"profile": {"name": "John", "skills": ["Python", "JavaScript"]}}}
        result = extract_text_from_json(data)

        assert "user" in result
        assert "profile" in result
        assert "name" in result
        assert "John" in result
        assert "skills" in result
        assert "Python" in result
        assert "JavaScript" in result

    def test_extract_text_from_json_empty_strings(self):
        """Test text extraction ignores empty strings."""
        data = {"valid": "content", "empty": "", "whitespace": "   "}
        result = extract_text_from_json(data)

        assert "valid" in result
        assert "content" in result
        assert "empty" in result  # Key is included
        assert "" not in result  # Empty value is not included
        assert "   " not in result  # Whitespace-only value is not included

    def test_extract_text_from_json_mixed_types(self):
        """Test text extraction with mixed data types."""
        data = {
            "string_field": "text value",
            "number_field": 42,
            "boolean_field": True,
            "null_field": None,
            "list_field": ["item1", 123, "item2"],
        }
        result = extract_text_from_json(data)

        assert "string_field" in result
        assert "text value" in result
        assert "number_field" in result  # Key is included
        assert "boolean_field" in result  # Key is included
        assert "null_field" in result  # Key is included
        assert "list_field" in result  # Key is included
        assert "item1" in result
        assert "item2" in result
        # Numbers, booleans, and None values should not be included
        assert 42 not in result
        assert True not in result
        assert None not in result
