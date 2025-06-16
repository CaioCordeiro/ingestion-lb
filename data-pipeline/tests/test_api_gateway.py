"""Tests for API Gateway service."""

from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest
from api_gateway.main import app, forward_request
from fastapi.testclient import TestClient


class TestAPIGateway:
    """Test class for API Gateway service."""

    @pytest.fixture
    def client(self):
        """Test client fixture."""
        return TestClient(app)

    @pytest.fixture
    def mock_http_client(self):
        """Mock HTTP client."""
        with patch("api_gateway.main.http_client") as mock:
            yield mock

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "api_gateway"}

    def test_correlation_id_middleware_generates_id(self, client):
        """Test that middleware generates correlation ID when not provided."""
        response = client.get("/health")
        assert "X-Correlation-ID" in response.headers
        assert len(response.headers["X-Correlation-ID"]) > 0

    def test_correlation_id_middleware_preserves_id(self, client):
        """Test that middleware preserves provided correlation ID."""
        correlation_id = "test-correlation-123"
        response = client.get("/health", headers={"X-Correlation-ID": correlation_id})
        assert response.headers["X-Correlation-ID"] == correlation_id

    def test_ingest_text_success(self, client, mock_http_client):
        """Test successful text ingestion forwarding."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_http_client.request = AsyncMock(return_value=mock_response)

        test_data = {"text": "Test message"}
        response = client.post("/ingest/text", json=test_data)

        assert response.status_code == 200
        mock_http_client.request.assert_called_once()

    def test_ingest_csv_success(self, client, mock_http_client):
        """Test successful CSV ingestion forwarding."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_http_client.post = AsyncMock(return_value=mock_response)
        # mock_http_client.post.return_value = mock_response

        csv_content = "name,age\nJohn,30"
        files = {"file": ("test.csv", csv_content, "text/csv")}
        response = client.post("/ingest/csv", files=files)

        assert response.status_code == 200
        mock_http_client.post.assert_called_once()

    def test_ingest_json_success(self, client, mock_http_client):
        """Test successful JSON ingestion forwarding."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        mock_response.headers = {"content-type": "application/json"}
        # mock_http_client.request.return_value = mock_response
        mock_http_client.request = AsyncMock(return_value=mock_response)

        test_data = {"name": "John", "age": 30}
        response = client.post("/ingest/json", json=test_data)

        assert response.status_code == 200
        mock_http_client.request.assert_called_once()

    def test_ingest_file_success(self, client, mock_http_client):
        """Test successful file ingestion forwarding."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_http_client.post = AsyncMock(return_value=mock_response)

        pdf_content = b"%PDF-1.4 test content"
        files = {"file": ("test.pdf", pdf_content, "application/pdf")}
        response = client.post("/ingest/file", files=files)

        assert response.status_code == 200
        mock_http_client.post.assert_called_once()

    def test_service_unavailable_error(self, client, mock_http_client):
        """Test handling of service unavailable error."""
        mock_http_client.request.side_effect = httpx.RequestError("Connection failed")

        test_data = {"text": "Test message"}
        response = client.post("/ingest/text", json=test_data)

        assert response.status_code == 503
        assert "Service unavailable" in response.json()["detail"]

    def test_forward_request_removes_host_header(self, client, mock_http_client):
        """Test that forward_request removes host header."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_http_client.request = AsyncMock(return_value=mock_response)

        _ = client.post("/ingest/text", json={"text": "test"})

        # Check that the forwarded request doesn't include host header
        call_args = mock_http_client.request.call_args
        headers = call_args[1]["headers"]
        assert "host" not in headers

    def test_forward_request_includes_correlation_id(self, client, mock_http_client):
        """Test that forward_request includes correlation ID in headers."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_http_client.request = AsyncMock(return_value=mock_response)

        correlation_id = "test-correlation-123"
        _ = client.post(
            "/ingest/text",
            json={"text": "test"},
            headers={"X-Correlation-ID": correlation_id},
        )

        # Check that correlation ID is included in forwarded headers
        call_args = mock_http_client.request.call_args
        headers = call_args[1]["headers"]
        assert headers["X-Correlation-ID"] == correlation_id

    def test_file_upload_forwarding(self, client, mock_http_client):
        """Test that file uploads are properly forwarded."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"status": "success"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_http_client.post = AsyncMock(return_value=mock_response)

        test_content = "test,content\n1,2"
        files = {"file": ("test.csv", test_content, "text/csv")}
        response = client.post("/ingest/csv", files=files)

        assert response.status_code == 200

        # Verify file was included in the forwarded request
        call_args = mock_http_client.post.call_args
        assert "files" in call_args[1]

    def test_error_response_forwarding(self, client, mock_http_client):
        """Test that error responses are properly forwarded."""
        mock_response = Mock()
        mock_response.status_code = 400
        mock_response.content = b'{"detail": "Bad request"}'
        mock_response.headers = {"content-type": "application/json"}
        mock_http_client.request = AsyncMock(return_value=mock_response)

        response = client.post("/ingest/text", json={"text": "test"})

        assert response.status_code == 400
        assert response.json()["detail"] == "Bad request"

    @pytest.mark.asyncio
    async def test_forward_request_function_success(self):
        """Test forward_request function directly."""
        with patch("api_gateway.main.http_client") as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = b'{"status": "success"}'
            mock_response.headers = {"content-type": "application/json"}
            mock_client.request = AsyncMock(return_value=mock_response)

            # Create mock request
            mock_request = Mock()
            mock_request.method = "POST"
            mock_request.headers = {"content-type": "application/json"}
            mock_request.query_params = {}
            mock_request.state.correlation_id = "test-123"
            mock_request.body = AsyncMock(return_value=b'{"test": "data"}')

            result = await forward_request("http://test-service", mock_request, "/test")

            assert result.status_code == 200

    @pytest.mark.asyncio
    async def test_forward_request_with_files(self):
        """Test forward_request function with files."""
        with patch("api_gateway.main.http_client") as mock_client:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.content = b'{"status": "success"}'
            mock_response.headers = {"content-type": "application/json"}
            mock_client.post = AsyncMock(return_value=mock_response)

            # Create mock request
            mock_request = Mock()
            mock_request.method = "POST"
            mock_request.headers = {"content-type": "multipart/form-data"}
            mock_request.query_params = {}
            mock_request.state.correlation_id = "test-123"

            # Create mock file
            mock_file = Mock()
            mock_file.filename = "test.csv"
            mock_file.content_type = "text/csv"
            mock_file.read = AsyncMock(return_value=b"test,data")

            files = {"file": mock_file}

            result = await forward_request("http://test-service", mock_request, "/test", files)

            assert result.status_code == 200
            mock_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_forward_request_connection_error(self):
        """Test forward_request function with connection error."""
        with patch("api_gateway.main.http_client") as mock_client:
            mock_client.request.side_effect = httpx.RequestError("Connection failed")

            # Create mock request
            mock_request = Mock()
            mock_request.method = "POST"
            mock_request.headers = {"content-type": "application/json"}
            mock_request.query_params = {}
            mock_request.state.correlation_id = "test-123"
            mock_request.body = AsyncMock(return_value=b'{"test": "data"}')

            with pytest.raises(Exception) as exc_info:
                await forward_request("http://test-service", mock_request, "/test")

            assert "Service unavailable" in str(exc_info.value.detail)
