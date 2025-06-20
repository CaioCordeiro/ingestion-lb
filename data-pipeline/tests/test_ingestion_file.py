"""Tests for file ingestion service."""

from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient
from ingestion_file.main import app, extract_text_from_docx, extract_text_from_pdf


class TestFileIngestionService:
    """Test class for file ingestion service."""

    @pytest.fixture
    def client(self):
        """Test client fixture."""
        return TestClient(app)

    @pytest.fixture
    def mock_producer(self):
        """Mock RabbitMQ producer."""
        with patch("ingestion_file.main.rabbitmq_producer") as mock:
            mock.connect.return_value = True
            mock.publish_message.return_value = True
            yield mock

    @pytest.fixture
    def sample_pdf_content(self):
        """Sample PDF content."""
        # Simple PDF content for testing
        return b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n%%EOF"

    @pytest.fixture
    def sample_docx_content(self):
        """Sample DOCX content (mock)."""
        return b"PK\x03\x04\x14\x00\x00\x00\x08\x00"  # ZIP header for DOCX

    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "service": "ingestion_file"}

    @patch("ingestion_file.main.extract_text_from_pdf")
    def test_ingest_pdf_success(self, mock_extract_pdf, client, mock_producer, sample_pdf_content):
        """Test successful PDF ingestion."""
        mock_extract_pdf.return_value = "Extracted PDF text content"
        files = {"file": ("test.pdf", sample_pdf_content, "application/pdf")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "success"
        assert response_data["filename"] == "test.pdf"
        assert response_data["text_length"] == len("Extracted PDF text content")

        mock_producer.publish_message.assert_called_once()

    @patch("ingestion_file.main.extract_text_from_docx")
    def test_ingest_docx_success(
        self, mock_extract_docx, client, mock_producer, sample_docx_content
    ):
        """Test successful DOCX ingestion."""
        mock_extract_docx.return_value = "Extracted DOCX text content"
        files = {
            "file": (
                "test.docx",
                sample_docx_content,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
        }

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "success"
        assert response_data["filename"] == "test.docx"

    def test_ingest_unsupported_format(self, client):
        """Test ingestion with unsupported file format."""
        files = {"file": ("test.txt", b"some text", "text/plain")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 400
        assert "Unsupported file format" in response.json()["detail"]

    @patch("ingestion_file.main.extract_text_from_pdf")
    def test_ingest_pdf_no_text(self, mock_extract_pdf, client, mock_producer, sample_pdf_content):
        """Test PDF with no extractable text."""
        mock_extract_pdf.return_value = ""
        files = {"file": ("empty.pdf", sample_pdf_content, "application/pdf")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["status"] == "warning"
        assert "No text content extracted" in response_data["message"]

    @patch("ingestion_file.main.extract_text_from_pdf")
    def test_ingest_pdf_extraction_error(
        self, mock_extract_pdf, client, mock_producer, sample_pdf_content
    ):
        """Test PDF extraction error."""
        mock_extract_pdf.side_effect = Exception("PDF extraction failed")
        files = {"file": ("error.pdf", sample_pdf_content, "application/pdf")}

        response = client.post("/ingest", files=files)

        assert response.status_code == 500
        assert "PDF extraction failed" in response.json()["detail"]

    @patch("ingestion_file.main.extract_text_from_pdf")
    def test_ingest_pdf_producer_failure(
        self, mock_extract_pdf, client, mock_producer, sample_pdf_content
    ):
        """Test handling of producer failure."""
        mock_extract_pdf.return_value = "Test content"
        mock_producer.publish_message.return_value = False
        files = {"file": ("test.pdf", sample_pdf_content, "application/pdf")}

        response = client.post("/ingest", files=files)
        _ = response.json()
        assert response.status_code == 500
        assert "Failed to process file ingestion" in response.json()["detail"]

    def test_ingest_with_correlation_id(self, client, mock_producer):
        """Test file ingestion with correlation ID."""
        with patch("ingestion_file.main.extract_text_from_pdf") as mock_extract:
            mock_extract.return_value = "Test content"
            files = {"file": ("test.pdf", b"pdf content", "application/pdf")}
            correlation_id = "test-correlation-123"

            response = client.post(
                "/ingest", files=files, headers={"X-Correlation-ID": correlation_id}
            )

            assert response.status_code == 200
            assert response.json()["correlation_id"] == correlation_id

    @pytest.mark.asyncio
    async def test_extract_text_from_pdf_success(self):
        """Test PDF text extraction function."""
        with patch("pypdf.PdfReader") as mock_reader:
            mock_page = Mock()
            mock_page.extract_text.return_value = "Page content"
            mock_reader.return_value.pages = [mock_page]

            result = await extract_text_from_pdf(b"pdf content")

            assert result == "Page content\n"

    @pytest.mark.asyncio
    async def test_extract_text_from_pdf_error(self):
        """Test PDF text extraction error."""
        with patch("pypdf.PdfReader") as mock_reader:
            mock_reader.side_effect = Exception("Invalid PDF")

            with pytest.raises(Exception) as exc_info:
                await extract_text_from_pdf(b"invalid pdf")

            assert "Error extracting text from PDF" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_extract_text_from_docx_success(self):
        """Test DOCX text extraction function."""
        with patch("docx.Document") as mock_document:
            mock_paragraph = Mock()
            mock_paragraph.text = "Paragraph content"
            mock_document.return_value.paragraphs = [mock_paragraph]
            mock_document.return_value.tables = []

            result = await extract_text_from_docx(b"docx content")

            assert "Paragraph content" in result

    @pytest.mark.asyncio
    async def test_extract_text_from_docx_with_tables(self):
        """Test DOCX text extraction with tables."""
        with patch("docx.Document") as mock_document:
            # Mock paragraph
            mock_paragraph = Mock()
            mock_paragraph.text = "Paragraph content"

            # Mock table
            mock_cell = Mock()
            mock_cell.text = "Cell content"
            mock_row = Mock()
            mock_row.cells = [mock_cell]
            mock_table = Mock()
            mock_table.rows = [mock_row]

            mock_document.return_value.paragraphs = [mock_paragraph]
            mock_document.return_value.tables = [mock_table]

            result = await extract_text_from_docx(b"docx content")

            assert "Paragraph content" in result
            assert "Cell content" in result

    @pytest.mark.asyncio
    async def test_extract_text_from_docx_error(self):
        """Test DOCX text extraction error."""
        with patch("docx.Document") as mock_document:
            mock_document.side_effect = Exception("Invalid DOCX")

            with pytest.raises(Exception) as exc_info:
                await extract_text_from_docx(b"invalid docx")

            assert "Error extracting text from DOCX" in str(exc_info.value)
