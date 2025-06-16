"""Tests for processing service."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from processing_service.main import clean_text, main, process_message


class TestProcessingService:
    """Test class for processing service."""

    @pytest.fixture
    def sample_message_data(self):
        """Sample message data for testing."""
        return {
            "correlation_id": "test-correlation-123",
            "source_system": "api_text",
            "source_identifier": "test-source",
            "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "This is a TEST message with MIXED case and   extra   spaces!",
        }

    @pytest.fixture
    def mock_es_client(self):
        """Mock Elasticsearch client."""
        client = Mock()
        client.index_metadata.return_value = True
        return client

    @pytest.fixture
    def mock_rabbitmq_producer(self):
        """Mock RabbitMQ producer."""
        producer = Mock()
        producer.publish_standardized_message.return_value = True
        return producer

    def test_clean_text_basic(self):
        """Test basic text cleaning functionality."""
        input_text = "This is a TEST message with MIXED case and   extra   spaces!"
        expected = "this is a test message with mixed case and extra spaces!"

        result = clean_text(input_text)

        assert result == expected

    def test_clean_text_empty_string(self):
        """Test cleaning empty string."""
        result = clean_text("")
        assert result == ""

    def test_clean_text_none(self):
        """Test cleaning None input."""
        result = clean_text(None)
        assert result == ""

    def test_clean_text_whitespace_only(self):
        """Test cleaning whitespace-only string."""
        result = clean_text("   \n\t  ")
        assert result == ""

    def test_clean_text_special_characters(self):
        """Test cleaning text with special characters."""
        input_text = "Hello, World! How are you? I'm fine."
        # Special characters should be preserved (commented out in function)
        result = clean_text(input_text)
        assert "," in result
        assert "!" in result
        assert "?" in result
        assert "'" in result

    def test_process_message_success(
        self, sample_message_data, mock_es_client, mock_rabbitmq_producer
    ):
        """Test successful message processing."""
        result = process_message(sample_message_data, mock_es_client, mock_rabbitmq_producer)

        assert result is True
        mock_rabbitmq_producer.publish_standardized_message.assert_called_once()
        mock_es_client.index_metadata.assert_called_once()

    def test_process_message_empty_text(self, mock_es_client, mock_rabbitmq_producer):
        """Test processing message with empty text."""
        message_data = {
            "correlation_id": "test-correlation-123",
            "source_system": "api_text",
            "source_identifier": "test-source",
            "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "",
        }

        result = process_message(message_data, mock_es_client, mock_rabbitmq_producer)

        assert result is False
        mock_rabbitmq_producer.publish_standardized_message.assert_not_called()
        mock_es_client.index_metadata.assert_called_once()

        # Check that metadata indicates failure
        call_args = mock_es_client.index_metadata.call_args[0][0]
        assert call_args.processing_status == "failed"
        assert "Empty text content" in call_args.error_message

    def test_process_message_missing_required_field(self, mock_es_client, mock_rabbitmq_producer):
        """Test processing message with missing required field."""
        message_data = {
            "correlation_id": "test-correlation-123",
            "source_system": "api_text",
            # Missing ingestion_timestamp_utc
            "raw_text": "Test message",
        }

        result = process_message(message_data, mock_es_client, mock_rabbitmq_producer)

        assert result is False
        mock_rabbitmq_producer.publish_standardized_message.assert_not_called()
        mock_es_client.index_metadata.assert_called_once()

        # Check that metadata indicates failure
        call_args = mock_es_client.index_metadata.call_args[0][0]
        assert call_args.processing_status == "failed"
        assert "No valid ingestion timestamp found in message" in call_args.error_message

    def test_process_message_producer_failure(
        self, sample_message_data, mock_es_client, mock_rabbitmq_producer
    ):
        """Test handling of producer failure."""
        mock_rabbitmq_producer.publish_standardized_message.return_value = False

        result = process_message(sample_message_data, mock_es_client, mock_rabbitmq_producer)

        assert result is False
        mock_rabbitmq_producer.publish_standardized_message.assert_called_once()

        # Should call index_metadata twice (once for failure update)
        assert mock_es_client.index_metadata.call_count == 2

        # Check final metadata status
        final_call_args = mock_es_client.index_metadata.call_args[0][0]
        assert final_call_args.processing_status == "failed"
        assert "Failed to publish to output queue" in final_call_args.error_message

    def test_process_message_es_failure(
        self, sample_message_data, mock_es_client, mock_rabbitmq_producer
    ):
        """Test handling of Elasticsearch failure."""
        mock_es_client.index_metadata.return_value = False

        result = process_message(sample_message_data, mock_es_client, mock_rabbitmq_producer)

        assert result is False
        mock_rabbitmq_producer.publish_standardized_message.assert_called_once()

        # Should call index_metadata twice (once for failure update)
        assert mock_es_client.index_metadata.call_count == 2

        # Check final metadata status
        final_call_args = mock_es_client.index_metadata.call_args[0][0]
        assert final_call_args.processing_status == "partial_success"
        assert "Failed to store metadata in Elasticsearch" in final_call_args.error_message

    def test_process_message_exception_handling(self, mock_es_client, mock_rabbitmq_producer):
        """Test handling of unexpected exceptions."""
        mock_rabbitmq_producer.publish_standardized_message.side_effect = Exception(
            "Unexpected error"
        )

        message_data = {
            "correlation_id": "test-correlation-123",
            "source_system": "api_text",
            "source_identifier": "test-source",
            "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "Test message",
        }

        result = process_message(message_data, mock_es_client, mock_rabbitmq_producer)

        assert result is False
        mock_es_client.index_metadata.assert_called_once()

        # Check that metadata indicates failure
        call_args = mock_es_client.index_metadata.call_args[0][0]
        assert call_args.processing_status == "failed"
        assert "Unexpected error" in call_args.error_message

    def test_process_message_metadata_exception(
        self, sample_message_data, mock_es_client, mock_rabbitmq_producer
    ):
        """Test handling of metadata storage exception."""
        mock_rabbitmq_producer.publish_standardized_message.side_effect = Exception("Test error")
        mock_es_client.index_metadata.side_effect = Exception("ES error")

        result = process_message(sample_message_data, mock_es_client, mock_rabbitmq_producer)

        assert result is False
        # Should still attempt to store metadata despite the error

    @patch("processing_service.main.RabbitMQConsumer")
    @patch("processing_service.main.RabbitMQProducer")
    @patch("sys.exit")
    def test_main_es_connection_failure(
        self, mock_exit, mock_producer_class, mock_consumer_class, mock_es_client
    ):
        """Test main function with Elasticsearch connection failure."""
        mock_es_client.connect.return_value = False

        main(mock_es_client)

        mock_exit.assert_called_once_with(1)

    @patch("processing_service.main.RabbitMQConsumer")
    @patch("processing_service.main.RabbitMQProducer")
    @patch("sys.exit")
    def test_main_producer_connection_failure(
        self, mock_exit, mock_producer_class, mock_consumer_class, mock_es_client
    ):
        """Test main function with RabbitMQ producer connection failure."""
        mock_es_client.connect.return_value = True

        mock_producer = Mock()
        mock_producer.connect.return_value = False
        mock_producer_class.return_value = mock_producer

        main(mock_es_client)

        mock_exit.assert_called_once_with(1)

    @patch("processing_service.main.RabbitMQConsumer")
    @patch("processing_service.main.RabbitMQProducer")
    @patch("sys.exit")
    def test_main_consumer_connection_failure(
        self, mock_exit, mock_producer_class, mock_consumer_class, mock_es_client
    ):
        """Test main function with RabbitMQ consumer connection failure."""
        mock_es_client.connect.return_value = True

        mock_producer = Mock()
        mock_producer.connect.return_value = True
        mock_producer_class.return_value = mock_producer

        mock_consumer = Mock()
        mock_consumer.connect.return_value = False
        mock_consumer_class.return_value = mock_consumer

        main(mock_es_client)

        mock_exit.assert_called_once_with(1)

    @patch("processing_service.main.RabbitMQConsumer")
    @patch("processing_service.main.RabbitMQProducer")
    def test_main_successful_startup(
        self, mock_producer_class, mock_consumer_class, mock_es_client
    ):
        """Test main function successful startup."""
        mock_es_client.connect.return_value = True

        mock_producer = Mock()
        mock_producer.connect.return_value = True
        mock_producer_class.return_value = mock_producer

        mock_consumer = Mock()
        mock_consumer.connect.return_value = True
        mock_consumer.start_consuming.side_effect = KeyboardInterrupt()
        mock_consumer_class.return_value = mock_consumer

        # Should not raise exception
        main(mock_es_client)

        # Verify cleanup was called
        mock_consumer.close.assert_called_once()
        mock_producer.close.assert_called_once()
        mock_es_client.close.assert_called_once()

    def test_process_message_creates_correct_standardized_message(
        self, sample_message_data, mock_es_client, mock_rabbitmq_producer
    ):
        """Test that process_message creates correct standardized message."""
        process_message(sample_message_data, mock_es_client, mock_rabbitmq_producer)

        # Get the standardized message that was published
        call_args = mock_rabbitmq_producer.publish_standardized_message.call_args[0][0]

        assert call_args.correlation_id == "test-correlation-123"
        assert call_args.source_system == "api_text"
        assert call_args.source_identifier == "test-source"
        assert (
            call_args.processed_text == "this is a test message with mixed case and extra spaces!"
        )
        assert isinstance(call_args.processing_timestamp_utc, datetime)

    def test_process_message_creates_correct_metadata(
        self, sample_message_data, mock_es_client, mock_rabbitmq_producer
    ):
        """Test that process_message creates correct metadata."""
        process_message(sample_message_data, mock_es_client, mock_rabbitmq_producer)

        # Get the metadata that was indexed
        call_args = mock_es_client.index_metadata.call_args[0][0]

        assert call_args.correlation_id == "test-correlation-123"
        assert call_args.source_system == "api_text"
        assert call_args.source_identifier == "test-source"
        assert call_args.processing_status == "success"
        assert call_args.error_message is None
        assert call_args.text_length > 0
        assert "original_text_length" in call_args.additional_metadata
        assert "processing_duration_ms" in call_args.additional_metadata
