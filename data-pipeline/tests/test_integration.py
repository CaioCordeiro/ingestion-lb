"""Integration tests for the data pipeline."""

import asyncio
from unittest.mock import Mock, patch

import pytest
from processing_service.main import process_message


class TestIntegrationScenarios:
    """Integration test scenarios for the complete pipeline."""

    @pytest.fixture
    def mock_infrastructure(self):
        """Mock all infrastructure components."""
        with patch("shared.rabbitmq.pika"), patch(
            "shared.elasticsearch_client.Elasticsearch"
        ), patch("shared.logging_config.configure_logger"):
            # Mock Elasticsearch client
            es_client = Mock()
            es_client.connect.return_value = True
            es_client.index_metadata.return_value = True
            es_client.close.return_value = None

            # Mock RabbitMQ producer
            rabbitmq_producer = Mock()
            rabbitmq_producer.connect.return_value = True
            rabbitmq_producer.publish_message.return_value = True
            rabbitmq_producer.publish_standardized_message.return_value = True
            rabbitmq_producer.close.return_value = None

            # Mock logger
            logger = Mock()

            yield {
                "es_client": es_client,
                "rabbitmq_producer": rabbitmq_producer,
                "logger": logger,
            }

    def test_end_to_end_text_processing(self, mock_infrastructure):
        """Test complete text processing flow."""
        # Simulate message from ingestion service
        raw_message = {
            "correlation_id": "integration-test-123",
            "source_system": "api_text",
            "source_identifier": None,
            "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "This is a TEST message with MIXED case and   extra   spaces!",
        }

        # Process the message
        result = process_message(
            raw_message,
            mock_infrastructure["es_client"],
            mock_infrastructure["rabbitmq_producer"],
        )

        # Verify success
        assert result is True

        # Verify standardized message was published
        mock_infrastructure["rabbitmq_producer"].publish_standardized_message.assert_called_once()
        published_message = mock_infrastructure[
            "rabbitmq_producer"
        ].publish_standardized_message.call_args[0][0]

        # Verify message content
        assert published_message.correlation_id == "integration-test-123"
        assert published_message.source_system == "api_text"
        assert (
            published_message.processed_text
            == "this is a test message with mixed case and extra spaces!"
        )

        # Verify metadata was stored
        mock_infrastructure["es_client"].index_metadata.assert_called_once()
        stored_metadata = mock_infrastructure["es_client"].index_metadata.call_args[0][0]

        # Verify metadata content
        assert stored_metadata.correlation_id == "integration-test-123"
        assert stored_metadata.processing_status == "success"
        assert stored_metadata.text_length > 0

    def test_error_handling_flow(self, mock_infrastructure):
        """Test error handling throughout the pipeline."""
        # Simulate message with missing required field
        invalid_message = {
            "correlation_id": "error-test-123",
            "source_system": "api_text",
            "source_identifier": None,
            # "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "This is a TEST message with MIXED case and   extra   spaces!",
        }
        # Process the message
        result = process_message(
            invalid_message,
            mock_infrastructure["es_client"],
            mock_infrastructure["rabbitmq_producer"],
        )

        # Verify failure
        assert result is False

        # Verify no standardized message was published
        mock_infrastructure["rabbitmq_producer"].publish_standardized_message.assert_not_called()

        # Verify error metadata was stored
        mock_infrastructure["es_client"].index_metadata.assert_called_once()
        stored_metadata = mock_infrastructure["es_client"].index_metadata.call_args[0][0]

        # Verify error metadata content
        assert stored_metadata.correlation_id == "error-test-123"
        assert stored_metadata.processing_status == "failed"
        assert "No valid ingestion timestamp found in message" in stored_metadata.error_message

    def test_partial_failure_flow(self, mock_infrastructure):
        """Test partial failure scenarios."""
        # Configure producer to fail
        mock_infrastructure["rabbitmq_producer"].publish_standardized_message.return_value = False

        raw_message = {
            "correlation_id": "partial-failure-test-123",
            "source_system": "api_text",
            "source_identifier": None,
            "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "Test message",
        }

        # Process the message
        result = process_message(
            raw_message,
            mock_infrastructure["es_client"],
            mock_infrastructure["rabbitmq_producer"],
        )

        # Verify failure
        assert result is False

        # Verify attempt to publish was made
        mock_infrastructure["rabbitmq_producer"].publish_standardized_message.assert_called_once()

        # Verify error metadata was stored (should be called twice - once for success, once for failure update)
        assert mock_infrastructure["es_client"].index_metadata.call_count == 2

        # Check final metadata indicates failure
        final_metadata = mock_infrastructure["es_client"].index_metadata.call_args[0][0]
        assert final_metadata.processing_status == "failed"
        assert "Failed to publish to output queue" in final_metadata.error_message

    def test_message_correlation_tracking(self, mock_infrastructure):
        """Test that correlation IDs are properly tracked through the pipeline."""
        correlation_id = "correlation-tracking-test-456"

        raw_message = {
            "correlation_id": correlation_id,
            "source_system": "csv_upload",
            "source_identifier": "test.csv",
            "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "CSV row data",
        }

        # Process the message
        result = process_message(
            raw_message,
            mock_infrastructure["es_client"],
            mock_infrastructure["rabbitmq_producer"],
        )

        assert result is True

        # Verify correlation ID is preserved in standardized message
        published_message = mock_infrastructure[
            "rabbitmq_producer"
        ].publish_standardized_message.call_args[0][0]
        assert published_message.correlation_id == correlation_id

        # Verify correlation ID is preserved in metadata
        stored_metadata = mock_infrastructure["es_client"].index_metadata.call_args[0][0]
        assert stored_metadata.correlation_id == correlation_id

        # Verify document ID is generated and consistent
        assert published_message.document_id == stored_metadata.document_id
        assert len(published_message.document_id) > 0

    def test_different_source_systems(self, mock_infrastructure):
        """Test processing messages from different source systems."""
        source_systems = [
            ("api_text", None),
            ("csv_upload", "data.csv"),
            ("json_upload", None),
            ("file_upload", "document.pdf"),
        ]

        for source_system, source_identifier in source_systems:
            raw_message = {
                "correlation_id": f"source-test-{source_system}",
                "source_system": source_system,
                "source_identifier": source_identifier,
                "ingestion_timestamp_utc": "2024-01-01T12:00:00",
                "raw_text": f"Content from {source_system}",
            }

            # Process the message
            result = process_message(
                raw_message,
                mock_infrastructure["es_client"],
                mock_infrastructure["rabbitmq_producer"],
            )

            assert result is True

            # Verify source system is preserved
            published_message = mock_infrastructure[
                "rabbitmq_producer"
            ].publish_standardized_message.call_args[0][0]
            assert published_message.source_system == source_system
            assert published_message.source_identifier == source_identifier

    def test_text_processing_quality(self, mock_infrastructure):
        """Test text processing quality and consistency."""
        test_cases = [
            {"input": "UPPERCASE TEXT", "expected": "uppercase text"},
            {
                "input": "Text   with    multiple    spaces",
                "expected": "text with multiple spaces",
            },
            {
                "input": "  Leading and trailing spaces  ",
                "expected": "leading and trailing spaces",
            },
            {
                "input": "Mixed\nLine\nBreaks\tAnd\tTabs",
                "expected": "mixed line breaks and tabs",
            },
        ]

        for i, test_case in enumerate(test_cases):
            raw_message = {
                "correlation_id": f"text-quality-test-{i}",
                "source_system": "api_text",
                "source_identifier": None,
                "ingestion_timestamp_utc": "2024-01-01T12:00:00",
                "raw_text": test_case["input"],
            }

            # Process the message
            result = process_message(
                raw_message,
                mock_infrastructure["es_client"],
                mock_infrastructure["rabbitmq_producer"],
            )

            assert result is True

            # Verify text processing quality
            published_message = mock_infrastructure[
                "rabbitmq_producer"
            ].publish_standardized_message.call_args[0][0]
            assert published_message.processed_text == test_case["expected"]

    def test_metadata_completeness(self, mock_infrastructure):
        """Test that all required metadata fields are populated."""
        raw_message = {
            "correlation_id": "metadata-test-789",
            "source_system": "api_text",
            "source_identifier": "test-source",
            "ingestion_timestamp_utc": "2024-01-01T12:00:00",
            "raw_text": "Test message for metadata validation",
        }

        # Process the message
        result = process_message(
            raw_message,
            mock_infrastructure["es_client"],
            mock_infrastructure["rabbitmq_producer"],
        )

        assert result is True

        # Verify metadata completeness
        stored_metadata = mock_infrastructure["es_client"].index_metadata.call_args[0][0]

        # Required fields
        assert stored_metadata.document_id is not None
        assert stored_metadata.correlation_id == "metadata-test-789"
        assert stored_metadata.source_system == "api_text"
        assert stored_metadata.source_identifier == "test-source"
        assert stored_metadata.ingestion_timestamp_utc is not None
        assert stored_metadata.processing_timestamp_utc is not None
        assert stored_metadata.text_length > 0
        assert stored_metadata.processing_status == "success"
        assert stored_metadata.error_message is None

        # Additional metadata
        assert stored_metadata.additional_metadata is not None
        assert "original_text_length" in stored_metadata.additional_metadata
        assert "processing_duration_ms" in stored_metadata.additional_metadata
        assert stored_metadata.additional_metadata["original_text_length"] > 0
        assert stored_metadata.additional_metadata["processing_duration_ms"] >= 0

    @pytest.mark.asyncio
    async def test_concurrent_message_processing(self, mock_infrastructure):
        """Test concurrent processing of multiple messages."""
        messages = []
        for i in range(5):
            messages.append(
                {
                    "correlation_id": f"concurrent-test-{i}",
                    "source_system": "api_text",
                    "source_identifier": None,
                    "ingestion_timestamp_utc": "2024-01-01T12:00:00",
                    "raw_text": f"Concurrent message {i}",
                }
            )

        # Process messages concurrently
        async def process_async(message):
            return process_message(
                message,
                mock_infrastructure["es_client"],
                mock_infrastructure["rabbitmq_producer"],
            )

        tasks = [process_async(msg) for msg in messages]
        results = await asyncio.gather(*tasks)

        # Verify all messages processed successfully
        assert all(results)

        # Verify all messages were published and stored
        assert mock_infrastructure["rabbitmq_producer"].publish_standardized_message.call_count == 5
        assert mock_infrastructure["es_client"].index_metadata.call_count == 5
