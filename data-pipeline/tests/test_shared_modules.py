"""Tests for shared modules."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from shared.elasticsearch_client import ElasticsearchClient
from shared.logging_config import configure_logger
from shared.rabbitmq import RabbitMQConsumer, RabbitMQProducer
from shared.schemas import RawTextMessage, StandardizedTextMessage, TextMetadata


class TestSchemas:
    """Test class for shared schemas."""

    def test_raw_text_message_creation(self):
        """Test RawTextMessage creation."""
        message = RawTextMessage(
            correlation_id="test-123",
            source_system="api_text",
            source_identifier="test-source",
            ingestion_timestamp_utc=datetime.utcnow(),
            raw_text="Test message",
        )

        assert message.correlation_id == "test-123"
        assert message.source_system == "api_text"
        assert message.raw_text == "Test message"

    def test_standardized_text_message_creation(self):
        """Test StandardizedTextMessage creation."""
        now = datetime.utcnow()
        message = StandardizedTextMessage(
            document_id="doc-123",
            correlation_id="test-123",
            source_system="api_text",
            source_identifier="test-source",
            ingestion_timestamp_utc=now,
            processing_timestamp_utc=now,
            processed_text="processed text",
        )

        assert message.document_id == "doc-123"
        assert message.processed_text == "processed text"

    def test_text_metadata_creation(self):
        """Test TextMetadata creation."""
        now = datetime.utcnow()
        metadata = TextMetadata(
            document_id="doc-123",
            correlation_id="test-123",
            source_system="api_text",
            source_identifier="test-source",
            ingestion_timestamp_utc=now,
            processing_timestamp_utc=now,
            text_length=100,
            processing_status="success",
            error_message=None,
            additional_metadata={"key": "value"},
        )

        assert metadata.document_id == "doc-123"
        assert metadata.text_length == 100
        assert metadata.processing_status == "success"


class TestRabbitMQProducer:
    """Test class for RabbitMQ Producer."""

    @pytest.fixture
    def mock_pika(self):
        """Mock pika module."""
        with patch("shared.rabbitmq.pika") as mock:
            yield mock

    @pytest.fixture
    def producer(self, mock_logger):
        """RabbitMQ producer fixture."""
        return RabbitMQProducer(logger=mock_logger)

    def test_producer_initialization(self, producer):
        """Test producer initialization."""
        assert producer.host == "rabbitmq"
        assert producer.port == 5672
        assert producer.queue_name == "raw_text_queue"

    def test_connect_success(self, producer, mock_pika):
        """Test successful connection."""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.is_open = True
        mock_pika.BlockingConnection.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        result = producer.connect()

        assert result is True
        assert producer.connection == mock_connection
        assert producer.channel == mock_channel

    def test_publish_message_success(self, producer, mock_pika):
        """Test successful message publishing."""
        # Setup connection
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.is_open = True
        producer.connection = mock_connection
        producer.channel = mock_channel

        message = RawTextMessage(
            correlation_id="test-123",
            source_system="api_text",
            source_identifier=None,
            ingestion_timestamp_utc=datetime.utcnow(),
            raw_text="Test message",
        )

        result = producer.publish_message(message)

        assert result is True
        mock_channel.basic_publish.assert_called_once()

    def test_publish_standardized_message_success(self, producer):
        """Test successful standardized message publishing."""
        # Setup connection
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.is_open = True
        producer.connection = mock_connection
        producer.channel = mock_channel

        message = StandardizedTextMessage(
            document_id="doc-123",
            correlation_id="test-123",
            source_system="api_text",
            source_identifier=None,
            ingestion_timestamp_utc=datetime.utcnow(),
            processing_timestamp_utc=datetime.utcnow(),
            processed_text="Processed text",
        )

        result = producer.publish_standardized_message(message)

        assert result is True
        mock_channel.basic_publish.assert_called_once()

    def test_close_connection(self, producer):
        """Test closing connection."""
        mock_connection = Mock()
        mock_connection.is_open = True
        producer.connection = mock_connection

        producer.close()

        mock_connection.close.assert_called_once()


class TestRabbitMQConsumer:
    """Test class for RabbitMQ Consumer."""

    @pytest.fixture
    def mock_pika(self):
        """Mock pika module."""
        with patch("shared.rabbitmq.pika") as mock:
            yield mock

    @pytest.fixture
    def consumer(self, mock_logger):
        """RabbitMQ consumer fixture."""
        return RabbitMQConsumer(logger=mock_logger)

    def test_consumer_initialization(self, consumer):
        """Test consumer initialization."""
        assert consumer.host == "rabbitmq"
        assert consumer.port == 5672
        assert consumer.queue_name == "raw_text_queue"

    def test_connect_success(self, consumer, mock_pika):
        """Test successful connection."""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.is_open = True
        mock_pika.BlockingConnection.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        result = consumer.connect()

        assert result is True
        assert consumer.connection == mock_connection
        assert consumer.channel == mock_channel

    def test_start_consuming_success(self, consumer):
        """Test successful message consumption."""
        # Setup connection
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.is_open = True
        consumer.connection = mock_connection
        consumer.channel = mock_channel

        # Mock start_consuming to raise KeyboardInterrupt (simulating user stop)
        mock_channel.start_consuming.side_effect = KeyboardInterrupt()

        callback = Mock(return_value=True)

        # Should not raise exception
        consumer.start_consuming(callback)

        mock_channel.basic_consume.assert_called_once()


class TestElasticsearchClient:
    """Test class for Elasticsearch Client."""

    @pytest.fixture
    def mock_elasticsearch(self):
        """Mock Elasticsearch module."""
        with patch("shared.elasticsearch_client.Elasticsearch") as mock:
            yield mock

    @pytest.fixture
    def es_client(self, mock_logger):
        """Elasticsearch client fixture."""
        return ElasticsearchClient(logger=mock_logger)

    def test_client_initialization(self, es_client):
        """Test client initialization."""
        assert es_client.hosts == ["http://elasticsearch:9200"]
        assert es_client.username == "elastic"
        assert es_client.index_name == "text_metadata"

    def test_connect_success(self, es_client, mock_elasticsearch):
        """Test successful connection."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.indices.exists.return_value = True
        mock_elasticsearch.return_value = mock_client

        result = es_client.connect()

        assert result is True
        assert es_client.client == mock_client

    def test_connect_failure(self, es_client, mock_elasticsearch):
        """Test connection failure."""
        mock_elasticsearch.side_effect = Exception("Connection failed")

        result = es_client.connect(max_retries=1, retry_delay=0)

        assert result is False

    def test_connect_creates_index(self, es_client, mock_elasticsearch):
        """Test that connect creates index if it doesn't exist."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.indices.exists.return_value = False
        mock_elasticsearch.return_value = mock_client

        result = es_client.connect()

        assert result is True
        mock_client.indices.create.assert_called_once()

    def test_index_metadata_success(self, es_client):
        """Test successful metadata indexing."""
        # Setup client
        mock_client = Mock()
        mock_client.index.return_value = {"_id": "doc-123"}
        es_client.client = mock_client

        metadata = TextMetadata(
            document_id="doc-123",
            correlation_id="test-123",
            source_system="api_text",
            source_identifier=None,
            ingestion_timestamp_utc=datetime.utcnow(),
            processing_timestamp_utc=datetime.utcnow(),
            text_length=100,
            processing_status="success",
            error_message=None,
        )

        result = es_client.index_metadata(metadata)

        assert result is True
        mock_client.index.assert_called_once()

    def test_index_metadata_no_client(self, es_client, mock_elasticsearch):
        """Test indexing without client connection."""
        es_client.client = None

        # Mock connect to fail
        mock_elasticsearch.side_effect = Exception("Connection failed")

        metadata = TextMetadata(
            document_id="doc-123",
            correlation_id="test-123",
            source_system="api_text",
            source_identifier=None,
            ingestion_timestamp_utc=datetime.utcnow(),
            processing_timestamp_utc=datetime.utcnow(),
            text_length=100,
            processing_status="success",
            error_message=None,
        )

        result = es_client.index_metadata(metadata)

        assert result is False

    def test_close_client(self, es_client):
        """Test closing client."""
        mock_client = Mock()
        es_client.client = mock_client

        es_client.close()

        mock_client.close.assert_called_once()


class TestLoggingConfig:
    """Test class for logging configuration."""

    def test_configure_logger(self):
        """Test logger configuration."""
        logger = configure_logger("test_service")

        assert logger.name == "test_service"
        assert len(logger.handlers) > 0
        assert len(logger.filters) > 0

    def test_logger_service_filter(self):
        """Test that service filter adds service name."""
        logger = configure_logger("test_service")

        # Create a test log record
        record = logger.makeRecord(
            name="test_service",
            level=20,  # INFO
            fn="test.py",
            lno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        # Apply filters
        for filter_obj in logger.filters:
            filter_obj.filter(record)

        assert hasattr(record, "service")
        assert record.service == "test_service"
        assert hasattr(record, "correlation_id")
