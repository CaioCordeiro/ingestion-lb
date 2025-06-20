"""Pytest configuration and shared fixtures."""

import os
import sys
from unittest.mock import Mock

import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def mock_logger():
    """Mock logger fixture."""
    logger = Mock()
    logger.info = Mock()
    logger.warning = Mock()
    logger.error = Mock()
    logger.debug = Mock()
    return logger


@pytest.fixture
def mock_rabbitmq_producer():
    """Mock RabbitMQ producer fixture."""
    producer = Mock()
    producer.connect = Mock(return_value=True)
    producer.publish_message = Mock(return_value=True)
    producer.publish_standardized_message = Mock(return_value=True)
    producer.close = Mock()
    return producer


@pytest.fixture
def mock_rabbitmq_consumer():
    """Mock RabbitMQ consumer fixture."""
    consumer = Mock()
    consumer.connect = Mock(return_value=True)
    consumer.start_consuming = Mock()
    consumer.close = Mock()
    return consumer


@pytest.fixture
def mock_elasticsearch_client():
    """Mock Elasticsearch client fixture."""
    client = Mock()
    client.connect = Mock(return_value=True)
    client.index_metadata = Mock(return_value=True)
    client.close = Mock()
    return client


@pytest.fixture
def sample_correlation_id():
    """Sample correlation ID for testing."""
    return "test-correlation-123"
