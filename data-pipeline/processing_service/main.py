import os
import re
import sys
import uuid
from datetime import datetime
from typing import Any, Dict

# Add parent directory to path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.elasticsearch_client import ElasticsearchClient
from shared.logging_config import configure_logger
from shared.rabbitmq import RabbitMQConsumer, RabbitMQProducer
from shared.schemas import StandardizedTextMessage, TextMetadata

# Configure logger
logger = configure_logger("processing_service")

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "raw_text_queue")
RABBITMQ_DLQ = os.getenv("RABBITMQ_DLQ", "dead_letter_queue")
RABBITMQ_OUTPUT_QUEUE = os.getenv("RABBITMQ_OUTPUT_QUEUE", "processed_text_queue")
logger.info(f"RabbitMQ DLQ: {RABBITMQ_DLQ}")
# Elasticsearch configuration
ES_HOSTS = os.getenv("ES_HOSTS", "http://elasticsearch:9200").split(",")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "changeme")
ES_INDEX = os.getenv("ES_INDEX", "text_metadata")


def clean_text(text: str) -> str:
    """
    Clean and normalize text.

    Args:
        text: Raw text to clean

    Returns:
        Cleaned text
    """
    if not text:
        return ""

    # Convert to lowercase
    text = text.lower()

    # Remove extra whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Remove special characters (optional, commented out)
    # text = re.sub(r'[^\w\s]', '', text)

    return text


def process_message(
    message_data: Dict[str, Any],
    es_client: ElasticsearchClient,
    rabbitmq_producer: RabbitMQProducer,
) -> bool:
    """
    Process a message from RabbitMQ.

    Args:
        message_data: Message data from RabbitMQ

    Returns:
        bool: True if processing was successful, False otherwise
    """
    document_id = str(uuid.uuid4())
    try:
        # Extract correlation ID for logging
        correlation_id = message_data.get("correlation_id", "unknown")

        logger.info("Processing message", extra={"correlation_id": correlation_id})

        # Extract raw text
        raw_text = message_data.get("raw_text", "")
        if not isinstance(message_data.get("ingestion_timestamp_utc"), str):
            # Create metadata for failed processing
            metadata = TextMetadata(
                document_id=document_id,
                correlation_id=correlation_id,
                source_system=message_data.get("source_system", "unknown"),
                source_identifier=message_data.get("source_identifier"),
                ingestion_timestamp_utc=datetime.utcnow(),
                processing_timestamp_utc=datetime.utcnow(),
                text_length=0,
                processing_status="failed",
                error_message="No valid ingestion timestamp found in message",
                additional_metadata={"original_message": message_data},
            )

            # Store metadata in Elasticsearch
            es_client.index_metadata(metadata)

            logger.warning(
                "No valid ingestion timestamp found in message",
                extra={"correlation_id": correlation_id},
            )
            return False
        if not raw_text:
            # Create metadata for failed processing
            metadata = TextMetadata(
                document_id=document_id,
                correlation_id=correlation_id,
                source_system=message_data.get("source_system", "unknown"),
                source_identifier=message_data.get("source_identifier"),
                ingestion_timestamp_utc=datetime.fromisoformat(
                    message_data.get("ingestion_timestamp_utc")
                ),
                processing_timestamp_utc=datetime.utcnow(),
                text_length=0,
                processing_status="failed",
                error_message="Empty text content in message",
                additional_metadata={"original_message": message_data},
            )

            # Store metadata in Elasticsearch
            es_client.index_metadata(metadata)

            logger.warning(
                "Empty text content in message",
                extra={"correlation_id": correlation_id},
            )
            return False

        # Clean the text
        processed_text = clean_text(raw_text)
        processing_timestamp = datetime.utcnow()

        # Create a standardized message
        standardized_message = StandardizedTextMessage(
            document_id=document_id,
            correlation_id=correlation_id,
            source_system=message_data.get("source_system", "unknown"),
            source_identifier=message_data.get("source_identifier"),
            ingestion_timestamp_utc=datetime.fromisoformat(
                message_data.get("ingestion_timestamp_utc")
            ),
            processing_timestamp_utc=processing_timestamp,
            processed_text=processed_text,
        )

        # Create metadata for Elasticsearch
        metadata = TextMetadata(
            document_id=document_id,
            correlation_id=correlation_id,
            source_system=message_data.get("source_system", "unknown"),
            source_identifier=message_data.get("source_identifier"),
            ingestion_timestamp_utc=datetime.fromisoformat(
                message_data.get("ingestion_timestamp_utc")
            ),
            processing_timestamp_utc=processing_timestamp,
            text_length=len(processed_text),
            processing_status="success",
            error_message=None,
            additional_metadata={
                "original_text_length": len(raw_text),
                "processing_duration_ms": int(
                    (
                        processing_timestamp
                        - datetime.fromisoformat(message_data.get("ingestion_timestamp_utc"))
                    ).total_seconds()
                    * 1000
                ),
            },
        )

        # Publish processed text to output queue
        text_published = rabbitmq_producer.publish_standardized_message(standardized_message)

        # Store metadata in Elasticsearch
        metadata_stored = es_client.index_metadata(metadata)

        if text_published and metadata_stored:
            logger.info(
                "Message processed, published to output queue, and metadata stored successfully",
                extra={"correlation_id": correlation_id, "document_id": document_id},
            )
            return True
        else:
            # Update metadata to reflect partial failure
            if not text_published:
                metadata.processing_status = "failed"
                metadata.error_message = "Failed to publish to output queue"
            elif not metadata_stored:
                metadata.processing_status = "partial_success"
                metadata.error_message = "Failed to store metadata in Elasticsearch"

            # Try to store updated metadata
            es_client.index_metadata(metadata)

            logger.error(
                f"Failed to fully process message - text_published: {text_published}, metadata_stored: {metadata_stored}",
                extra={"correlation_id": correlation_id, "document_id": document_id},
            )
            return False

    except KeyError as e:
        # Create metadata for failed processing
        metadata = TextMetadata(
            document_id=document_id,
            correlation_id=message_data.get("correlation_id", "unknown"),
            source_system=message_data.get("source_system", "unknown"),
            source_identifier=message_data.get("source_identifier"),
            ingestion_timestamp_utc=datetime.fromisoformat(
                message_data.get("ingestion_timestamp_utc", datetime.utcnow().isoformat())
            ),
            processing_timestamp_utc=datetime.utcnow(),
            text_length=0,
            processing_status="failed",
            error_message=f"Missing required field: {str(e)}",
            additional_metadata={"original_message": message_data},
        )

        # Store metadata in Elasticsearch
        es_client.index_metadata(metadata)

        logger.error(
            f"Missing required field in message: {str(e)}",
            extra={"correlation_id": message_data.get("correlation_id", "unknown")},
        )
        return False

    except Exception as e:
        # Create metadata for failed processing
        try:
            metadata = TextMetadata(
                document_id=document_id,
                correlation_id=message_data.get("correlation_id", "unknown"),
                source_system=message_data.get("source_system", "unknown"),
                source_identifier=message_data.get("source_identifier"),
                ingestion_timestamp_utc=datetime.fromisoformat(
                    message_data.get("ingestion_timestamp_utc", datetime.utcnow().isoformat())
                ),
                processing_timestamp_utc=datetime.utcnow(),
                text_length=len(message_data.get("raw_text", "")),
                processing_status="failed",
                error_message=str(e),
                additional_metadata={"original_message": message_data},
            )

            # Store metadata in Elasticsearch
            es_client.index_metadata(metadata)
        except Exception as meta_error:
            logger.error(f"Failed to store error metadata: {str(meta_error)}")

        logger.error(
            f"Error processing message: {str(e)}",
            extra={
                "correlation_id": message_data.get("correlation_id", "unknown"),
                "document_id": document_id,
            },
        )
        return False


def main(es_client: ElasticsearchClient):
    """Main function to run the processing service."""
    logger.info("Starting Processing Service")

    # Connect to Elasticsearch
    if not es_client.connect():
        logger.error("Failed to connect to Elasticsearch. Exiting.")
        sys.exit(1)

    # Create RabbitMQ producer for output queue
    rabbitmq_producer = RabbitMQProducer(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        username=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        queue_name=RABBITMQ_QUEUE,
        output_queue_name=RABBITMQ_OUTPUT_QUEUE,
        logger=logger,
    )

    if not rabbitmq_producer.connect():
        logger.error("Failed to connect to RabbitMQ producer. Exiting.")
        sys.exit(1)

    # Connect to RabbitMQ and start consuming messages
    rabbitmq_consumer = RabbitMQConsumer(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        username=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        queue_name=RABBITMQ_QUEUE,
        dead_letter_queue=RABBITMQ_DLQ,
        logger=logger,
    )

    if not rabbitmq_consumer.connect():
        logger.error("Failed to connect to RabbitMQ. Exiting.")
        sys.exit(1)

    # Create a wrapper function that includes the clients
    def message_callback(message_data: Dict[str, Any]) -> bool:
        # Log the raw message data
        logger.info(f"Received message: {message_data}")
        return process_message(message_data, es_client, rabbitmq_producer)

    try:
        # Start consuming messages
        logger.info(f"Consuming messages from queue: {RABBITMQ_QUEUE}")
        rabbitmq_consumer.start_consuming(message_callback)

    except KeyboardInterrupt:
        logger.info("Processing Service stopped by user")

    except Exception as e:
        logger.error(f"Processing Service stopped due to error: {str(e)}")

    finally:
        # Close connections
        rabbitmq_consumer.close()
        rabbitmq_producer.close()
        es_client.close()
        logger.info("Processing Service shut down")


if __name__ == "__main__":
    # Create Elasticsearch client
    es_client = ElasticsearchClient(
        hosts=ES_HOSTS,
        username=ES_USER,
        password=ES_PASS,
        index_name=ES_INDEX,
        logger=logger,
    )

    # Run the main function
    main(es_client)
