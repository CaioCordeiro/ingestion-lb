import json
import logging
import os
import sys
import time
from datetime import datetime
import re
from typing import Dict, Any

# Add parent directory to path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.logging_config import configure_logger
from shared.rabbitmq import RabbitMQConsumer
from shared.elasticsearch_client import ElasticsearchClient
from shared.schemas import StandardizedTextMessage

# Configure logger
logger = configure_logger("processing_service")

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "raw_text_queue")
RABBITMQ_DLQ = os.getenv("RABBITMQ_DLQ", "dead_letter_queue")
logger.info(f"RabbitMQ DLQ: {RABBITMQ_DLQ}")
# Elasticsearch configuration
ES_HOSTS = os.getenv("ES_HOSTS", "http://elasticsearch:9200").split(",")
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "changeme")
ES_INDEX = os.getenv("ES_INDEX", "processed_texts")

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
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Remove special characters (optional, commented out)
    # text = re.sub(r'[^\w\s]', '', text)
    
    return text

def process_message(message_data: Dict[str, Any], client: ElasticsearchClient) -> bool:
    """
    Process a message from RabbitMQ.
    
    Args:
        message_data: Message data from RabbitMQ
        
    Returns:
        bool: True if processing was successful, False otherwise
    """
    try:
        # Extract correlation ID for logging
        correlation_id = message_data.get("correlation_id", "unknown")
        
        logger.info(
            "Processing message",
            extra={"correlation_id": correlation_id}
        )
        
        # Extract raw text
        raw_text = message_data.get("raw_text", "")
        
        if not raw_text:
            logger.warning(
                "Empty text content in message",
                extra={"correlation_id": correlation_id}
            )
            return False
        
        # Clean the text
        processed_text = clean_text(raw_text)
        
        # Create a standardized message
        standardized_message = StandardizedTextMessage(
            correlation_id=correlation_id,
            source_system=message_data.get("source_system", "unknown"),
            source_identifier=message_data.get("source_identifier"),
            ingestion_timestamp_utc=datetime.fromisoformat(message_data.get("ingestion_timestamp_utc")),
            processed_text=processed_text
        )
        
        # Index the message in Elasticsearch
        success = client.index_document(standardized_message)
        
        if success:
            logger.info(
                "Message processed and indexed successfully",
                extra={"correlation_id": correlation_id}
            )
            return True
        else:
            logger.error(
                "Failed to index message in Elasticsearch",
                extra={"correlation_id": correlation_id}
            )
            return False
            
    except KeyError as e:
        logger.error(
            f"Missing required field in message: {str(e)}",
            extra={"correlation_id": message_data.get("correlation_id", "unknown")}
        )
        return False
        
    except Exception as e:
        logger.error(
            f"Error processing message: {str(e)}",
            extra={"correlation_id": message_data.get("correlation_id", "unknown")}
        )
        return False

def main(client: ElasticsearchClient):
    """Main function to run the processing service."""
    logger.info("Starting Processing Service")
    
    # Connect to Elasticsearch
    if not client.connect():
        logger.error("Failed to connect to Elasticsearch. Exiting.")
        sys.exit(1)
    
    # Connect to RabbitMQ and start consuming messages
    rabbitmq_consumer = RabbitMQConsumer(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        username=RABBITMQ_USER,
        password=RABBITMQ_PASS,
        queue_name=RABBITMQ_QUEUE,
        dead_letter_queue=RABBITMQ_DLQ,
        logger=logger
    )
    
    if not rabbitmq_consumer.connect():
        logger.error("Failed to connect to RabbitMQ. Exiting.")
        sys.exit(1)
    
    # Create a wrapper function that includes the Elasticsearch client
    def message_callback(message_data: Dict[str, Any]) -> bool:
        # Log the raw message data
        logger.info(f"Received message: {message_data}")
        return process_message(message_data, client)
    
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
        client.close()
        logger.info("Processing Service shut down")

if __name__ == "__main__":
    # Create Elasticsearch client
    es_client = ElasticsearchClient(
        hosts=ES_HOSTS,
        username=ES_USER,
        password=ES_PASS,
        index_name=ES_INDEX,
        logger=logger
    )
    
    # Run the main function
    main(es_client)
