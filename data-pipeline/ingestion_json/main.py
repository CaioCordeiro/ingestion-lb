from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
import logging
import os
import sys
from datetime import datetime
import uuid
import json
from typing import Any, Dict, List, Union

# Add parent directory to path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.logging_config import configure_logger
from shared.rabbitmq import RabbitMQProducer
from shared.schemas import RawTextMessage

# Configure logger
logger = configure_logger("ingestion_json")

# Create FastAPI app
app = FastAPI(
    title="JSON Ingestion Service",
    description="Service for ingesting JSON data",
    version="1.0.0"
)

# RabbitMQ configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "raw_text_queue")

# Create RabbitMQ producer
rabbitmq_producer = RabbitMQProducer(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    username=RABBITMQ_USER,
    password=RABBITMQ_PASS,
    queue_name=RABBITMQ_QUEUE,
    logger=logger
)

# Connect to RabbitMQ on startup
@app.on_event("startup")
async def startup_event():
    """Connect to RabbitMQ on startup."""
    rabbitmq_producer.connect()
    logger.info("JSON Ingestion Service started")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Close connections on shutdown."""
    rabbitmq_producer.close()
    logger.info("JSON Ingestion Service shutting down")

def extract_text_from_json(
    json_data: Union[Dict[str, Any], List[Any], str, int, float, bool, None],
    text_values: List[str] = None
) -> List[str]:
    """
    Recursively extract all string values from a JSON object.
    
    Args:
        json_data: JSON data to extract text from
        text_values: List to accumulate text values (used in recursion)
        
    Returns:
        List of extracted string values
    """
    if text_values is None:
        text_values = []
    
    if isinstance(json_data, dict):
        # Process dictionary
        for key, value in json_data.items():
            # Add the key if it's a string
            if isinstance(key, str) and key.strip():
                text_values.append(key)
            
            # Recursively process the value
            extract_text_from_json(value, text_values)
            
    elif isinstance(json_data, list):
        # Process list
        for item in json_data:
            extract_text_from_json(item, text_values)
            
    elif isinstance(json_data, str) and json_data.strip():
        # Add non-empty string values
        text_values.append(json_data)
    
    return text_values

# Routes
@app.post("/ingest")
async def ingest_json(
    request: Request,
    x_correlation_id: str = Header(None)
):
    """
    Ingest JSON data, extract text, and publish to RabbitMQ.
    
    Args:
        request: Request containing JSON data
        x_correlation_id: Correlation ID from request header
        
    Returns:
        JSON response with status and correlation ID
    """
    # Use provided correlation ID or generate a new one
    correlation_id = x_correlation_id or str(uuid.uuid4())
    
    logger.info(
        "Processing JSON ingestion request",
        extra={"correlation_id": correlation_id}
    )
    
    try:
        # Parse the JSON data
        json_data = await request.json()
        
        # Extract text from JSON
        text_values = extract_text_from_json(json_data)
        
        # Concatenate all text values
        combined_text = " ".join(text_values)
        
        if not combined_text.strip():
            logger.warning(
                "No text content found in JSON data",
                extra={"correlation_id": correlation_id}
            )
            return JSONResponse(
                status_code=200,
                content={
                    "status": "warning",
                    "message": "No text content found in JSON data",
                    "correlation_id": correlation_id
                }
            )
        
        # Create a raw text message
        message = RawTextMessage(
            correlation_id=correlation_id,
            source_system="json_upload",
            source_identifier=None,
            ingestion_timestamp_utc=datetime.utcnow(),
            raw_text=combined_text
        )
        
        # Publish the message to RabbitMQ
        success = rabbitmq_producer.publish_message(message)
        
        if success:
            logger.info(
                "JSON ingestion successful",
                extra={
                    "correlation_id": correlation_id,
                    "text_length": len(combined_text),
                    "extracted_fields": len(text_values)
                }
            )
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "JSON ingested successfully",
                    "correlation_id": correlation_id,
                    "extracted_fields": len(text_values)
                }
            )
        else:
            logger.error(
                "Failed to publish message to RabbitMQ",
                extra={"correlation_id": correlation_id}
            )
            raise HTTPException(
                status_code=500,
                detail="Failed to process JSON ingestion"
            )
            
    except json.JSONDecodeError:
        logger.error(
            "Invalid JSON format",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON format. Please check the request body."
        )
        
    except Exception as e:
        logger.error(
            f"Error processing JSON ingestion: {str(e)}",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=500,
            detail=f"Error processing JSON ingestion: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "ingestion_json"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8003, reload=True)
