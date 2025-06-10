from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import logging
import os
import sys
from datetime import datetime
import uuid

# Add parent directory to path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.logging_config import configure_logger
from shared.rabbitmq import RabbitMQProducer
from shared.schemas import RawTextMessage

# Configure logger
logger = configure_logger("ingestion_text")

# Create FastAPI app
app = FastAPI(
    title="Text Ingestion Service",
    description="Service for ingesting raw text data",
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
    logger.info("Text Ingestion Service started")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Close connections on shutdown."""
    rabbitmq_producer.close()
    logger.info("Text Ingestion Service shutting down")

# Request model
class TextRequest(BaseModel):
    text: str

# Routes
@app.post("/ingest")
async def ingest_text(
    request: TextRequest,
    x_correlation_id: str = Header(None)
):
    """
    Ingest raw text data and publish to RabbitMQ.
    
    Args:
        request: TextRequest containing the text to ingest
        x_correlation_id: Correlation ID from request header
        
    Returns:
        JSON response with status and correlation ID
    """
    # Use provided correlation ID or generate a new one
    correlation_id = x_correlation_id or str(uuid.uuid4())
    
    logger.info(
        "Processing text ingestion request",
        extra={"correlation_id": correlation_id}
    )
    
    try:
        # Create a raw text message
        message = RawTextMessage(
            correlation_id=correlation_id,
            source_system="api_text",
            source_identifier=None,
            ingestion_timestamp_utc=datetime.utcnow(),
            raw_text=request.text
        )
        
        # Publish the message to RabbitMQ
        success = rabbitmq_producer.publish_message(message)
        
        if success:
            logger.info(
                "Text ingestion successful",
                extra={"correlation_id": correlation_id}
            )
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "Text ingested successfully",
                    "correlation_id": correlation_id
                }
            )
        else:
            logger.error(
                "Failed to publish message to RabbitMQ",
                extra={"correlation_id": correlation_id}
            )
            raise HTTPException(
                status_code=500,
                detail="Failed to process text ingestion"
            )
            
    except Exception as e:
        logger.error(
            f"Error processing text ingestion: {str(e)}",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(
            status_code=500,
            detail=f"Error processing text ingestion: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "ingestion_text"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
