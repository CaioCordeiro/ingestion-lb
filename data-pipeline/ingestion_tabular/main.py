import os
import sys
import uuid
from datetime import datetime
from io import StringIO

import pandas as pd
from fastapi import FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse

# Add parent directory to path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.logging_config import configure_logger
from shared.rabbitmq import RabbitMQProducer
from shared.schemas import RawTextMessage

# Configure logger
logger = configure_logger("ingestion_tabular")

# Create FastAPI app
app = FastAPI(
    title="Tabular Data Ingestion Service",
    description="Service for ingesting tabular data (CSV files)",
    version="1.0.0",
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
    logger=logger,
)


# Connect to RabbitMQ on startup
@app.on_event("startup")
async def startup_event():
    """Connect to RabbitMQ on startup."""
    rabbitmq_producer.connect()
    logger.info("Tabular Ingestion Service started")


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Close connections on shutdown."""
    rabbitmq_producer.close()
    logger.info("Tabular Ingestion Service shutting down")


# Routes
@app.post("/ingest")
async def ingest_csv(file: UploadFile = File(...), x_correlation_id: str = Header(None)):
    """
    Ingest CSV data, extract text from each row, and publish to RabbitMQ.

    Args:
        file: CSV file to ingest
        x_correlation_id: Correlation ID from request header

    Returns:
        JSON response with status and correlation ID
    """
    # Use provided correlation ID or generate a new one
    correlation_id = x_correlation_id or str(uuid.uuid4())

    logger.info(
        f"Processing CSV ingestion request: {file.filename}",
        extra={"correlation_id": correlation_id},
    )

    if not file.filename.lower().endswith(".csv"):
        logger.warning(
            f"Invalid file format: {file.filename}. Expected CSV.",
            extra={"correlation_id": correlation_id},
        )
        raise HTTPException(
            status_code=400, detail="Invalid file format. Please upload a CSV file."
        )

    try:
        # Read the CSV file
        contents = await file.read()

        # Convert to string for pandas
        s = contents.decode("utf-8")

        # Use pandas to read the CSV
        df = pd.read_csv(StringIO(s))

        # Track successful and failed messages
        successful_rows = 0
        failed_rows = 0

        # Process each row
        for index, row in df.iterrows():
            try:
                # Concatenate all columns into a single text string
                row_text = " ".join(str(value) for value in row.values if pd.notna(value))

                # Create a unique correlation ID for each row based on the main correlation ID
                row_correlation_id = f"{correlation_id}-row-{index}"

                # Create a raw text message
                message = RawTextMessage(
                    correlation_id=row_correlation_id,
                    source_system="csv_upload",
                    source_identifier=file.filename,
                    ingestion_timestamp_utc=datetime.utcnow(),
                    raw_text=row_text,
                )

                # Publish the message to RabbitMQ
                success = rabbitmq_producer.publish_message(message)

                if success:
                    successful_rows += 1
                else:
                    failed_rows += 1

            except Exception as e:
                logger.error(
                    f"Error processing row {index}: {str(e)}",
                    extra={"correlation_id": correlation_id},
                )
                failed_rows += 1

        # Log the results
        logger.info(
            f"CSV ingestion completed: {successful_rows} rows processed, {failed_rows} rows failed",
            extra={"correlation_id": correlation_id},
        )

        # Return response
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"CSV ingested: {successful_rows} rows processed, {failed_rows} rows failed",
                "correlation_id": correlation_id,
                "filename": file.filename,
            },
        )

    except pd.errors.EmptyDataError:
        logger.error(f"Empty CSV file: {file.filename}", extra={"correlation_id": correlation_id})
        raise HTTPException(status_code=400, detail="The CSV file is empty.")

    except pd.errors.ParserError:
        logger.error(
            f"Invalid CSV format: {file.filename}",
            extra={"correlation_id": correlation_id},
        )
        raise HTTPException(
            status_code=400,
            detail="Invalid CSV format. Please check the file and try again.",
        )

    except Exception as e:
        logger.error(
            f"Error processing CSV ingestion: {str(e)}",
            extra={"correlation_id": correlation_id},
        )
        raise HTTPException(status_code=500, detail=f"Error processing CSV ingestion: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "ingestion_tabular"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=True)
