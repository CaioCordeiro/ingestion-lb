from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class StandardizedTextMessage(BaseModel):
    """
    Standardized message format for processed text data.
    This model is used by the Processing Service to publish
    standardized messages to Elasticsearch.
    """
    correlation_id: str = Field(..., description="Unique identifier for tracking the request flow")
    source_system: str = Field(..., description="Originating system (e.g., 'api_text', 'csv_upload')")
    source_identifier: Optional[str] = Field(None, description="Optional identifier like filename or ID")
    ingestion_timestamp_utc: datetime = Field(..., description="UTC timestamp when data was ingested")
    processed_text: str = Field(..., description="The processed text content")

class RawTextMessage(BaseModel):
    """
    Raw text message format used by ingestion services to publish
    to the raw_text_queue in RabbitMQ.
    """
    correlation_id: str
    source_system: str
    source_identifier: Optional[str] = None
    ingestion_timestamp_utc: datetime
    raw_text: str
