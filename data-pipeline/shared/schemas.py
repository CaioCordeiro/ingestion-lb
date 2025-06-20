import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class StandardizedTextMessage(BaseModel):
    """
    Standardized message format for processed text data to be sent to output queue.
    """

    document_id: str = Field(
        ..., description="Unique document identifier for correlation with metadata"
    )
    correlation_id: str = Field(
        ..., description="Unique identifier for tracking the request flow"
    )
    source_system: str = Field(
        ..., description="Originating system (e.g., 'api_text', 'csv_upload')"
    )
    source_identifier: Optional[str] = Field(
        None, description="Optional identifier like filename or ID"
    )
    ingestion_timestamp_utc: datetime = Field(
        ..., description="UTC timestamp when data was ingested"
    )
    processing_timestamp_utc: datetime = Field(
        ..., description="UTC timestamp when data was processed"
    )
    processed_text: str = Field(..., description="The processed text content")


class TextMetadata(BaseModel):
    """
    Metadata model for storing in Elasticsearch (without the actual text).
    """

    document_id: str = Field(
        ..., description="Unique document identifier for correlation with text queue"
    )
    correlation_id: str = Field(
        ..., description="Unique identifier for tracking the request flow"
    )
    source_system: str = Field(
        ..., description="Originating system (e.g., 'api_text', 'csv_upload')"
    )
    source_identifier: Optional[str] = Field(
        None, description="Optional identifier like filename or ID"
    )
    ingestion_timestamp_utc: datetime = Field(
        ..., description="UTC timestamp when data was ingested"
    )
    processing_timestamp_utc: datetime = Field(
        ..., description="UTC timestamp when data was processed"
    )
    text_length: int = Field(..., description="Length of the processed text")
    processing_status: str = Field(
        ..., description="Status of processing (success/failed)"
    )
    error_message: Optional[str] = Field(
        None, description="Error message if processing failed"
    )
    additional_metadata: Optional[Dict[str, Any]] = Field(
        None, description="Additional metadata specific to source system"
    )


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
