import logging
import time
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch
from elasticsearch import exceptions as es_exceptions

from .schemas import TextMetadata


class ElasticsearchClient:
    """
    Client for interacting with Elasticsearch.
    Handles connection, indexing, and error handling.
    """

    def __init__(
        self,
        hosts: List[str] = ["http://elasticsearch:9200"],
        username: str = "elastic",
        password: str = "changeme",
        index_name: str = "text_metadata",
        logger: Optional[logging.Logger] = None,
    ):
        self.hosts = hosts
        self.username = username
        self.password = password
        self.index_name = index_name
        self.logger = logger or logging.getLogger(__name__)
        self.client = None

    def connect(self, max_retries: int = 5, retry_delay: int = 5) -> bool:
        """
        Connect to Elasticsearch with retry logic.

        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds

        Returns:
            bool: True if connection successful, False otherwise
        """
        retry_count = 0

        while retry_count < max_retries:
            try:
                self.client = Elasticsearch(
                    self.hosts,
                    basic_auth=(self.username, self.password),
                    verify_certs=False,
                    request_timeout=30,
                )

                # Check if connection is successful
                if self.client.ping():
                    self.logger.info(f"Connected to Elasticsearch at {self.hosts}")

                    # Create index if it doesn't exist
                    if not self.client.indices.exists(index=self.index_name):
                        self._create_index()

                    return True
                else:
                    raise Exception("Elasticsearch ping failed")

            except Exception as e:
                retry_count += 1
                self.logger.warning(
                    f"Failed to connect to Elasticsearch (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                else:
                    self.logger.error(
                        "Max retries reached. Could not connect to Elasticsearch."
                    )
                    return False

    def _create_index(self) -> None:
        """Create the index with appropriate mappings."""
        try:
            # Define index mappings
            mappings = {
                "mappings": {
                    "properties": {
                        "document_id": {"type": "keyword"},
                        "correlation_id": {"type": "keyword"},
                        "source_system": {"type": "keyword"},
                        "source_identifier": {"type": "keyword"},
                        "ingestion_timestamp_utc": {"type": "date"},
                        "processing_timestamp_utc": {"type": "date"},
                        "text_length": {"type": "integer"},
                        "processing_status": {"type": "keyword"},
                        "error_message": {"type": "text"},
                        "additional_metadata": {"type": "object", "enabled": False},
                    }
                },
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            }

            # Create the index
            self.client.indices.create(index=self.index_name, body=mappings)
            self.logger.info(f"Created index: {self.index_name}")

        except es_exceptions.RequestError as e:
            # Index might have been created by another instance
            if "resource_already_exists_exception" in str(e):
                self.logger.info(f"Index {self.index_name} already exists")
            else:
                raise

    def index_metadata(self, metadata: TextMetadata) -> bool:
        """
        Index metadata in Elasticsearch.

        Args:
            metadata: The TextMetadata to index

        Returns:
            bool: True if indexing was successful, False otherwise
        """
        if not self.client:
            if not self.connect():
                return False

        try:
            # Convert metadata to dict
            doc_dict = metadata.model_dump()

            # Index the metadata
            response = self.client.index(
                index=self.index_name, document=doc_dict, id=metadata.document_id
            )

            self.logger.info(
                f"Indexed metadata with ID: {response['_id']}",
                extra={
                    "correlation_id": metadata.correlation_id,
                    "document_id": metadata.document_id,
                },
            )
            return True

        except es_exceptions.ConnectionError:
            self.logger.error(
                "Lost connection to Elasticsearch. Attempting to reconnect...",
                extra={"correlation_id": metadata.correlation_id},
            )
            # Try to reconnect
            self.connect()
            return False

        except Exception as e:
            self.logger.error(
                f"Failed to index metadata: {str(e)}",
                extra={"correlation_id": metadata.correlation_id},
            )
            return False

    def close(self):
        """Close the Elasticsearch client."""
        if self.client:
            self.client.close()
            self.logger.info("Closed Elasticsearch connection")
