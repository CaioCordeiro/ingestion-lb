import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Optional

import pika
from pika.exceptions import AMQPConnectionError

from .schemas import RawTextMessage, StandardizedTextMessage


class RabbitMQProducer:
    """
    A RabbitMQ producer client for publishing messages to a queue.
    Implements connection retry logic and proper channel management.
    """

    def __init__(
        self,
        host: str = "rabbitmq",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        queue_name: str = "raw_text_queue",
        output_queue_name: str = "processed_text_queue",
        logger: Optional[logging.Logger] = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.output_queue_name = output_queue_name
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        self.channel = None

    def connect(self, max_retries: int = 5, retry_delay: int = 5) -> bool:
        """
        Establish connection to RabbitMQ with retry logic.

        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds

        Returns:
            bool: True if connection successful, False otherwise
        """
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Close existing connection if any
                if self.connection and self.connection.is_open:
                    self.connection.close()

                # Create new connection
                credentials = pika.PlainCredentials(self.username, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )

                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()

                # Declare the dead letter exchange
                self.channel.exchange_declare(
                    exchange="dead_letter_exchange",
                    exchange_type="direct",
                    durable=True,
                )

                # Declare the dead letter queue
                self.channel.queue_declare(queue="dead_letter_queue", durable=True)

                # Declare the main queue with dead letter configuration
                try:
                    # Try to declare the queue with dead letter configuration
                    self.channel.queue_declare(
                        queue=self.queue_name,
                        durable=True,
                        arguments={
                            "x-dead-letter-exchange": "dead_letter_exchange",
                            "x-dead-letter-routing-key": "dead_letter_queue",
                        },
                    )
                except Exception as e:
                    # If queue already exists without dead letter config, just declare it normally
                    self.logger.warning(
                        f"Queue {self.queue_name} exists without dead letter config, declaring normally: {str(e)}"
                    )
                    self.channel.queue_declare(queue=self.queue_name, durable=True)

                # Declare the output queue for processed text
                try:
                    self.channel.queue_declare(
                        queue=self.output_queue_name,
                        durable=True,
                        arguments={
                            "x-dead-letter-exchange": "dead_letter_exchange",
                            "x-dead-letter-routing-key": "dead_letter_queue",
                        },
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Output queue {self.output_queue_name} exists, declaring normally: {str(e)}"
                    )
                    self.channel.queue_declare(queue=self.output_queue_name, durable=True)

                self.logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
                return True

            except AMQPConnectionError as e:
                retry_count += 1
                self.logger.warning(
                    f"Failed to connect to RabbitMQ (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Max retries reached. Could not connect to RabbitMQ.")
                    return False

    def publish_message(self, message: RawTextMessage, queue_name: Optional[str] = None) -> bool:
        """
        Publish a message to the queue.

        Args:
            message: The RawTextMessage to publish

        Returns:
            bool: True if successful, False otherwise
        """
        target_queue = queue_name or self.queue_name
        if not self.connection or not self.channel or not self.connection.is_open:
            if not self.connect():
                return False

        try:
            # Convert message to JSON
            message_json = message.model_dump_json()

            # Publish the message
            self.channel.basic_publish(
                exchange="",
                routing_key=target_queue,
                body=message_json,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type="application/json",
                    message_id=str(uuid.uuid4()),
                    timestamp=int(datetime.now().timestamp()),
                    headers={"correlation_id": message.correlation_id},
                ),
            )

            self.logger.info(
                f"Published message to {target_queue}",
                extra={"correlation_id": message.correlation_id},
            )
            return True

        except Exception as e:
            self.logger.error(
                f"Failed to publish message: {str(e)}",
                extra={"correlation_id": message.correlation_id},
            )
            # Try to reconnect for next message
            self.connect()
            return False

    def publish_standardized_message(self, message: StandardizedTextMessage) -> bool:
        """
        Publish a standardized message to the output queue.

        Args:
            message: The StandardizedTextMessage to publish

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.connection or not self.channel or not self.connection.is_open:
            if not self.connect():
                return False

        try:
            # Convert message to JSON
            message_json = message.model_dump_json()

            # Publish the message to output queue
            self.channel.basic_publish(
                exchange="",
                routing_key=self.output_queue_name,
                body=message_json,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type="application/json",
                    message_id=str(uuid.uuid4()),
                    timestamp=int(datetime.now().timestamp()),
                    headers={
                        "correlation_id": message.correlation_id,
                        "document_id": message.document_id,
                    },
                ),
            )

            self.logger.info(
                f"Published standardized message to {self.output_queue_name}",
                extra={
                    "correlation_id": message.correlation_id,
                    "document_id": message.document_id,
                },
            )
            return True

        except Exception as e:
            self.logger.error(
                "Failed to publish standardized message: %s",
                {str(e)},
                extra={"correlation_id": message.correlation_id},
            )
            # Try to reconnect for next message
            self.connect()
            return False

    def close(self):
        """Close the connection to RabbitMQ."""
        if self.connection and self.connection.is_open:
            self.connection.close()
            self.logger.info("Closed RabbitMQ connection")


class RabbitMQConsumer:
    """
    A RabbitMQ consumer client for consuming messages from a queue.
    Implements connection retry logic, channel management, and
    message acknowledgment.
    """

    def __init__(
        self,
        host: str = "rabbitmq",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        queue_name: str = "raw_text_queue",
        dead_letter_queue: str = "dead_letter_queue",  # Make sure this matches the producer
        logger: Optional[logging.Logger] = None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.dead_letter_queue = dead_letter_queue
        self.logger = logger or logging.getLogger(__name__)
        self.connection = None
        self.channel = None

    def connect(self, max_retries: int = 5, retry_delay: int = 5) -> bool:
        """
        Establish connection to RabbitMQ with retry logic.

        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds

        Returns:
            bool: True if connection successful, False otherwise
        """
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Close existing connection if any
                if self.connection and self.connection.is_open:
                    self.connection.close()

                # Create new connection
                credentials = pika.PlainCredentials(self.username, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )

                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()

                # Declare the dead letter exchange
                self.channel.exchange_declare(
                    exchange="dead_letter_exchange",
                    exchange_type="direct",
                    durable=True,
                )

                # Declare the dead letter queue
                self.channel.queue_declare(queue=self.dead_letter_queue, durable=True)

                # Bind the dead letter queue to the dead letter exchange
                self.channel.queue_bind(
                    exchange="dead_letter_exchange",
                    queue=self.dead_letter_queue,
                    routing_key="dead_letter_queue",  # Use consistent routing key
                )

                # Declare the main queue with dead letter configuration
                try:
                    # Try to declare the queue with dead letter configuration
                    self.channel.queue_declare(
                        queue=self.queue_name,
                        durable=True,
                        arguments={
                            "x-dead-letter-exchange": "dead_letter_exchange",
                            "x-dead-letter-routing-key": self.dead_letter_queue,
                        },
                    )
                except Exception as e:
                    # If queue already exists without dead letter config, just declare it normally
                    self.logger.warning(
                        f"Queue {self.queue_name} exists without dead letter config, declaring normally: {str(e)}"
                    )
                    self.channel.queue_declare(queue=self.queue_name, durable=True)

                # Set QoS to avoid overwhelming the consumer
                self.channel.basic_qos(prefetch_count=1)

                self.logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
                return True

            except AMQPConnectionError as e:
                retry_count += 1
                self.logger.warning(
                    f"Failed to connect to RabbitMQ (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                if retry_count < max_retries:
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Max retries reached. Could not connect to RabbitMQ.")
                    return False

    def start_consuming(self, callback: Callable[[Dict[str, Any]], bool]) -> None:
        """
        Start consuming messages from the queue.

        Args:
            callback: Function to process messages. Should return True if message
                     was processed successfully, False otherwise.
        """
        if not self.connection or not self.channel or not self.connection.is_open:
            if not self.connect():
                self.logger.error("Cannot start consuming: not connected to RabbitMQ")
                return

        def process_message(ch, method, properties, body):
            """Internal callback to process the message and handle errors."""
            correlation_id = (
                properties.headers.get("correlation_id", "unknown")
                if properties.headers
                else "unknown"
            )

            try:
                # Parse the message
                message_data = json.loads(body)

                # Process the message using the provided callback
                success = callback(message_data)

                if success:
                    # Acknowledge the message if processing was successful
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self.logger.info(
                        "Message processed successfully",
                        extra={"correlation_id": correlation_id},
                    )
                else:
                    # Reject the message and requeue it if processing failed
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    self.logger.warning(
                        "Message processing failed, sending to dead letter queue",
                        extra={"correlation_id": correlation_id},
                    )

            except json.JSONDecodeError:
                self.logger.error(
                    "Failed to parse message as JSON",
                    extra={"correlation_id": correlation_id},
                )
                # Reject malformed messages without requeuing
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            except Exception as e:
                self.logger.error(
                    f"Error processing message: {str(e)}",
                    extra={"correlation_id": correlation_id},
                )
                # Reject the message without requeuing on unexpected errors
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        # Start consuming messages
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=process_message, auto_ack=False
        )

        self.logger.info(f"Started consuming from queue: {self.queue_name}")

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.logger.info("Consumer stopped by user")
        except Exception as e:
            self.logger.error(f"Consumer stopped due to error: {str(e)}")
        finally:
            self.close()

    def close(self):
        """Close the connection to RabbitMQ."""
        if self.connection and self.connection.is_open:
            self.connection.close()
            self.logger.info("Closed RabbitMQ connection")
