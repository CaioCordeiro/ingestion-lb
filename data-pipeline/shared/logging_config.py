import logging
import sys
from pythonjsonlogger import jsonlogger

def configure_logger(service_name: str) -> logging.Logger:
    """
    Configure a structured JSON logger for consistent logging across microservices.
    
    Args:
        service_name: Name of the service using the logger
        
    Returns:
        A configured logger instance
    """
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    
    # Create a handler that outputs to stdout
    handler = logging.StreamHandler(sys.stdout)
    
    # Create a custom JSON formatter with service name
    formatter = jsonlogger.JsonFormatter(
        fmt='%(asctime)s %(levelname)s %(name)s %(service)s %(correlation_id)s %(message)s',
        rename_fields={'levelname': 'level', 'asctime': 'timestamp'}
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Create a filter to add service name to all log records
    class ServiceFilter(logging.Filter):
        def filter(self, record):
            record.service = service_name
            if not hasattr(record, 'correlation_id'):
                record.correlation_id = '-'
            return True
    
    logger.addFilter(ServiceFilter())
    
    return logger
