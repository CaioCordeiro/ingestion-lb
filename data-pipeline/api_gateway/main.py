import uuid
from typing import Dict, Any, Optional
import httpx
from fastapi import FastAPI, Request, Response, UploadFile, File, Form, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging
import os
import sys

# Add parent directory to path to import shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.logging_config import configure_logger

# Configure logger
logger = configure_logger("api_gateway")

# Create FastAPI app
app = FastAPI(
    title="Data Pipeline API Gateway",
    description="API Gateway for data ingestion pipeline",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Service URLs (with environment variable fallbacks)
INGESTION_TEXT_URL = os.getenv("INGESTION_TEXT_URL", "http://ingestion_text:8001")
INGESTION_TABULAR_URL = os.getenv("INGESTION_TABULAR_URL", "http://ingestion_tabular:8002")
INGESTION_JSON_URL = os.getenv("INGESTION_JSON_URL", "http://ingestion_json:8003")
INGESTION_FILE_URL = os.getenv("INGESTION_FILE_URL", "http://ingestion_file:8004")

# Create a shared async HTTP client
http_client = httpx.AsyncClient(timeout=30.0)

# Middleware to add correlation ID
@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    # Generate a correlation ID if not provided in headers
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    
    # Add correlation ID to request state for logging
    request.state.correlation_id = correlation_id
    
    # Log the incoming request
    logger.info(
        f"Received request: {request.method} {request.url.path}",
        extra={
            "correlation_id": correlation_id,
            "method": request.method,
            "path": request.url.path,
            "query_params": str(request.query_params),
            "client_host": request.client.host if request.client else None
        }
    )
    
    # Process the request
    response = await call_next(request)
    
    # Add correlation ID to response headers
    response.headers["X-Correlation-ID"] = correlation_id
    
    # Log the response
    logger.info(
        f"Returning response: {response.status_code}",
        extra={
            "correlation_id": correlation_id,
            "status_code": response.status_code
        }
    )
    
    return response

# Helper function to forward requests
async def forward_request(
    target_url: str,
    request: Request,
    path: str,
    files: Optional[Dict[str, UploadFile]] = None
) -> Response:
    """
    Forward a request to a target service.
    
    Args:
        target_url: Base URL of the target service
        request: Original FastAPI request
        path: Path to append to the target URL
        files: Optional files to include in the forwarded request
        
    Returns:
        Response from the target service
    """
    url = f"{target_url}{path}"
    headers = dict(request.headers)
    
    # Ensure correlation ID is in headers
    headers["X-Correlation-ID"] = request.state.correlation_id
    
    # Remove host header to avoid conflicts
    headers.pop("host", None)
    
    try:
        if files:
            # Handle file uploads
            form_data = {}
            for field_name, file in files.items():
                form_data[field_name] = (file.filename, await file.read(), file.content_type)
            
            # Forward the request with files
            response = await http_client.post(
                url,
                headers=headers,
                files=form_data
            )
        else:
            # Get the request body
            body = await request.body()
            
            # Forward the request
            response = await http_client.request(
                request.method,
                url,
                headers=headers,
                content=body,
                params=request.query_params
            )
        
        # Create FastAPI response
        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )
        
    except httpx.RequestError as e:
        logger.error(
            f"Error forwarding request to {url}: {str(e)}",
            extra={"correlation_id": request.state.correlation_id}
        )
        raise HTTPException(status_code=503, detail=f"Service unavailable: {str(e)}")

# Routes
@app.post("/ingest/text")
async def ingest_text(request: Request):
    """Forward text ingestion requests to the text ingestion service."""
    return await forward_request(INGESTION_TEXT_URL, request, "/ingest")

@app.post("/ingest/csv")
async def ingest_csv(request: Request, file: UploadFile = File(...)):
    """Forward CSV ingestion requests to the tabular ingestion service."""
    return await forward_request(INGESTION_TABULAR_URL, request, "/ingest", {"file": file})

@app.post("/ingest/json")
async def ingest_json(request: Request):
    """Forward JSON ingestion requests to the JSON ingestion service."""
    return await forward_request(INGESTION_JSON_URL, request, "/ingest")

@app.post("/ingest/file")
async def ingest_file(request: Request, file: UploadFile = File(...)):
    """Forward file ingestion requests to the file ingestion service."""
    return await forward_request(INGESTION_FILE_URL, request, "/ingest", {"file": file})

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "api_gateway"}

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Close connections on shutdown."""
    await http_client.aclose()
    logger.info("API Gateway shutting down")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
