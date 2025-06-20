FROM python:3.11-slim

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8003

# Command to run the application
CMD ["uvicorn", "ingestion_json.main:app", "--host", "0.0.0.0", "--port", "8003"]
