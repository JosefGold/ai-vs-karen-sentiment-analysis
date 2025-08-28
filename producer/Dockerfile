FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies for confluent-kafka
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script
COPY send_messages.py .

# Create data directory
RUN mkdir -p data

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "send_messages.py", "--loop"] 