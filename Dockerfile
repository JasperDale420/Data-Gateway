FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
# We don't have a lock file, so installing directly from pyproject.toml or manually.
# Since pyproject.toml is modern but maybe not directly usable by pip without build backend if not set up.
# Let's assume we can install .[dev] if it is a valid package, or just install dependencies.
# Inspecting pyproject.toml revealed generic deps.
# Let's just install them explicitly for stability or using pip install .
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    redis \
    pydantic-settings \
    alpaca-py \
    unusualwhales-python-client \
    pandas \
    pyarrow \
    yfinance \
    fredapi \
    newsapi-python \
    coinbase-advanced-py \
    httpx

# Copy source code
COPY src /app/src
COPY .env.example /app/.env
# Note: Real .env should be mounted or passed as env vars

# Set PYTHONPATH
ENV PYTHONPATH=/app/src

# Create data directory
RUN mkdir -p /data

# Expose port
EXPOSE 8000

# Command to run (using main entry point)
CMD ["uvicorn", "dataingestion.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
