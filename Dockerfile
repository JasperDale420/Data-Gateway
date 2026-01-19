# Runtime stage (single-stage for simplicity)
FROM python:3.12-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r gateway && useradd -r -g gateway gateway

# Copy and install patched Unusual Whales SDK v5.1 first
COPY unusualwhales_sdk/ /tmp/unusualwhales_sdk/
RUN pip install --no-cache-dir /tmp/unusualwhales_sdk/ && rm -rf /tmp/unusualwhales_sdk/

# Copy pyproject.toml first to cache dependency installation
COPY pyproject.toml README.md ./

# Install dependencies explicitly
RUN pip install --no-cache-dir \
    "fastapi[standard]>=0.115.0" \
    "uvicorn[standard]>=0.32.0" \
    "websockets>=13.0" \
    "pydantic>=2.5" \
    "pydantic-settings>=2.0" \
    "cachetools>=5.3" \
    "httpx>=0.27" \
    "structlog>=24.0" \
    "python-dotenv>=1.0" \
    "pyyaml>=6.0" \
    "redis>=5.0" \
    "prometheus-client>=0.20" \
    "psutil>=5.9" \
    "yfinance>=0.2" \
    "alpaca-py>=0.28" \
    "msgpack>=1.0"

# Copy gateway source and install as package
COPY gateway/ gateway/
RUN pip install --no-cache-dir --no-deps .

# Copy config files
COPY clients.yaml providers.yaml ./

# Switch to non-root user
USER gateway

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8080/health').raise_for_status()"

# Run application
CMD ["python", "-m", "uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8080"]
