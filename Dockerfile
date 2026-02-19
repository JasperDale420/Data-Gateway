# Runtime stage (single-stage for simplicity)
FROM python:3.12-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r gateway && \
    useradd -r -g gateway -m -d /home/gateway gateway && \
    mkdir -p /home/gateway/.cache/py-yfinance && \
    chown -R gateway:gateway /home/gateway

# Copy and install patched Unusual Whales SDK v5.1 first
COPY vendor/unusualwhales_sdk/ /tmp/unusualwhales_sdk/
RUN pip install --no-cache-dir /tmp/unusualwhales_sdk/ && rm -rf /tmp/unusualwhales_sdk/

# Install curl for healthchecks and debugging
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml first to cache dependency installation
COPY pyproject.toml README.md ./

# Install only dependencies (cached until pyproject.toml changes)
# Create a minimal package stub so pip can parse pyproject.toml dependencies
RUN mkdir -p gateway && \
    echo '"""Stub for dependency resolution."""' > gateway/__init__.py && \
    pip install --no-cache-dir . && \
    pip uninstall -y uvloop || true && \
    rm -rf gateway

# Copy gateway source and reinstall package only (deps already installed above)
COPY gateway/ gateway/
RUN pip install --no-cache-dir --no-deps .

# Copy config files
COPY config/ config/

# Switch to non-root user
USER gateway

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8080/health').raise_for_status()"

# Run application
CMD ["python", "-m", "uvicorn", "gateway.main:app", "--host", "0.0.0.0", "--port", "8080"]
