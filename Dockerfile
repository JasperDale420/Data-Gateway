# Runtime stage (single-stage for simplicity)
FROM python:3.12-slim

WORKDIR /app

# Create non-root user
RUN groupadd -r gateway && \
    useradd -r -g gateway -m -d /home/gateway gateway && \
    mkdir -p /home/gateway/.cache/py-yfinance && \
    chown -R gateway:gateway /home/gateway

# Copy and install patched Unusual Whales SDK v5.1 first
COPY Data-Gateway/vendor/unusualwhales_sdk/ /tmp/unusualwhales_sdk/
RUN pip install --no-cache-dir /tmp/unusualwhales_sdk/ && rm -rf /tmp/unusualwhales_sdk/

# Install empire-core from monorepo
COPY empire-core/ /tmp/empire-core/
RUN pip install --no-cache-dir /tmp/empire-core/ && rm -rf /tmp/empire-core/

# Install empire-schemas from monorepo
COPY empire-schemas/ /tmp/empire-schemas/
RUN pip install --no-cache-dir /tmp/empire-schemas/ && rm -rf /tmp/empire-schemas/

# Install curl for healthchecks and debugging
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml first to cache dependency installation
COPY Data-Gateway/pyproject.toml Data-Gateway/README.md ./

# Install only dependencies (cached until pyproject.toml changes)
# Create a minimal package stub so pip can parse pyproject.toml dependencies
RUN mkdir -p gateway && \
    echo '"""Stub for dependency resolution."""' > gateway/__init__.py && \
    pip install --no-cache-dir . && \
    rm -rf gateway

# Remove uvloop if pulled in as a transitive dep (incompatible with container)
RUN pip uninstall -y uvloop 2>/dev/null || true

# Copy gateway source and reinstall package only (deps already installed above)
COPY Data-Gateway/gateway/ gateway/
RUN pip install --no-cache-dir --no-deps .

# Copy config files
COPY Data-Gateway/config/ config/

# Create logs directory writable by gateway user
RUN mkdir -p /app/logs && chown gateway:gateway /app/logs

# Switch to non-root user
USER gateway

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://localhost:8080/health').raise_for_status()"

# Run application
# --ws-ping-interval / --ws-ping-timeout: relax server-side WebSocket keepalive so
# transient event-loop lag during high-volatility bursts doesn't trigger mass
# client disconnects (default 20s/20s is too tight under load; clients use 30s/90s).
CMD ["python", "-m", "uvicorn", "gateway.main:app", \
    "--host", "0.0.0.0", \
    "--port", "8080", \
    "--ws-ping-interval", "30", \
    "--ws-ping-timeout", "90"]
