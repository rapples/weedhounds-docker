# Multi-stage build for Python environment
FROM python:3.11-slim AS builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and browsers
RUN playwright install chromium
RUN playwright install-deps

# Production stage
FROM python:3.11-slim AS production

# Install runtime dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    fonts-liberation \
    libxss1 \
    lsb-release \
    xdg-utils \
    xvfb \
    x11-utils \
    xauth \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Playwright system dependencies
RUN playwright install-deps

# Set working directory
WORKDIR /app

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app

# Copy and set up startup script (as root)
COPY start_with_xvfb.sh /usr/local/bin/start_with_xvfb.sh
RUN chmod +x /usr/local/bin/start_with_xvfb.sh

# Copy Playwright browsers to app user location
COPY --from=builder /root/.cache/ms-playwright /home/app/.cache/ms-playwright
RUN chown -R app:app /home/app/.cache && chown -R app:app /app

# Switch to app user
USER app

# Copy application code
COPY --chown=app:app . .

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8888/stats || exit 1

# Default command (will be overridden in docker-compose)
CMD ["/usr/local/bin/start_with_xvfb.sh", "python", "dutchie_token_service.py"]