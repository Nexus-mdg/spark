FROM mcr.microsoft.com/playwright/python:v1.41.0-jammy

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir -r requirements.txt

# Copy test scripts and configuration
COPY tests/ ./tests/
COPY config/ ./config/
COPY utils/ ./utils/
COPY pytest.ini .
COPY conftest.py .

# Set environment variables
ENV PYTHONPATH=/app
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Default command
CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short", "--asyncio-mode=auto"]