FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create uploads directory
RUN mkdir -p uploads

# Install Spark driver dependencies (JRE) late to preserve cache of earlier layers
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && \
    rm -rf /var/lib/apt/lists/* && \
    JAVA_HOME_PATH=$(dirname $(dirname $(readlink -f $(which java)))) && \
    ln -s "$JAVA_HOME_PATH" /opt/java-home
ENV JAVA_HOME=/opt/java-home
ENV PATH="$JAVA_HOME/bin:$PATH"

# Expose port 4999
EXPOSE 4999

# Run the application
CMD ["python", "app.py"]
