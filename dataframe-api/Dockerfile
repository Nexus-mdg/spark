FROM python:3.12-slim

WORKDIR /app

# Install system dependencies including JRE 21
RUN apt-get update && apt-get install -y \
    gcc \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create uploads directory
RUN mkdir -p uploads

# Expose port 4999
EXPOSE 4999

# Run the application
CMD ["python", "app.py"]