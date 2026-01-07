FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy dependency specification and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# By default run the collector
CMD ["python", "app.py"]
