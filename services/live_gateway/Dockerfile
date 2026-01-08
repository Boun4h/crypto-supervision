FROM python:3.12-slim

WORKDIR /app
COPY requirements-stream.txt /app/requirements-stream.txt
RUN pip install --no-cache-dir -r /app/requirements-stream.txt

COPY services /app/services

ENV PYTHONPATH=/app
CMD ["python", "-m", "services.live_gateway.main"]
