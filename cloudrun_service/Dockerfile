# Cloud Run container for the Pub/Sub-triggered ingester.
# It uses Functions Framework to expose an HTTP endpoint.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Functions Framework listens on $PORT
ENV PORT=8080
CMD ["functions-framework", "--target=pubsub_handler", "--port=8080"]
