FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

ENV PYTHONUNBUFFERED=1
CMD ["celery", "-A", "worker.celery", "worker", "--loglevel=INFO", "--concurrency=2"]
