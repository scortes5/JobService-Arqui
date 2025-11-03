FROM python:3.10-slim

WORKDIR /app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services ./services
COPY worker ./worker

ENV PYTHONPATH=/app

WORKDIR /app/worker

CMD ["celery", "-A", "worker", "worker", "--loglevel=info", "-Q", "reco"]
