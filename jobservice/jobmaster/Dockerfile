FROM python:3.10-slim

WORKDIR /app

COPY jobmaster/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY services ./services
COPY jobmaster ./jobmaster


ENV PYTHONPATH=/app

WORKDIR /app/jobmaster

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "4000"]
