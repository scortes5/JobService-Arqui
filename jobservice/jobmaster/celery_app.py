from celery import Celery
import os

BROKER = os.getenv("BROKER_URL", "redis://redis:6379/0")
BACKEND = os.getenv("RESULT_BACKEND", "redis://redis:6379/0")

celery = Celery("jobmaster", broker=BROKER, backend=BACKEND)
# Debe coincidir con el nombre del módulo de tareas del worker
celery.autodiscover_tasks(["tasks"])
