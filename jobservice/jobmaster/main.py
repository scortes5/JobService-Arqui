import os
import uuid
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from celery import Celery

BROKER_URL = os.getenv("BROKER_URL", "redis://redis:6379/0")
RESULT_BACKEND = os.getenv("RESULT_BACKEND", "redis://redis:6379/0")

celery = Celery("reco", broker=BROKER_URL, backend=RESULT_BACKEND)

app = FastAPI(title="JobMaster - Recommendations", version="1.0.0")

class PropertyIn(BaseModel):
    comuna: str
    dormitorios: int
    precio: float
    # lat/lon de la propiedad comprada (en grados)
    lat: float = Field(..., description="latitud")
    lon: float = Field(..., description="longitud")

class JobCreateIn(BaseModel):
    property: PropertyIn

@app.get("/heartbeat")
def heartbeat():
    return {"ok": True}

@app.post("/job")
def create_job(payload: JobCreateIn):
    """
    Crea un job de recomendación:
    - task name: tasks.recommend
    - args: dict con la propiedad base
    - task_id: uuid4 para facilidad de trazabilidad
    """
    try:
        job_id = str(uuid.uuid4())
        celery.send_task("tasks.recommend", args=[payload.property.model_dump()], task_id=job_id)
        return {"job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"error creando job: {e}")

@app.get("/job/{job_id}")
def get_job(job_id: str):
    """
    Consulta estado y resultado del job:
    - PENDING | STARTED | RETRY | FAILURE | SUCCESS
    """
    result = celery.AsyncResult(job_id)
    # result.result puede ser None si aún no termina
    return {"status": result.status, "result": result.result}


@app.get("/mock/properties")
def mock_properties() -> List[dict]:
    """ Catálogo falso para testear el worker sin depender del backend Java. """
    return [
        {
            "id": 1, "titulo": "Depto en Ñuñoa", "comuna": "Ñuñoa",
            "dormitorios": 2, "precio": 520000, "lat": -33.457, "lon": -70.603,
            "url": "https://ejemplo.com/1", "img": "https://ejemplo.com/1.jpg"
        },
        {
            "id": 2, "titulo": "Casa en Providencia", "comuna": "Providencia",
            "dormitorios": 3, "precio": 800000, "lat": -33.44, "lon": -70.61,
            "url": "https://ejemplo.com/2", "img": "https://ejemplo.com/2.jpg"
        },
        {
            "id": 3, "titulo": "Depto barato en Ñuñoa", "comuna": "Ñuñoa",
            "dormitorios": 2, "precio": 500000, "lat": -33.46, "lon": -70.60,
            "url": "https://ejemplo.com/3", "img": "https://ejemplo.com/3.jpg"
        },
    ]