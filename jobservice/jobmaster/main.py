import uuid
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from celery.result import AsyncResult
from celery_app import celery_app  

app = FastAPI(title="JobMaster - Recommendations", version="1.0.0")

# ======== MODELOS ======== #
class PropertyIn(BaseModel):
    comuna: str
    dormitorios: int
    precio: float
    lat: float = Field(..., description="Latitud de la propiedad")
    lon: float = Field(..., description="Longitud de la propiedad")

class JobCreateIn(BaseModel):
    property: PropertyIn

# ======== ENDPOINTS ======== #
@app.get("/heartbeat")
def heartbeat():
    return {"ok": True, "service": "JobMaster"}

@app.post("/job")
def create_job(payload: JobCreateIn):
    """
    Crea un job de recomendación:
    - task: tasks.recommend
    - args: propiedad base
    """
    try:
        job_id = str(uuid.uuid4())
        celery_app.send_task(
            "tasks.recommend",
            args=[payload.property.model_dump()],
            queue="reco",
            task_id=job_id
        )
        return {"job_id": job_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"error creando job: {e}")

@app.get("/job/{job_id}")
def get_job(job_id: str):
    result = AsyncResult(job_id, app=celery_app)
    return {"status": result.status, "result": result.result}

@app.get("/mock/properties")
def mock_properties() -> List[dict]:
    """Mock de propiedades para testear worker sin backend real"""
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
