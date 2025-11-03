import uuid
from typing import List, Optional, Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from celery.result import AsyncResult
from celery_app import celery_app  
from utils.extract_comuna import extract_comuna
from utils.geo_api import geocode



app = FastAPI(title="JobMaster - Recommendations", version="1.0.0")

# ======== MODELOS ======== #
class PropertyIn(BaseModel):
    id: Optional[Union[int, str]] = None
    location: str | None = None
    bedrooms: int | float | str | None = None
    price: float | int | str | None = None
    lat: float | None = None
    lon: float | None = None

class JobCreateIn(BaseModel):
    property: PropertyIn = Field(...)


def _as_int(val):
    try:
        return int(val) if val is not None else None
    except Exception:
        return None

def _as_float(val):
    try:
        return float(val) if val is not None else None
    except Exception:
        return None

# ======== ENDPOINTS ======== #
@app.get("/heartbeat")
def heartbeat():
    return {"ok": True, "service": "JobMaster"}

@app.post("/job")
def create_job(payload: JobCreateIn):
    p = payload.property.model_dump()

    comuna = extract_comuna(p.get("location"))
    property_id = p.get("id")
    dormitorios = _as_int(p.get("bedrooms"))
    price = _as_float(p.get("price"))
    if (p.get("lat") is None or p.get("lon") is None) and p.get("location"):
        g = geocode(p["location"])
    if g:
        p["lat"], p["lon"] = g["lat"], g["lon"]


    derived = {
        "property_id": property_id,
        "comuna": comuna,
        "dormitorios": dormitorios,
        "price": price,
        "lat": p.get("lat"),
        "lon": p.get("lon"),
        "raw": p,
    }

    job_id = str(uuid.uuid4())
    celery_app.send_task("tasks.recommend", args=[derived], queue=celery_app.conf.task_queues[0].name, task_id=job_id)
    return {"job_id": job_id}


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
