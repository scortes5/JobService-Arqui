import uuid
from typing import List, Optional, Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from celery.result import AsyncResult
from celery_app import celery_app
from services.extract_comuna import extract_comuna
from services.geo_api import geocode
from services.properties_api import get_internal_properties

app = FastAPI(title="JobMaster - Recommendations", version="1.0.0")


# ======== MODELOS ======== #
class PropertyIn(BaseModel):
    id: Optional[Union[int, str]] = None

    # lo que manda el backend
    name: str | None = None          # "bla bla bla, La Florida, chile"
    beedrooms: int | float | str | None = Field(
        default=None, alias="beedrooms"
    )

    # soporte adicional por si en algún momento llegan estos campos
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
    # dump con alias para conservar "beedrooms" si viene así
    p = payload.property.model_dump(by_alias=True)

    # 1) Determinar string de ubicación (location/name)
    location_str = p.get("location") or p.get("name")

    # 2) Extraer comuna
    comuna = extract_comuna(location_str) if location_str else None

    # 3) Normalizar dormitorios: primero bedrooms, luego beedrooms
    bedrooms_raw = p.get("bedrooms")
    if bedrooms_raw is None:
        bedrooms_raw = p.get("beedrooms")
    dormitorios = _as_int(bedrooms_raw)

    # 4) Normalizar precio
    price = _as_float(p.get("price"))

    # 5) Geocoding si falta lat/lon y tenemos string de ubicación
    g = None
    if (p.get("lat") is None or p.get("lon") is None) and location_str:
        g = geocode(location_str)
    if g is not None:
        p["lat"], p["lon"] = g["lat"], g["lon"]

    # 6) Payload derivado para el worker
    derived = {
        "property_id": p.get("id"),
        "comuna": comuna,
        "dormitorios": dormitorios,
        "price": price,
        "lat": p.get("lat"),
        "lon": p.get("lon"),
        "raw": p,
    }
    
    # Log para debugging
    print(f"[JobMaster] Creating job for property:")
    print(f"  - location_str: {location_str}")
    print(f"  - comuna extracted: {comuna}")
    print(f"  - dormitorios: {dormitorios}")
    print(f"  - price: {price}")
    print(f"  - lat/lon: {p.get('lat')}, {p.get('lon')}")

    # 7) Encolar tarea en Celery (SIN task_queues[0])
    job_id = str(uuid.uuid4())
    celery_app.send_task(
        "tasks.recommend",
        args=[derived],
        task_id=job_id,           # usa la cola por defecto ("reco" en el worker)
    )
    return {"job_id": job_id}


@app.get("/job/{job_id}")
def get_job(job_id: str):
    result = AsyncResult(job_id, app=celery_app)
    return {"status": result.status, "result": result.result}


@app.get("/debug/properties")
def debug_properties(page: int = 1, limit: int = 5):
    data = get_internal_properties(page=page, limit=limit)
    return data
