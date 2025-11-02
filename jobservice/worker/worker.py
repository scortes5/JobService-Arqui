import os
import math
from typing import List, Dict, Any
import requests
from celery import Celery

# === Config Celery ===
BROKER_URL = os.getenv("BROKER_URL", "redis://redis:6379/0")
RESULT_BACKEND = os.getenv("RESULT_BACKEND", "redis://redis:6379/1")
celery = Celery("reco", broker=BROKER_URL, backend=RESULT_BACKEND)
celery.conf.task_default_queue = "reco"

# === Helpers ===
def haversine_km(lat1, lon1, lat2, lon2):
    """Calcula distancia entre coordenadas (km)"""
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def fetch_all_properties():
    """
    Obtiene las propiedades desde un endpoint interno o mock.
    En esta versión: usa el mock de JobMaster directamente.
    """
    url = os.getenv("PROPERTIES_API_URL", "http://jobmaster:4000/mock/properties")
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def basic_filter_and_rank(base: Dict[str, Any], props: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filtra propiedades similares y ordena por distancia y precio."""
    candidates = []
    for p in props:
        try:
            if (
                str(p.get("comuna", "")).strip().lower() == base["comuna"].strip().lower()
                and int(p.get("dormitorios", -1)) == int(base["dormitorios"])
                and float(p.get("precio", float("inf"))) <= float(base["precio"])
            ):
                dist = haversine_km(base["lat"], base["lon"], float(p["lat"]), float(p["lon"]))
                candidates.append({**p, "_distance_km": round(dist, 3)})
        except Exception:
            continue

    candidates.sort(key=lambda x: (x["_distance_km"], float(x["precio"])))
    return candidates[:3]

# === Task principal ===
@celery.task(name="tasks.recommend")
def recommend(base_property: Dict[str, Any]):
    """
    Recibe la propiedad base desde el JobMaster y retorna recomendaciones.
    """
    all_props = fetch_all_properties()
    recos = basic_filter_and_rank(base_property, all_props)
    if not recos:
        return {"message": "sin coincidencias", "recommendations": []}
    return {
        "message": "ok",
        "recommendations": [
            {
                "id": p.get("id"),
                "titulo": p.get("titulo"),
                "precio": p.get("precio"),
                "comuna": p.get("comuna"),
                "dormitorios": p.get("dormitorios"),
                "lat": p.get("lat"),
                "lon": p.get("lon"),
                "distance_km": p["_distance_km"],
                "url": p.get("url"),
                "img": p.get("img"),
            }
            for p in recos
        ],
    }
