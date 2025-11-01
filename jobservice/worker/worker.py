import os
import math
from typing import List, Dict, Any
import requests
from celery import Celery
import time


BROKER_URL = os.getenv("BROKER_URL", "redis://redis:6379/0")
RESULT_BACKEND = os.getenv("RESULT_BACKEND", "redis://redis:6379/0")
celery = Celery("reco", broker=BROKER_URL, backend=RESULT_BACKEND)

# ---------- Helpers ----------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088  # km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlmb/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

_token_cache = {"value": None, "exp": 0}

def _get_auth0_token():
    if _token_cache["value"] and (_token_cache["exp"] - 30) > time.time():
        return _token_cache["value"]

    url = f"https://{os.getenv('AUTH0_DOMAIN')}/oauth/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("AUTH0_CLIENT_ID"),
        "client_secret": os.getenv("AUTH0_CLIENT_SECRET"),
        "audience": os.getenv("AUTH0_AUDIENCE"),
    }
    r = requests.post(url, json=data, timeout=10)
    r.raise_for_status()
    token = r.json()
    _token_cache["value"] = token["access_token"]
    _token_cache["exp"] = time.time() + token.get("expires_in", 3600)
    return _token_cache["value"]

def fetch_all_properties():
    url = os.getenv("PROPERTIES_API_URL")
    headers = {"Authorization": f"Bearer {_get_auth0_token()}"}
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def basic_filter_and_rank(base: Dict[str, Any], props: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filtro: misma comuna, mismos dormitorios, precio <= base.precio
    Ranking: por distancia (asc), luego por precio (asc)
    Devuelve top 3 (si existen)
    """
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
            # Ignorar registros incompletos
            continue

    # Orden: primero distancia, luego precio
    candidates.sort(key=lambda x: (x["_distance_km"], float(x["precio"])))
    return candidates[:3]

# ---------- Task ----------
@celery.task(name="tasks.recommend")
def recommend(base_property: Dict[str, Any]):
    """
    Lógica de recomendación 'baseline' (cumple E2 sin bonus):
    - Obtiene catálogo completo del backend Java
    - Filtra por comuna, dormitorios, y precio <=
    - Ordena por distancia y precio
    - Retorna top 3 (o vacío si no hay match)
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
