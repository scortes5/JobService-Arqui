import os
import math
from typing import List, Dict, Any
from celery import Celery
from services.properties_api import get_internal_properties
from services.extract_comuna import extract_comuna
from services.geo_api import geocode
from services.bedrooms import _parse_bedrooms

BROKER_URL = os.getenv("BROKER_URL", "redis://redis:6379/0")
RESULT_BACKEND = os.getenv("RESULT_BACKEND", "redis://redis:6379/1")
celery = Celery("reco", broker=BROKER_URL, backend=RESULT_BACKEND)
celery.conf.task_default_queue = "reco"


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def fetch_all_properties() -> List[Dict[str, Any]]:
    data = get_internal_properties(page=1, limit=500)
    return data.get("results", [])


def basic_filter_and_rank(base: Dict[str, Any], props: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    1) Filtra por comuna, dormitorios y precio.
    2) Si hay <= 3 o no tengo lat/lon base -> ordena por precio.
    3) Si hay > 3 y tengo lat/lon base -> calcula distancia (geocode) y ordena por distancia + precio.
    """
    candidates: List[Dict[str, Any]] = []

    base_comuna = (base.get("comuna") or "").strip().lower()
    base_dorms = int(base.get("dormitorios") or 0)
    base_price = float(base.get("price") or 0.0)
    base_lat = base.get("lat")
    base_lon = base.get("lon")

    # 1) Filtro inicial por comuna/dormitorios/precio
    for p in props:
        try:
            loc_str = p.get("location") or p.get("name") or ""
            comuna_p = (extract_comuna(loc_str) or "").strip().lower()
            dormitorios_p = _parse_bedrooms(p.get("bedrooms"))
            price_p = float(p.get("price") or 0.0)

            if (
                comuna_p == base_comuna
                and dormitorios_p == base_dorms
                and price_p <= base_price
            ):
                p = {**p}  # copia para no mutar el original
                candidates.append(p)
        except Exception:
            continue

    if not candidates:
        return []

    # 2) Si no tengo coordenadas base o hay 3 o menos, solo ordeno por precio
    if base_lat is None or base_lon is None or len(candidates) <= 3:
        for p in candidates:
            p["_distance_km"] = 0.0
        candidates.sort(key=lambda x: (x["_distance_km"], float(x.get("price") or 0.0)))
        return candidates[:3]

    # 3) Tengo lat/lon base Y más de 3 candidatos -> uso geocodificación + Haversine
    enriched: List[Dict[str, Any]] = []
    for p in candidates:
        loc_str = p.get("location") or p.get("name") or ""
        dist = float("inf")

        if loc_str:
            try:
                g = geocode(loc_str)
                if g and g.get("lat") is not None and g.get("lon") is not None:
                    dist = haversine_km(
                        float(base_lat),
                        float(base_lon),
                        float(g["lat"]),
                        float(g["lon"]),
                    )
            except Exception:
                dist = float("inf")

        p["_distance_km"] = dist
        enriched.append(p)

    # descarto las que no pude geocodificar (distancia infinita)
    enriched = [p for p in enriched if math.isfinite(p["_distance_km"])]

    if not enriched:
        # fallback: solo precio si nada tuvo distancia válida
        for p in candidates:
            p["_distance_km"] = 0.0
        candidates.sort(key=lambda x: (x["_distance_km"], float(x.get("price") or 0.0)))
        return candidates[:3]

    # ordeno por distancia y luego por precio
    enriched.sort(key=lambda x: (x["_distance_km"], float(x.get("price") or 0.0)))
    return enriched[:3]


@celery.task(name="tasks.recommend")
def recommend(base_property: Dict[str, Any]):
    all_props = fetch_all_properties()
    recos = basic_filter_and_rank(base_property, all_props)

    if not recos:
        return {"message": "sin coincidencias", "recommendations": []}

    return {
        "message": "ok",
        "recommendations": [
            {
                "id": p.get("id"),
                "titulo": p.get("name"),
                "precio": p.get("price"),
                "comuna": extract_comuna(p.get("location") or p.get("name") or ""),
                "dormitorios": _parse_bedrooms(p.get("bedrooms")),
                "lat": base_property.get("lat"),
                "lon": base_property.get("lon"),
                "url": p.get("url"),
                "img": p.get("img"),
            }
            for p in recos
        ],
    }
