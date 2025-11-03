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


# ---------------- helpers ---------------- #

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


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


# ---------------- ranking ---------------- #

def basic_filter_and_rank(base: Dict[str, Any],
                          props: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Estrategia:
    1) Intento estricto: misma comuna + mismos dormitorios + precio <= base.
    2) Si no hay: misma comuna + mismos dormitorios (ignoro precio).
    3) Si no hay: misma comuna (ignoro dormitorios y precio).
    4) Si no hay: fallback global (las más baratas del sistema).
    En todos los casos, si tengo lat/lon base y >3 candidatos, ordeno por distancia + precio.
    """

    base_comuna = (base.get("comuna") or "").strip().lower()
    base_dorms = base.get("dormitorios") or 0
    base_price = base.get("price")
    base_lat = base.get("lat")
    base_lon = base.get("lon")

    try:
        base_dorms = int(base_dorms)
    except Exception:
        base_dorms = 0

    base_price = _safe_float(base_price, default=0.0)

    strict_candidates: List[Dict[str, Any]] = []
    same_comuna_dorms: List[Dict[str, Any]] = []
    same_comuna: List[Dict[str, Any]] = []

    for p in props:
        try:
            loc_str = p.get("location") or p.get("name") or ""
            comuna_p = (extract_comuna(loc_str) or "").strip().lower()
            dormitorios_p = _parse_bedrooms(p.get("bedrooms"))
            price_p = _safe_float(p.get("price"), default=float("inf"))

            # misma comuna + mismos dormitorios
            if comuna_p == base_comuna and dormitorios_p == base_dorms:
                p_copy = {**p}  # no mutar original
                same_comuna_dorms.append(p_copy)

                # versión estricta: además precio <= base
                if price_p <= base_price:
                    strict_candidates.append(p_copy)

            # misma comuna (para fallback más laxo)
            elif comuna_p == base_comuna:
                p_copy = {**p}
                same_comuna.append(p_copy)

        except Exception:
            continue

    # elegimos el "nivel" más estricto que tenga resultados
    if strict_candidates:
        candidates = strict_candidates
    elif same_comuna_dorms:
        candidates = same_comuna_dorms
    elif same_comuna:
        candidates = same_comuna
    else:
        # Fallback global: tomar las más baratas del sistema
        fallback = []
        for p in props:
            p_copy = {**p}
            fallback.append(p_copy)
        if not fallback:
            return []
        fallback.sort(key=lambda x: _safe_float(x.get("price"), default=float("inf")))
        candidates = fallback[:20]  # recorto universo antes de rankear

    # Si no tengo coordenadas base o hay pocas, ordeno solo por precio
    if base_lat is None or base_lon is None or len(candidates) <= 3:
        for p in candidates:
            p["_distance_km"] = 0.0
        candidates.sort(key=lambda x: (_safe_float(x.get("_distance_km"), 0.0),
                                       _safe_float(x.get("price"), float("inf"))))
        return candidates[:3]

    # Tengo lat/lon base y suficientes candidatos → geocodifico y calculo distancias
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

    enriched = [p for p in enriched if math.isfinite(p["_distance_km"])]

    # si por algún motivo nadie tuvo distancia finita, vuelvo a ordenar solo por precio
    if not enriched:
        for p in candidates:
            p["_distance_km"] = 0.0
        candidates.sort(key=lambda x: (_safe_float(x.get("_distance_km"), 0.0),
                                       _safe_float(x.get("price"), float("inf"))))
        return candidates[:3]

    enriched.sort(key=lambda x: (x["_distance_km"],
                                 _safe_float(x.get("price"), float("inf"))))
    return enriched[:3]


# ---------------- task Celery ---------------- #

@celery.task(name="tasks.recommend")
def recommend(base_property: Dict[str, Any]):
    """
    base_property viene desde JobMaster así:

    {
      "property_id": ...,
      "comuna": ...,
      "dormitorios": ...,
      "price": ...,
      "lat": ...,
      "lon": ...,
      "raw": { ... payload original ... }
    }
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
