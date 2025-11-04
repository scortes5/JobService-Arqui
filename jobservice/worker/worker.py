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
    1) Obtener comuna, dormitorios, precio y ubicación de la propiedad base.
    2) Filtrar propiedades del sistema con:
       - misma comuna
       - mismo número de dormitorios (si se conoce)
       - precio <= precio base (si se conoce)
    3) Ordenar por:
       - distancia geográfica a la propiedad base
       - luego por precio (menor a mayor)
    4) Devolver a lo más 3 coincidencias. Si no hay, lista vacía.
    """

    base_comuna = (base.get("comuna") or "").strip().lower()
    
    # CRÍTICO: Si no hay comuna base, no podemos filtrar (enunciado requiere misma comuna)
    if not base_comuna:
        print("⚠ WARNING: Base property has no comuna - cannot filter by comuna (required by spec)")
        return []

    # dormitorios base: si no se puede parsear, NO filtramos por dormitorios
    base_dorms_raw = base.get("dormitorios")
    try:
        base_dorms = int(base_dorms_raw) if base_dorms_raw is not None else None
    except Exception:
        base_dorms = None

    # precio base: si no se puede parsear, NO filtramos por precio
    raw_base_price = base.get("price")
    if raw_base_price is None:
        base_price = None
    else:
        try:
            base_price = float(raw_base_price)
        except Exception:
            base_price = None

    base_lat = base.get("lat")
    base_lon = base.get("lon")
    base_id = base.get("property_id")

    candidates: List[Dict[str, Any]] = []
    
    stats = {
        "total": len(props),
        "same_id": 0,
        "no_comuna": 0,
        "diff_comuna": 0,
        "diff_dorms": 0,
        "price_too_high": 0,
        "passed": 0,
        "errors": 0
    }

    print(f"Base property: comuna='{base_comuna}', dormitorios={base_dorms}, price={base_price}, lat={base_lat}, lon={base_lon}")
    print(f"Total properties to filter: {len(props)}")

    # 2) FILTRO ESTRICTO según enunciado, pero sólo si tenemos datos para filtrar
    for p in props:
        try:
            loc_str = p.get("location") or p.get("name") or ""
            comuna_p = (extract_comuna(loc_str) or "").strip().lower()
            dormitorios_p = _parse_bedrooms(p.get("bedrooms"))
            price_p = _safe_float(p.get("price"), default=float("inf"))
            lat_p = p.get("lat")
            lon_p = p.get("lon")

            # excluir la misma propiedad base si está en el listado interno
            if base_id is not None and p.get("id") == base_id:
                stats["same_id"] += 1
                continue
            
            # Si no se pudo extraer comuna de la propiedad candidata
            if not comuna_p:
                stats["no_comuna"] += 1
                continue

            # misma comuna (SIEMPRE obligatorio)
            if comuna_p != base_comuna:
                stats["diff_comuna"] += 1
                continue

            # mismos dormitorios (solo si conozco dormitorios base)
            if base_dorms is not None and dormitorios_p != base_dorms:
                stats["diff_dorms"] += 1
                continue

            # precio <= precio base (solo si conozco precio base)
            if base_price is not None and price_p > base_price:
                stats["price_too_high"] += 1
                continue

            stats["passed"] += 1
            print(f"✓ Candidate property: comuna='{comuna_p}', dormitorios={dormitorios_p}, price={price_p}, lat={lat_p}, lon={lon_p}")

            p_copy = {**p}
            candidates.append(p_copy)
        except Exception as e:
            stats["errors"] += 1
            print(f"✗ Error processing property {p.get('id')}: {e}")
            continue

    # 4) Si no hay coincidencias, se devuelve lista vacía
    print(f"\nFilter stats: {stats}")
    if not candidates:
        print("⚠ No candidates found after filtering")
        return []

    # 3) ORDENAR según cercanía geográfica y precio
    # Si no tengo coordenadas base, solo ordeno por precio
    if base_lat is None or base_lon is None:
        for p in candidates:
            p["_distance_km"] = None
        candidates.sort(key=lambda x: _safe_float(x.get("price"), float("inf")))
        return candidates[:3]

    # Tengo lat/lon base → calculo distancias
    enriched: List[Dict[str, Any]] = []
    for p in candidates:
        loc_str = p.get("location") or p.get("name") or ""
        dist = float("inf")
        lat_p = p.get("lat")
        lon_p = p.get("lon")

        # Si la propiedad no tiene lat/lon guardados, intento geocodificar
        if (lat_p is None or lon_p is None) and loc_str:
            try:
                g = geocode(loc_str)
                if g and g.get("lat") is not None and g.get("lon") is not None:
                    lat_p = g["lat"]
                    lon_p = g["lon"]
                    p["lat"], p["lon"] = lat_p, lon_p
            except Exception:
                pass

        if lat_p is not None and lon_p is not None:
            try:
                dist = haversine_km(
                    float(base_lat),
                    float(base_lon),
                    float(lat_p),
                    float(lon_p),
                )
            except Exception:
                dist = float("inf")

        p["_distance_km"] = dist
        enriched.append(p)

    # Si no pude calcular distancias finitas, ordeno solo por precio
    enriched_valid = [p for p in enriched if math.isfinite(p.get("_distance_km", float("inf")))]
    if not enriched_valid:
        for p in candidates:
            p["_distance_km"] = None
        candidates.sort(key=lambda x: _safe_float(x.get("price"), float("inf")))
        return candidates[:3]

    # Orden final: primero distancia, luego precio
    enriched_valid.sort(
        key=lambda x: (
            x["_distance_km"],
            _safe_float(x.get("price"), float("inf")),
        )
    )

    return enriched_valid[:3]


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
                "lat": p.get("lat"),
                "lon": p.get("lon"),
                "url": p.get("url"),
                "img": p.get("img"),
            }
            for p in recos
        ],
    }
