import os, time, threading, requests
from typing import Optional, Dict, Any

# --- cache en memoria (proceso) ---
_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = threading.Lock()

# --- rate limit nominatin: 1 req/s ---
_last_nominatim_call = 0.0
_rl_lock = threading.Lock()

def _cache_get(addr: str) -> Optional[Dict[str, Any]]:
    key = addr.strip().lower()
    with _cache_lock:
        return _cache.get(key)

def _cache_put(addr: str, data: Dict[str, Any]) -> None:
    key = addr.strip().lower()
    with _cache_lock:
        _cache[key] = data

def geocode_nominatim(addr: str) -> Optional[Dict[str, Any]]:
    global _last_nominatim_call
    # throttle 1 req/s
    with _rl_lock:
        now = time.time()
        wait = 1.0 - (now - _last_nominatim_call)
        if wait > 0:
            time.sleep(wait)
        _last_nominatim_call = time.time()

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": addr,
        "format": "jsonv2",
        "countrycodes": "cl",   # prioriza Chile
        "addressdetails": 1,
        "limit": 1
    }
    headers = {
        "User-Agent": os.getenv("GEOCODER_UA", "JobMaster/1.0 (contacto: you@example.com)")
    }
    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not data:
        return None
    return {"lat": float(data[0]["lat"]), "lon": float(data[0]["lon"]), "provider": "nominatim"}

def geocode_google(addr: str, api_key: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": addr, "key": api_key, "region": "cl"}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if j.get("status") != "OK" or not j.get("results"):
        return None
    loc = j["results"][0]["geometry"]["location"]
    return {"lat": float(loc["lat"]), "lon": float(loc["lng"]), "provider": "google"}

def geocode_mapbox(addr: str, token: str) -> Optional[Dict[str, Any]]:
    import urllib.parse as up
    q = up.quote(addr)
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{q}.json"
    params = {"access_token": token, "limit": 1, "country": "cl"}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    if not j.get("features"):
        return None
    center = j["features"][0]["center"]  # [lon, lat]
    return {"lat": float(center[1]), "lon": float(center[0]), "provider": "mapbox"}

def geocode(addr: str) -> Optional[Dict[str, Any]]:
    """Devuelve {'lat':..., 'lon':..., 'provider': 'nominatim|google|mapbox'} o None."""
    if not addr or not addr.strip():
        return None

    # cache hit
    c = _cache_get(addr)
    if c:
        return c

    # elige proveedor según env
    gkey = os.getenv("GOOGLE_MAPS_API_KEY")
    mtoken = os.getenv("MAPBOX_TOKEN")

    geo = None
    try:
        if gkey:
            geo = geocode_google(addr, gkey)
        elif mtoken:
            geo = geocode_mapbox(addr, mtoken)
        else:
            geo = geocode_nominatim(addr)
    except requests.RequestException:
        geo = None

    if geo:
        _cache_put(addr, geo)
    return geo

print(geocode("Av. Libertador Bernardo O'Higgins 1234, Santiago, Chile"))
print(geocode("Fray Montalva 360, Las Condes, Chile"))
