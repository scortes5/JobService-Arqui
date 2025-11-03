import os
import requests
from .auth0_client import auth0_client

PROPERTIES_API_BASE_URL = os.getenv(
    "PROPERTIES_API_BASE_URL",
    "https://api.iic2173grupo4.tech",
)

def get_internal_properties(page: int = 1, limit: int = 25) -> dict:
    token = auth0_client.get_token()
    headers = {
        "Authorization": f"Bearer {token}",
    }
    params = {
        "page": page,
        "limit": limit,
    }

    resp = requests.get(
        f"{PROPERTIES_API_BASE_URL}/properties/internal",
        headers=headers,
        params=params,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()
