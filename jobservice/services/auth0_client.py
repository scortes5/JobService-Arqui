import os
import time
import requests
from typing import Optional

AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_CLIENT_ID = os.getenv("AUTH0_CLIENT_ID")
AUTH0_CLIENT_SECRET = os.getenv("AUTH0_CLIENT_SECRET")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")

class Auth0M2MClient:
    def __init__(self):
        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        now = time.time()
        # si el token aún sirve, lo reutilizamos
        if self._access_token and now < self._expires_at - 30:
            return self._access_token

        url = f"https://{AUTH0_DOMAIN}/oauth/token"
        payload = {
            "client_id": AUTH0_CLIENT_ID,
            "client_secret": AUTH0_CLIENT_SECRET,
            "audience": AUTH0_AUDIENCE,
            "grant_type": "client_credentials",
        }

        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        self._access_token = data["access_token"]
        # Auth0 manda "expires_in" en segundos
        self._expires_at = now + int(data.get("expires_in", 3600))

        return self._access_token

auth0_client = Auth0M2MClient()
