import asyncio
import logging
import time
import base64
import os
import aiohttp
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

log = logging.getLogger("session_manager")

class SessionManager:
    def __init__(self, cfg):
        self._cfg = cfg
        self._http = None
        self._lock = asyncio.Lock()
        self._private_key = None

        try:
            key_path = os.path.expanduser(self._cfg.private_key_path)
            if not os.path.exists(key_path):
                raise FileNotFoundError(f"Private key file not found at {key_path}")
            with open(key_path, "rb") as f:
                self._private_key = load_pem_private_key(f.read(), password=None)
            log.info("Successfully loaded RSA private key for V2 signing.")
        except Exception as e:
            log.critical(f"CRITICAL: Failed to load private key from {self._cfg.private_key_path}: {e}")
            raise RuntimeError(f"Could not initialize SessionManager: {e}") from e

    async def __aenter__(self):
        # Apply the SSL bypass directly to the session connector
        connector = aiohttp.TCPConnector(ssl=True)
        self._http = aiohttp.ClientSession(connector=connector)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._http:
            await self._http.close()
        log.info("Session closed completely.")



    def _build_headers(self, method: str, full_path: str) -> dict[str, str]:
        """Generates dynamic V2 cryptographic headers signed via RSA-PSS padding."""
        timestamp = str(int(time.time() * 1000))
        
        # Ensure query parameters are stripped for signature validation
        clean_path = full_path.split("?")[0]
        
        if "ws" in clean_path:
            parsed_path = "/trade-api/ws/v2"
        else:
            stripped = clean_path.replace("/trade-api/v2", "").lstrip("/")
            parsed_path = f"/trade-api/v2/{stripped}"
        
        # Pre-image string validation layout
        msg = f"{timestamp}{method}{parsed_path}"
        
        signature_bytes = self._private_key.sign(
            msg.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH
            ),
            hashes.SHA256()
        )
        signature_b64 = base64.b64encode(signature_bytes).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": self._cfg.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature_b64,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "kalshi-hft-bot/1.0",
        }

    async def get_headers(self, method: str, path: str) -> dict[str, str]:
        return self._build_headers(method, path)

    def _resolve_base(self, ticker: str) -> str:
        if not ticker:
            return self._cfg.rest_base
        # Implement longest-prefix matching for shard routing
        for prefix in sorted(self._cfg.shard_routing.keys(), key=len, reverse=True):
            if ticker.startswith(prefix):
                return self._cfg.shard_routing[prefix]
        return self._cfg.shard_routing.get("DEFAULT", self._cfg.rest_base)

    async def get(self, path: str, ticker: str = "", params: dict = None, **kwargs):
        base = self._resolve_base(ticker)
        headers = await self.get_headers("GET", path)
        
        # Format URL mapping dynamically
        base_clean = base.rstrip('/')
        path_clean = path.lstrip('/')
        url = f"{base_clean}/{path_clean}"
        
        async with self._http.get(url, headers=headers, params=params) as r:
            if r.status >= 400:
                body = await r.text()
                raise Exception(f"HTTP {r.status}: {body} on path {path}")
            return await r.json()

    async def post(self, path: str, ticker: str = "", payload: dict = None, **kwargs):
        base = self._resolve_base(ticker)
        headers = await self.get_headers("POST", path)
        base_clean = base.rstrip('/')
        path_clean = path.lstrip('/')
        url = f"{base_clean}/{path_clean}"
        
        async with self._http.post(url, headers=headers, json=payload or {}) as r:
            if r.status >= 400:
                body = await r.text()
                raise Exception(f"HTTP {r.status}: {body} on path {path}")
            return await r.json()

    async def delete(self, path: str, ticker: str = "", **kwargs):
        base = self._resolve_base(ticker)
        headers = await self.get_headers("DELETE", path)
        base_clean = base.rstrip('/')
        path_clean = path.lstrip('/')
        url = f"{base_clean}/{path_clean}"
        
        async with self._http.delete(url, headers=headers) as r:
            if r.status >= 400:
                body = await r.text()
                raise Exception(f"HTTP {r.status}: {body} on path {path}")
            return await r.json()

    @property
    def session(self):
        return self._http
