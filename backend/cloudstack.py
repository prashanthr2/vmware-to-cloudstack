from __future__ import annotations

import base64
import hashlib
import hmac
import os
from typing import Optional
from urllib.parse import quote_plus

import requests
from requests import Response


class CloudStackClient:
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        secret_key: str,
        timeout_seconds: int = 30,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.secret_key = secret_key
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_sources(cls, config: Optional[dict] = None) -> "CloudStackClient":
        cfg = (config or {}).get("cloudstack", {})

        endpoint = os.getenv("CLOUDSTACK_ENDPOINT", cfg.get("endpoint", ""))
        api_key = os.getenv("CLOUDSTACK_API_KEY", cfg.get("api_key", ""))
        secret_key = os.getenv("CLOUDSTACK_SECRET_KEY", cfg.get("secret_key", ""))

        cfg_timeout = cfg.get("timeout_seconds", 30)
        timeout_seconds = int(os.getenv("CLOUDSTACK_TIMEOUT_SECONDS", str(cfg_timeout)))

        return cls(
            endpoint=endpoint,
            api_key=api_key,
            secret_key=secret_key,
            timeout_seconds=timeout_seconds,
        )

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        value = (endpoint or "").strip().rstrip("/")
        if not value:
            return value

        lowered = value.lower()
        if lowered.endswith("/client/api"):
            return value

        if lowered.endswith("/client"):
            return f"{value}/api"

        if lowered.endswith("/api"):
            return value

        return f"{value}/client/api"

    def _validate_config(self) -> None:
        if not self.endpoint or not self.api_key or not self.secret_key:
            raise ValueError(
                "CloudStack config is missing. Configure cloudstack in config.yaml or set CLOUDSTACK_ENDPOINT/CLOUDSTACK_API_KEY/CLOUDSTACK_SECRET_KEY."
            )

    @staticmethod
    def _safe_json(response: Response, endpoint: str, command: str) -> dict:
        try:
            payload = response.json()
        except ValueError as exc:
            body = (response.text or "").strip().replace("\n", " ")
            snippet = body[:180] if body else "<empty response>"
            raise ValueError(
                f"CloudStack returned non-JSON response for {command} at {endpoint} "
                f"(status={response.status_code}). Response starts with: {snippet}"
            ) from exc

        if not isinstance(payload, dict):
            raise ValueError(f"CloudStack response for {command} is not a JSON object.")

        return payload

    def _sign(self, params: dict[str, str]) -> str:
        ordered = sorted((key.lower(), value) for key, value in params.items())
        canonical = "&".join(f"{k}={quote_plus(str(v)).lower()}" for k, v in ordered)

        digest = hmac.new(
            self.secret_key.encode("utf-8"),
            canonical.encode("utf-8"),
            hashlib.sha1,
        ).digest()

        return base64.b64encode(digest).decode("utf-8")

    def _call(self, command: str, **extra_params):
        self._validate_config()
        endpoint = self._normalize_endpoint(self.endpoint)

        params = {
            "command": command,
            "apikey": self.api_key,
            "response": "json",
        }
        params.update({k: str(v) for k, v in extra_params.items() if v is not None})
        params["signature"] = self._sign(params)

        response = requests.get(endpoint, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()

        payload = self._safe_json(response, endpoint, command)
        if "errorresponse" in payload:
            error = payload.get("errorresponse", {})
            message = error.get("errortext") or error.get("errorcode") or "Unknown CloudStack API error"
            raise ValueError(f"CloudStack API error for {command}: {message}")

        return payload

    def _extract_list(self, payload: dict, command: str, key: str) -> list[dict]:
        response_key = f"{command.lower()}response"
        response_section = payload.get(response_key, {})
        return response_section.get(key, [])

    def list_zones(self) -> list[dict]:
        payload = self._call("listZones")
        return self._extract_list(payload, "listZones", "zone")

    def list_clusters(self) -> list[dict]:
        payload = self._call("listClusters")
        return self._extract_list(payload, "listClusters", "cluster")

    def list_storage(self) -> list[dict]:
        payload = self._call("listStoragePools")
        return self._extract_list(payload, "listStoragePools", "storagepool")

    def list_networks(self) -> list[dict]:
        payload = self._call("listNetworks")
        return self._extract_list(payload, "listNetworks", "network")

    def list_disk_offerings(self) -> list[dict]:
        payload = self._call("listDiskOfferings")
        return self._extract_list(payload, "listDiskOfferings", "diskoffering")

    def list_service_offerings(self) -> list[dict]:
        payload = self._call("listServiceOfferings")
        return self._extract_list(payload, "listServiceOfferings", "serviceoffering")

