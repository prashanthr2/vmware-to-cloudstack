from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import requests
import yaml
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from .cloudstack import CloudStackClient
from .migration import MigrationManager
from .models import (
    MigrationFinalizeResponse,
    MigrationJobInfo,
    MigrationLogsResponse,
    MigrationSpecRequest,
    MigrationSpecResponse,
    MigrationStartRequest,
    MigrationStartResponse,
    MigrationStatusResponse,
    VMwareVMInfo,
)
from .vmware import VMwareClient

app = FastAPI(title="VMware to CloudStack Migration Backend", version="1.4.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("MIGRATOR_CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _load_runtime_config() -> dict[str, Any]:
    config_path = Path(os.getenv("MIGRATOR_CONFIG_FILE", "config.yaml"))
    if not config_path.exists():
        return {}

    try:
        with config_path.open("r", encoding="utf-8") as stream:
            data = yaml.safe_load(stream) or {}
            if not isinstance(data, dict):
                raise ValueError("config.yaml root must be a mapping/object")
            return data
    except Exception as exc:
        raise RuntimeError(f"Failed to read config file '{config_path}': {exc}") from exc


def _safe_vm_name(vm_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", vm_name)


def _parse_bool(raw: Optional[str], default: bool = False) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_vcenter_host(raw: str) -> tuple[str, Optional[int]]:
    value = raw.strip()
    if not value:
        return "", None

    if "://" in value:
        parsed = urlparse(value)
        host = parsed.hostname or ""
        return host, parsed.port

    host = value.split("/")[0]
    if host.count(":") == 1:
        left, right = host.split(":", 1)
        if right.isdigit():
            return left, int(right)

    return host, None


def _vmware_client_for_request(request: Request) -> VMwareClient:
    header_host = request.headers.get("x-vcenter-host")
    if not header_host:
        return vmware_client

    host, parsed_port = _parse_vcenter_host(header_host)
    username = request.headers.get("x-vcenter-user", "")
    password = request.headers.get("x-vcenter-password", "")

    if not host or not username or not password:
        raise HTTPException(
            status_code=400,
            detail="Selected vCenter environment is missing host, username, or password.",
        )

    port_raw = request.headers.get("x-vcenter-port")
    try:
        port = int(port_raw) if port_raw else (parsed_port or 443)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid x-vcenter-port header.") from exc

    verify_ssl = _parse_bool(request.headers.get("x-vcenter-verify-ssl"), default=False)

    return VMwareClient(
        host=host,
        username=username,
        password=password,
        port=port,
        verify_ssl=verify_ssl,
    )


def _cloudstack_client_for_request(request: Request) -> CloudStackClient:
    endpoint = request.headers.get("x-cloudstack-endpoint")
    if not endpoint:
        return cloudstack_client

    api_key = request.headers.get("x-cloudstack-api-key", "")
    secret_key = request.headers.get("x-cloudstack-secret-key", "")

    if not api_key or not secret_key:
        raise HTTPException(
            status_code=400,
            detail="Selected CloudStack environment is missing API key or secret key.",
        )

    timeout_raw = request.headers.get("x-cloudstack-timeout-seconds")
    try:
        timeout_seconds = int(timeout_raw) if timeout_raw else 30
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid x-cloudstack-timeout-seconds header.") from exc

    return CloudStackClient(
        endpoint=endpoint,
        api_key=api_key,
        secret_key=secret_key,
        timeout_seconds=timeout_seconds,
    )


def _resolve_vm_control_dir(payload: MigrationSpecRequest, request: Request) -> str:
    safe_name = _safe_vm_name(payload.vm_name)

    if payload.vm_moref:
        return f"{safe_name}_{payload.vm_moref}"

    try:
        vm_list = _vmware_client_for_request(request).list_vms()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unable to resolve VM MoRef automatically. Provide vm_moref in the request "
                f"or fix VMware connectivity. Error: {exc}"
            ),
        ) from exc

    matches = [vm for vm in vm_list if vm.get("name") == payload.vm_name]
    if not matches:
        raise HTTPException(status_code=404, detail=f"VM '{payload.vm_name}' not found in VMware.")

    if len(matches) > 1:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Multiple VMware VMs named '{payload.vm_name}' found. "
                "Please pass vm_moref in /migration/spec request."
            ),
        )

    moref = matches[0].get("moref")
    if not moref:
        raise HTTPException(
            status_code=400,
            detail=f"VM '{payload.vm_name}' is missing MoRef in VMware response. Pass vm_moref explicitly.",
        )

    return f"{safe_name}_{moref}"


runtime_config = _load_runtime_config()
vmware_client = VMwareClient.from_sources(runtime_config)
cloudstack_client = CloudStackClient.from_sources(runtime_config)
migration_manager = MigrationManager.from_sources(runtime_config)


@app.get("/vmware/vms", response_model=list[VMwareVMInfo])
def list_vmware_vms(request: Request) -> list[dict[str, Any]]:
    try:
        return _vmware_client_for_request(request).list_vms()
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch VMs from VMware: {exc}") from exc


@app.get("/cloudstack/zones")
def list_cloudstack_zones(request: Request) -> list[dict[str, Any]]:
    return _cloudstack_call(_cloudstack_client_for_request(request).list_zones)


@app.get("/cloudstack/clusters")
def list_cloudstack_clusters(request: Request) -> list[dict[str, Any]]:
    return _cloudstack_call(_cloudstack_client_for_request(request).list_clusters)


@app.get("/cloudstack/storage")
def list_cloudstack_storage(request: Request) -> list[dict[str, Any]]:
    return _cloudstack_call(_cloudstack_client_for_request(request).list_storage)


@app.get("/cloudstack/networks")
def list_cloudstack_networks(request: Request) -> list[dict[str, Any]]:
    return _cloudstack_call(_cloudstack_client_for_request(request).list_networks)


@app.get("/cloudstack/diskofferings")
def list_cloudstack_disk_offerings(request: Request) -> list[dict[str, Any]]:
    return _cloudstack_call(_cloudstack_client_for_request(request).list_disk_offerings)


@app.get("/cloudstack/serviceofferings")
def list_cloudstack_service_offerings(request: Request) -> list[dict[str, Any]]:
    return _cloudstack_call(_cloudstack_client_for_request(request).list_service_offerings)


def _cloudstack_call(fn: Callable[[], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    try:
        return fn()
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"CloudStack API request failed: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected CloudStack error: {exc}") from exc


@app.post("/migration/spec", response_model=MigrationSpecResponse)
def generate_migration_spec(payload: MigrationSpecRequest, request: Request) -> MigrationSpecResponse:
    try:
        control_dir_name = _resolve_vm_control_dir(payload, request)
        spec_file = migration_manager.generate_spec(payload, control_dir_name=control_dir_name)
        return MigrationSpecResponse(
            vm_name=payload.vm_name,
            spec_file=str(spec_file),
            created_at=datetime.now(timezone.utc),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to generate spec: {exc}") from exc


@app.post("/migration/start", response_model=MigrationStartResponse)
def start_migration(payload: MigrationStartRequest) -> MigrationStartResponse:
    try:
        job = migration_manager.start_migration(vm_name=payload.vm_name, spec_file=payload.spec_file)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to start migration: {exc}") from exc

    return MigrationStartResponse(
        vm_name=job.vm_name,
        job_id=job.job_id,
        spec_file=job.spec_file,
        status=job.status,
    )


@app.get("/migration/status/{vm}", response_model=MigrationStatusResponse)
def migration_status(vm: str) -> MigrationStatusResponse:
    status = migration_manager.get_status(vm)
    if status is None:
        raise HTTPException(status_code=404, detail=f"No migration state found for VM '{vm}'.")

    return MigrationStatusResponse(**status)


@app.get("/migration/jobs", response_model=list[MigrationJobInfo])
def list_migration_jobs(vm: Optional[str] = None, limit: int = Query(default=100, ge=1, le=1000)) -> list[MigrationJobInfo]:
    jobs = migration_manager.list_jobs(vm_name=vm, limit=limit)
    return [MigrationJobInfo(**job) for job in jobs]


@app.post("/migration/finalize/{vm}", response_model=MigrationFinalizeResponse)
def request_finalize(vm: str) -> MigrationFinalizeResponse:
    try:
        marker = migration_manager.create_finalize_marker(vm)
        return MigrationFinalizeResponse(vm_name=vm, finalize_file=str(marker), message="Finalize marker created")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create finalize marker: {exc}") from exc


@app.get("/migration/logs/{vm}", response_model=MigrationLogsResponse)
def migration_logs(
    vm: str,
    lines: int = Query(default=200, ge=10, le=2000),
    job_id: Optional[str] = None,
) -> MigrationLogsResponse:
    try:
        logs = migration_manager.get_logs(vm_name=vm, lines=lines, job_id=job_id)
        return MigrationLogsResponse(**logs)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read logs: {exc}") from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
