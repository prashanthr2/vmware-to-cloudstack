from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import requests
import yaml
from fastapi import FastAPI, HTTPException, Query
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

app = FastAPI(title="VMware to CloudStack Migration Backend", version="1.3.0")

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


def _resolve_vm_control_dir(payload: MigrationSpecRequest) -> str:
    safe_name = _safe_vm_name(payload.vm_name)

    if payload.vm_moref:
        return f"{safe_name}_{payload.vm_moref}"

    try:
        vm_list = vmware_client.list_vms()
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
def list_vmware_vms() -> list[dict[str, Any]]:
    try:
        return vmware_client.list_vms()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch VMs from VMware: {exc}") from exc


@app.get("/cloudstack/zones")
def list_cloudstack_zones() -> list[dict[str, Any]]:
    return _cloudstack_call(cloudstack_client.list_zones)


@app.get("/cloudstack/clusters")
def list_cloudstack_clusters() -> list[dict[str, Any]]:
    return _cloudstack_call(cloudstack_client.list_clusters)


@app.get("/cloudstack/storage")
def list_cloudstack_storage() -> list[dict[str, Any]]:
    return _cloudstack_call(cloudstack_client.list_storage)


@app.get("/cloudstack/networks")
def list_cloudstack_networks() -> list[dict[str, Any]]:
    return _cloudstack_call(cloudstack_client.list_networks)


@app.get("/cloudstack/diskofferings")
def list_cloudstack_disk_offerings() -> list[dict[str, Any]]:
    return _cloudstack_call(cloudstack_client.list_disk_offerings)


@app.get("/cloudstack/serviceofferings")
def list_cloudstack_service_offerings() -> list[dict[str, Any]]:
    return _cloudstack_call(cloudstack_client.list_service_offerings)


def _cloudstack_call(fn: Callable[[], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    try:
        return fn()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"CloudStack API request failed: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Unexpected CloudStack error: {exc}") from exc


@app.post("/migration/spec", response_model=MigrationSpecResponse)
def generate_migration_spec(payload: MigrationSpecRequest) -> MigrationSpecResponse:
    try:
        control_dir_name = _resolve_vm_control_dir(payload)
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

