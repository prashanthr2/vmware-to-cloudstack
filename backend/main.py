from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

import requests
from fastapi import FastAPI, HTTPException

from .cloudstack import CloudStackClient
from .migration import MigrationManager
from .models import (
    MigrationSpecRequest,
    MigrationSpecResponse,
    MigrationStartRequest,
    MigrationStartResponse,
    MigrationStatusResponse,
    VMwareVMInfo,
)
from .vmware import VMwareClient

app = FastAPI(title="VMware to CloudStack Migration Backend", version="1.0.0")

vmware_client = VMwareClient.from_env()
cloudstack_client = CloudStackClient.from_env()
migration_manager = MigrationManager()


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
        spec_file = migration_manager.generate_spec(payload)
        return MigrationSpecResponse(
            vm_name=payload.vm_name,
            spec_file=str(spec_file),
            created_at=datetime.now(timezone.utc),
        )
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


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
