from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, Field


class VMwareDiskInfo(BaseModel):
    label: str
    size_gb: float
    datastore: Optional[str] = None


class VMwareVMInfo(BaseModel):
    name: str
    moref: str
    cpu: int
    memory: int
    disks: list[VMwareDiskInfo] = Field(default_factory=list)
    datastore: list[str] = Field(default_factory=list)


class DiskSpecInput(BaseModel):
    storageid: str
    diskofferingid: str


class MigrationSpecRequest(BaseModel):
    vm_name: str
    zoneid: str
    clusterid: str
    networkid: str
    serviceofferingid: str
    boot_storageid: str
    disks: dict[str, DiskSpecInput] = Field(default_factory=dict)


class MigrationSpecResponse(BaseModel):
    vm_name: str
    spec_file: str
    created_at: datetime


class MigrationStartRequest(BaseModel):
    vm_name: str
    spec_file: Optional[str] = None


class MigrationStartResponse(BaseModel):
    vm_name: str
    job_id: str
    spec_file: str
    status: str


class MigrationStatusResponse(BaseModel):
    vm_name: str
    stage: Optional[str] = None
    progress: Optional[Union[float, int]] = None
    disks: dict[str, Any] = Field(default_factory=dict)
    job_id: Optional[str] = None
    job_status: Optional[str] = None
    updated_at: Optional[datetime] = None
