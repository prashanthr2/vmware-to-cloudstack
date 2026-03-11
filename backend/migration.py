from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union
from uuid import uuid4

import yaml

from .models import MigrationSpecRequest


@dataclass
class MigrationJob:
    job_id: str
    vm_name: str
    spec_file: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    return_code: Optional[int] = None
    error: Optional[str] = None


class MigrationManager:
    def __init__(
        self,
        base_dir: Union[str, Path],
        specs_dir: Union[str, Path],
        python_cmd: str,
        migrate_script: str,
        command_cwd: Union[str, Path],
        max_workers: int,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.specs_dir = Path(specs_dir)
        self.python_cmd = python_cmd
        self.migrate_script = migrate_script
        self.command_cwd = Path(command_cwd)

        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="migration-worker")

        self._lock = threading.Lock()
        self._jobs: dict[str, MigrationJob] = {}
        self._futures: dict[str, Future] = {}
        self._jobs_by_vm: dict[str, list[str]] = {}

    @classmethod
    def from_sources(cls, config: Optional[dict] = None) -> "MigrationManager":
        migration_cfg = (config or {}).get("migration", {})

        default_base_dir = migration_cfg.get("control_dir", "/var/lib/vm-migrator")
        base_dir = os.getenv("MIGRATOR_BASE_DIR", str(default_base_dir))

        default_specs_dir = migration_cfg.get("specs_dir", str(Path(base_dir) / "specs"))
        specs_dir = os.getenv("MIGRATOR_SPECS_DIR", str(default_specs_dir))

        python_cmd = os.getenv("MIGRATOR_PYTHON", str(migration_cfg.get("python_bin", sys.executable)))
        migrate_script = os.getenv("MIGRATOR_SCRIPT", str(migration_cfg.get("migrate_script", "migrate.py")))

        default_workdir = migration_cfg.get("workdir", os.getcwd())
        command_cwd = os.getenv("MIGRATOR_WORKDIR", str(default_workdir))

        default_workers = int(migration_cfg.get("parallel_vms", 4))
        max_workers = int(os.getenv("MIGRATOR_MAX_WORKERS", str(default_workers)))

        return cls(
            base_dir=base_dir,
            specs_dir=specs_dir,
            python_cmd=python_cmd,
            migrate_script=migrate_script,
            command_cwd=command_cwd,
            max_workers=max_workers,
        )

    @staticmethod
    def _safe_vm_name(vm_name: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]", "_", vm_name)

    def _latest_spec_for_vm(self, vm_name: str) -> Path:
        safe_name = self._safe_vm_name(vm_name)
        if not self.specs_dir.exists():
            raise FileNotFoundError(f"Spec directory does not exist: {self.specs_dir}")

        candidates = sorted(self.specs_dir.glob(f"{safe_name}-*.yaml"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            direct = self.specs_dir / f"{safe_name}.yaml"
            if direct.exists():
                return direct
            raise FileNotFoundError(f"No spec file found for VM '{vm_name}'.")

        return candidates[0]

    def _resolve_spec_file(self, vm_name: str, spec_file: Optional[str]) -> Path:
        if not spec_file:
            return self._latest_spec_for_vm(vm_name)

        raw = Path(spec_file)
        if raw.exists():
            return raw

        candidate = self.specs_dir / spec_file
        if candidate.exists():
            return candidate

        raise FileNotFoundError(f"Spec file not found: {spec_file}")

    def _validated_state_path(self, vm_name: str) -> Optional[Path]:
        candidates = [self.base_dir / vm_name / "state.json", self.base_dir / self._safe_vm_name(vm_name) / "state.json"]
        base_resolved = self.base_dir.resolve()

        for candidate in candidates:
            resolved = candidate.resolve()
            try:
                resolved.relative_to(base_resolved)
            except ValueError:
                continue
            if resolved.exists():
                return resolved

        return None

    def generate_spec(self, request: MigrationSpecRequest) -> Path:
        self.specs_dir.mkdir(parents=True, exist_ok=True)

        migration_block = request.migration.model_dump(exclude_none=True)

        spec_payload = {
            "vm": {"name": request.vm_name},
            "migration": migration_block,
            "target": {
                "cloudstack": {
                    "zoneid": request.zoneid,
                    "clusterid": request.clusterid,
                    "networkid": request.networkid,
                    "serviceofferingid": request.serviceofferingid,
                    "storageid": request.boot_storageid,
                }
            },
            "disks": {
                str(index): {
                    "storageid": disk.storageid,
                    "diskofferingid": disk.diskofferingid,
                }
                for index, disk in request.disks.items()
            },
        }

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        safe_name = self._safe_vm_name(request.vm_name)
        spec_path = self.specs_dir / f"{safe_name}-{timestamp}.yaml"

        with spec_path.open("w", encoding="utf-8") as stream:
            yaml.safe_dump(spec_payload, stream, sort_keys=False)

        return spec_path

    def start_migration(self, vm_name: str, spec_file: Optional[str] = None) -> MigrationJob:
        resolved_spec = self._resolve_spec_file(vm_name, spec_file)

        job_id = str(uuid4())
        job = MigrationJob(
            job_id=job_id,
            vm_name=vm_name,
            spec_file=str(resolved_spec),
            status="queued",
            started_at=datetime.now(timezone.utc),
        )

        with self._lock:
            self._jobs[job_id] = job
            self._jobs_by_vm.setdefault(vm_name, []).append(job_id)
            self._futures[job_id] = self.executor.submit(self._run_subprocess, job_id)

        return job

    def _run_subprocess(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.status = "running"

        vm_dir = self.base_dir / self._safe_vm_name(job.vm_name)
        vm_dir.mkdir(parents=True, exist_ok=True)

        stdout_path = vm_dir / f"{job.job_id}.stdout.log"
        stderr_path = vm_dir / f"{job.job_id}.stderr.log"

        command = [
            self.python_cmd,
            self.migrate_script,
            "--spec",
            job.spec_file,
        ]

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                cwd=str(self.command_cwd),
            )

            stdout_path.write_text(result.stdout or "", encoding="utf-8")
            stderr_path.write_text(result.stderr or "", encoding="utf-8")

            with self._lock:
                job.return_code = result.returncode
                job.finished_at = datetime.now(timezone.utc)
                if result.returncode == 0:
                    job.status = "completed"
                else:
                    job.status = "failed"
                    job.error = f"Migration exited with return code {result.returncode}."
        except Exception as exc:  # pragma: no cover - defensive runtime handling
            with self._lock:
                job.finished_at = datetime.now(timezone.utc)
                job.status = "failed"
                job.error = str(exc)

    def _latest_job_for_vm(self, vm_name: str) -> Optional[MigrationJob]:
        with self._lock:
            ids = self._jobs_by_vm.get(vm_name, [])
            if not ids:
                return None
            return self._jobs[ids[-1]]

    def get_status(self, vm_name: str) -> Optional[dict]:
        state_path = self._validated_state_path(vm_name)

        state_data: dict = {}
        if state_path is not None:
            try:
                state_data = json.loads(state_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                state_data = {}

        job = self._latest_job_for_vm(vm_name)

        if not state_data and job is None:
            return None

        return {
            "vm_name": vm_name,
            "stage": state_data.get("stage"),
            "progress": state_data.get("progress"),
            "disks": state_data.get("disks") or state_data.get("disk_status") or {},
            "job_id": job.job_id if job else None,
            "job_status": job.status if job else None,
            "updated_at": datetime.now(timezone.utc),
        }

