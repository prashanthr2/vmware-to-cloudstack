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

    def _candidate_vm_dirs(self, vm_name: str) -> list[Path]:
        safe_name = self._safe_vm_name(vm_name)
        dirs: list[Path] = []

        pattern_dirs = list(self.base_dir.glob(f"{safe_name}_*"))
        if vm_name != safe_name:
            pattern_dirs.extend(self.base_dir.glob(f"{vm_name}_*"))

        pattern_dirs = [d for d in pattern_dirs if d.is_dir()]
        pattern_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)

        for item in pattern_dirs:
            if item not in dirs:
                dirs.append(item)

        primary = self.base_dir / safe_name
        if primary.exists() and primary.is_dir() and primary not in dirs:
            dirs.append(primary)

        if vm_name != safe_name:
            raw = self.base_dir / vm_name
            if raw.exists() and raw.is_dir() and raw not in dirs:
                dirs.append(raw)

        return dirs

    def _latest_spec_for_vm(self, vm_name: str) -> Path:
        for vm_dir in self._candidate_vm_dirs(vm_name):
            direct_spec = vm_dir / "spec.yaml"
            if direct_spec.exists():
                return direct_spec

            latest_spec = vm_dir / "spec.latest.yaml"
            if latest_spec.exists():
                return latest_spec

            specs_subdir = vm_dir / "specs"
            if specs_subdir.exists() and specs_subdir.is_dir():
                candidates = sorted(specs_subdir.glob("*.yaml"), key=lambda p: p.stat().st_mtime, reverse=True)
                if candidates:
                    return candidates[0]

            root_candidates = sorted(vm_dir.glob("*.yaml"), key=lambda p: p.stat().st_mtime, reverse=True)
            if root_candidates:
                return root_candidates[0]

        # Backward compatibility for old global specs directory.
        safe_name = self._safe_vm_name(vm_name)
        if self.specs_dir.exists():
            legacy = sorted(self.specs_dir.glob(f"{safe_name}-*.yaml"), key=lambda p: p.stat().st_mtime, reverse=True)
            if legacy:
                return legacy[0]

            direct = self.specs_dir / f"{safe_name}.yaml"
            if direct.exists():
                return direct

        raise FileNotFoundError(f"No spec file found for VM '{vm_name}'.")

    def _resolve_spec_file(self, vm_name: str, spec_file: Optional[str]) -> Path:
        if not spec_file:
            return self._latest_spec_for_vm(vm_name)

        raw = Path(spec_file)
        if raw.exists():
            return raw

        candidate = self.specs_dir / spec_file
        if candidate.exists():
            return candidate

        for vm_dir in self._candidate_vm_dirs(vm_name):
            vm_candidate = vm_dir / spec_file
            if vm_candidate.exists():
                return vm_candidate

            vm_specs_candidate = vm_dir / "specs" / spec_file
            if vm_specs_candidate.exists():
                return vm_specs_candidate

        raise FileNotFoundError(f"Spec file not found: {spec_file}")

    def _validated_state_path(self, vm_name: str) -> Optional[Path]:
        base_resolved = self.base_dir.resolve()
        candidates = [vm_dir / "state.json" for vm_dir in self._candidate_vm_dirs(vm_name)]

        for candidate in candidates:
            resolved = candidate.resolve()
            try:
                resolved.relative_to(base_resolved)
            except ValueError:
                continue
            if resolved.exists():
                return resolved

        return None

    def _job_runtime_dir(self, vm_name: str, spec_file: str) -> Path:
        spec_path = Path(spec_file)
        try:
            resolved_spec = spec_path.resolve()
            base_resolved = self.base_dir.resolve()
            rel = resolved_spec.relative_to(base_resolved)

            if len(rel.parts) >= 2 and rel.parts[1] == "specs":
                return base_resolved / rel.parts[0]

            if len(rel.parts) >= 2:
                return base_resolved / rel.parts[0]
        except Exception:
            pass

        dirs = self._candidate_vm_dirs(vm_name)
        if dirs:
            return dirs[0]

        return self.base_dir / self._safe_vm_name(vm_name)

    def generate_spec(self, request: MigrationSpecRequest, control_dir_name: Optional[str] = None) -> Path:
        dir_name = control_dir_name or self._safe_vm_name(request.vm_name)
        vm_dir = self.base_dir / dir_name
        vm_dir.mkdir(parents=True, exist_ok=True)

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

        spec_path = vm_dir / "spec.yaml"
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

        vm_dir = self._job_runtime_dir(job.vm_name, job.spec_file)
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
                    job.error = None
                else:
                    job.status = "failed"
                    stderr_preview = (result.stderr or "").strip()
                    if stderr_preview:
                        job.error = stderr_preview[-1000:]
                    else:
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
            "job_error": job.error if job else None,
            "return_code": job.return_code if job else None,
            "updated_at": datetime.now(timezone.utc),
        }

