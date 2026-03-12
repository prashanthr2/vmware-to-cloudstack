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
        self._speed_samples: dict[str, tuple[float, int]] = {}

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

    @staticmethod
    def _safe_int(value) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _format_bytes(num_bytes: int) -> str:
        value = float(max(num_bytes, 0))
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if value < 1024.0 or unit == "TB":
                if unit == "B":
                    return f"{int(value)} {unit}"
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{num_bytes} B"

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

    def _load_state(self, vm_name: str) -> dict:
        state_path = self._validated_state_path(vm_name)
        if state_path is None:
            return {}

        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

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

    @staticmethod
    def _tail_file(path: Path, lines: int) -> str:
        if not path.exists() or lines <= 0:
            return ""

        text = path.read_text(encoding="utf-8", errors="replace")
        parts = text.splitlines()
        if len(parts) <= lines:
            return text

        return "\n".join(parts[-lines:])

    def _latest_job_for_vm(self, vm_name: str) -> Optional[MigrationJob]:
        with self._lock:
            ids = self._jobs_by_vm.get(vm_name, [])
            if not ids:
                return None
            return self._jobs[ids[-1]]

    def _build_disk_progress(
        self, vm_name: str, state_data: dict, stage: Optional[str] = None
    ) -> tuple[list[dict], int, int, Optional[float]]:
        raw_disks = state_data.get("disks")
        if not isinstance(raw_disks, dict):
            return [], 0, 0, None

        def first_int(values) -> int:
            for value in values:
                parsed = self._safe_int(value)
                if parsed > 0:
                    return parsed
            return 0

        def first_float(values) -> Optional[float]:
            for value in values:
                try:
                    parsed = float(value)
                    if parsed >= 0:
                        return parsed
                except (TypeError, ValueError):
                    continue
            return None

        boot_unit = state_data.get("boot_unit")
        boot_unit_str = str(boot_unit) if boot_unit is not None else None

        now_ts = datetime.now(timezone.utc).timestamp()
        stage_norm = (stage or "").lower()
        use_delta_metrics = stage_norm in {"delta", "final_sync", "waiting_finalize"}

        disk_progress: list[dict] = []
        total_bytes = 0
        copied_bytes_total = 0
        speed_values: list[float] = []

        for unit, disk in raw_disks.items():
            if not isinstance(disk, dict):
                continue

            unit_str = str(unit)
            capacity = first_int([
                disk.get("capacity"),
                disk.get("total_bytes"),
                disk.get("size_bytes"),
                disk.get("bytes_total"),
                disk.get("size"),
            ])
            path = disk.get("path")

            copied = first_int([
                disk.get("copied_bytes"),
                disk.get("copied"),
                disk.get("transferred_bytes"),
                disk.get("written_bytes"),
                disk.get("bytes_done"),
                disk.get("copied_size"),
                disk.get("bytes_written"),
            ])

            if copied <= 0 and isinstance(path, str) and path:
                try:
                    copied = os.path.getsize(path)
                except OSError:
                    copied = 0

            if capacity > 0 and copied > capacity:
                copied = capacity

            qemu_progress = first_float([
                disk.get("qemu_progress"),
                disk.get("read_progress"),
                disk.get("scan_progress"),
            ])
            progress_raw = disk.get("progress")

            delta_total = first_int([
                disk.get("delta_total_bytes"),
                disk.get("delta_total"),
                disk.get("changed_bytes_total"),
            ])
            delta_copied = first_int([
                disk.get("delta_bytes_written"),
                disk.get("delta_copied_bytes"),
                disk.get("changed_bytes_copied"),
            ])
            if delta_total > 0 and delta_copied > delta_total:
                delta_copied = delta_total

            used_total = first_int([
                disk.get("estimated_used_bytes"),
                disk.get("read_total_bytes"),
                disk.get("used_bytes"),
            ])

            read_total = 0
            read_copied = 0
            read_remaining = 0
            read_progress = None

            if use_delta_metrics and delta_total > 0:
                read_total = delta_total
                read_copied = delta_copied
                read_remaining = max(read_total - read_copied, 0)

                delta_progress_raw = disk.get("delta_progress")
                if isinstance(delta_progress_raw, (int, float)):
                    read_progress = float(delta_progress_raw)
                elif read_total > 0:
                    read_progress = round((read_copied / read_total) * 100, 2)
            else:
                if used_total <= 0 and copied > 0 and qemu_progress is not None and qemu_progress > 0:
                    ratio = max(min(qemu_progress, 100.0), 0.01) / 100.0
                    used_total = int(copied / ratio)

                if used_total <= 0 and copied > 0 and qemu_progress is not None and qemu_progress >= 99.9:
                    used_total = copied

                if capacity > 0 and used_total > capacity:
                    used_total = capacity

                if used_total > 0:
                    used_total = max(used_total, copied)

                read_total = used_total
                read_copied = copied
                read_remaining = max(read_total - read_copied, 0) if read_total > 0 else 0

                if qemu_progress is not None:
                    read_progress = float(qemu_progress)
                elif isinstance(progress_raw, (int, float)):
                    read_progress = float(progress_raw)
                elif read_total > 0:
                    read_progress = round((read_copied / read_total) * 100, 2)

            sample_key = f"{vm_name}:{unit_str}"
            speed_mbps = first_float([
                disk.get("speed_mbps"),
                disk.get("throughput_mbps"),
                disk.get("transfer_speed_mbps"),
            ])
            eta_seconds = first_int([disk.get("eta_seconds"), disk.get("eta")]) or None

            sample_copied = read_copied if read_total > 0 else copied
            if speed_mbps is None:
                previous = self._speed_samples.get(sample_key)
                if previous and sample_copied >= previous[1]:
                    delta_t = now_ts - previous[0]
                    if delta_t > 0:
                        speed_bps = (sample_copied - previous[1]) / delta_t
                        if speed_bps > 0:
                            speed_mbps = round(speed_bps / (1024 * 1024), 2)
                            if read_remaining > 0 and eta_seconds is None:
                                eta_seconds = int(read_remaining / speed_bps)

            if speed_mbps is not None and speed_mbps > 0:
                speed_values.append(speed_mbps)

            self._speed_samples[sample_key] = (now_ts, sample_copied)

            disk_type = disk.get("disk_type")
            if not disk_type:
                disk_type = "os" if boot_unit_str is not None and unit_str == boot_unit_str else "data"

            disk_progress.append(
                {
                    "unit": unit_str,
                    "disk_name": disk.get("label") or f"disk{unit_str}",
                    "disk_type": disk_type,
                    "datastore": disk.get("datastore"),
                    "provisioned_size": self._format_bytes(capacity) if capacity > 0 else None,
                    "provisioned_bytes": capacity if capacity > 0 else None,
                    "used_size": self._format_bytes(read_total) if read_total > 0 else None,
                    "used_bytes": read_total if read_total > 0 else None,
                    "total_size": self._format_bytes(capacity) if capacity > 0 else None,
                    "copied_size": self._format_bytes(read_copied),
                    "remaining_size": self._format_bytes(read_remaining) if read_total > 0 else None,
                    "total_bytes": capacity if capacity > 0 else None,
                    "copied_bytes": read_copied,
                    "remaining_bytes": read_remaining if read_total > 0 else None,
                    "speed_mbps": speed_mbps,
                    "eta_seconds": eta_seconds,
                    "progress": read_progress,
                }
            )

            summary_total = read_total if read_total > 0 else capacity
            summary_copied = read_copied if read_total > 0 else copied
            if summary_total > 0:
                total_bytes += summary_total
                copied_bytes_total += min(summary_copied, summary_total)

        disk_progress.sort(key=lambda d: self._safe_int(d.get("unit")))

        transfer_speed_mbps = first_float([
            state_data.get("transfer_speed_mbps"),
            state_data.get("speed_mbps"),
            state_data.get("throughput_mbps"),
        ])
        if transfer_speed_mbps is None and speed_values:
            transfer_speed_mbps = round(sum(speed_values), 2)

        return disk_progress, total_bytes, copied_bytes_total, transfer_speed_mbps

    def _build_status_payload(self, vm_name: str, state_data: dict, job: Optional[MigrationJob]) -> dict:
        stage = state_data.get("stage")
        stage_norm = (stage or "").lower()
        use_delta_metrics = stage_norm in {"delta", "final_sync", "waiting_finalize"}

        disk_progress, total_bytes, copied_bytes_total, transfer_speed_mbps = self._build_disk_progress(
            vm_name, state_data, stage=stage
        )

        state_progress = state_data.get("progress")

        overall_progress = None
        if use_delta_metrics and total_bytes > 0:
            overall_progress = round((copied_bytes_total / total_bytes) * 100, 2)
        elif isinstance(state_progress, (int, float)):
            overall_progress = float(state_progress)
        elif total_bytes > 0:
            overall_progress = round((copied_bytes_total / total_bytes) * 100, 2)
        elif stage == "done" or (job and job.status == "completed"):
            overall_progress = 100.0

        if use_delta_metrics:
            progress = overall_progress
        else:
            progress = state_progress if isinstance(state_progress, (int, float)) else overall_progress

        return {
            "vm_name": vm_name,
            "stage": stage,
            "progress": progress,
            "overall_progress": overall_progress,
            "transfer_speed_mbps": transfer_speed_mbps,
            "disks": state_data.get("disks") or state_data.get("disk_status") or {},
            "disk_progress": disk_progress,
            "job_id": job.job_id if job else None,
            "job_status": job.status if job else None,
            "job_error": job.error if job else None,
            "return_code": job.return_code if job else None,
            "updated_at": datetime.now(timezone.utc),
        }

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
            with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open(
                "w", encoding="utf-8"
            ) as stderr_file:
                stdout_file.write("$ " + " ".join(command) + "\n")
                stdout_file.flush()

                process = subprocess.Popen(
                    command,
                    stdout=stdout_file,
                    stderr=stderr_file,
                    text=True,
                    cwd=str(self.command_cwd),
                )
                return_code = process.wait()

            stderr_text = stderr_path.read_text(encoding="utf-8", errors="replace") if stderr_path.exists() else ""

            with self._lock:
                job.return_code = return_code
                job.finished_at = datetime.now(timezone.utc)
                if return_code == 0:
                    job.status = "completed"
                    job.error = None
                else:
                    job.status = "failed"
                    stderr_preview = (stderr_text or "").strip()
                    if stderr_preview:
                        job.error = stderr_preview[-1000:]
                    else:
                        job.error = f"Migration exited with return code {return_code}."
        except Exception as exc:  # pragma: no cover - defensive runtime handling
            with self._lock:
                job.finished_at = datetime.now(timezone.utc)
                job.status = "failed"
                job.error = str(exc)

    def get_status(self, vm_name: str) -> Optional[dict]:
        state_data = self._load_state(vm_name)
        job = self._latest_job_for_vm(vm_name)

        if not state_data and job is None:
            return None

        return self._build_status_payload(vm_name, state_data, job)

    def list_jobs(self, vm_name: Optional[str] = None, limit: int = 100) -> list[dict]:
        with self._lock:
            jobs = list(self._jobs.values())

        if vm_name:
            jobs = [job for job in jobs if job.vm_name == vm_name]

        jobs.sort(key=lambda j: j.started_at, reverse=True)

        result = []
        for job in jobs[:limit]:
            state_data = self._load_state(job.vm_name)
            progress_payload = self._build_status_payload(job.vm_name, state_data, job)
            result.append(
                {
                    "job_id": job.job_id,
                    "vm_name": job.vm_name,
                    "status": job.status,
                    "spec_file": job.spec_file,
                    "started_at": job.started_at,
                    "finished_at": job.finished_at,
                    "return_code": job.return_code,
                    "error": job.error,
                    "stage": progress_payload.get("stage"),
                    "progress": progress_payload.get("overall_progress") or progress_payload.get("progress"),
                }
            )

        return result

    def create_finalize_marker(self, vm_name: str) -> Path:
        latest_job = self._latest_job_for_vm(vm_name)

        if latest_job:
            target_dir = self._job_runtime_dir(latest_job.vm_name, latest_job.spec_file)
        else:
            dirs = self._candidate_vm_dirs(vm_name)
            if not dirs:
                raise FileNotFoundError(f"Control directory not found for VM '{vm_name}'.")
            target_dir = dirs[0]

        target_dir.mkdir(parents=True, exist_ok=True)
        finalize_file = target_dir / "FINALIZE"
        finalize_file.touch(exist_ok=True)
        return finalize_file

    def get_logs(self, vm_name: str, lines: int = 200, job_id: Optional[str] = None) -> dict:
        latest_job = self._latest_job_for_vm(vm_name)
        target_dir = None
        resolved_job_id = job_id

        if job_id:
            with self._lock:
                explicit_job = self._jobs.get(job_id)
            if explicit_job and explicit_job.vm_name == vm_name:
                target_dir = self._job_runtime_dir(explicit_job.vm_name, explicit_job.spec_file)
                resolved_job_id = explicit_job.job_id

        if target_dir is None and latest_job is not None:
            target_dir = self._job_runtime_dir(latest_job.vm_name, latest_job.spec_file)
            resolved_job_id = latest_job.job_id

        if target_dir is None:
            dirs = self._candidate_vm_dirs(vm_name)
            target_dir = dirs[0] if dirs else self.base_dir / self._safe_vm_name(vm_name)

        stdout_path = None
        stderr_path = None

        if resolved_job_id:
            stdout_candidate = target_dir / f"{resolved_job_id}.stdout.log"
            stderr_candidate = target_dir / f"{resolved_job_id}.stderr.log"
            if stdout_candidate.exists():
                stdout_path = stdout_candidate
            if stderr_candidate.exists():
                stderr_path = stderr_candidate

        if stdout_path is None:
            candidates = sorted(target_dir.glob("*.stdout.log"), key=lambda p: p.stat().st_mtime, reverse=True)
            if candidates:
                stdout_path = candidates[0]

        if stderr_path is None:
            candidates = sorted(target_dir.glob("*.stderr.log"), key=lambda p: p.stat().st_mtime, reverse=True)
            if candidates:
                stderr_path = candidates[0]

        return {
            "vm_name": vm_name,
            "job_id": resolved_job_id,
            "stdout_path": str(stdout_path) if stdout_path else None,
            "stderr_path": str(stderr_path) if stderr_path else None,
            "stdout": self._tail_file(stdout_path, lines) if stdout_path else "",
            "stderr": self._tail_file(stderr_path, lines) if stderr_path else "",
        }
