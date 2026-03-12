import os
import sys
import ssl
import time
import json
import hashlib
import argparse
import traceback
import subprocess
import nbd
import yaml
import random
import requests
import base64
import hmac
import urllib.parse
import string
import threading
import select
import fcntl
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim

'''VCENTER = '10.0.35.3'
VCUSER = 'administrator@vsphere.local'
VCPASS = 'P@ssword123'
DATA = '/mnt/68f38850-a051-3ff1-9865-6a24a2ba2864'
VDDK = "/opt/vmware-vddk/vmware-vix-disklib-distrib/"
STATE_FILE = f"{DATA}/migration_state.json"'''

CONFIG_FILE = "config.yaml"
CONTROL_DIR = "/var/lib/vm-migrator"

def load_config():
    with open(CONFIG_FILE) as f:
        return yaml.safe_load(f)

config = load_config()

VCENTER = config["vcenter"]["host"]
VCUSER = config["vcenter"]["user"]
VCPASS = config["vcenter"]["password"]

#DATA = config["migration"]["data_dir"]
VDDK = config["migration"]["vddk_path"]


def migrate_vm(vmname):

    log(f"Starting migration for {vmname}")

    mig = Migrator(vmname)
    
    log(f"Stage: {mig.state['stage']}")
    log(f"VMware Tools status: {mig.vm.guest.toolsStatus}")

    while True:

        stage = mig.state["stage"]
        log(f"Stage: {stage}")

        if stage == MigrationStage.INIT:

            mig.ensure_cbt_enabled()

            mig.state["stage"] = MigrationStage.BASE_COPY
            mig.save_state()

            mig.base_copy()

            mig.state["stage"] = MigrationStage.DELTA
            mig.save_state()

        elif stage in [MigrationStage.BASE_COPY, MigrationStage.DELTA]:

            mig.run_delta_loop()

        elif stage == MigrationStage.CONVERTING:

            mig.run_virt_v2v()

        elif stage == MigrationStage.IMPORT_ROOT_DISK:

            boot_unit = mig.get_boot_disk_unit()

            boot_disk = mig.get_v2v_boot_disk(boot_unit)

            vm_id = mig.import_vm_to_cloudstack(boot_disk)

            mig.state["vm_id"] = vm_id
            mig.state["stage"] = MigrationStage.IMPORT_DATA_DISK
            mig.save_state()

        elif stage == MigrationStage.IMPORT_DATA_DISK:

            mig.stage_import_data_disk()

            mig.state["stage"] = MigrationStage.DONE
            mig.save_state()

        elif stage == MigrationStage.DONE:

            log(f"{vmname} migration already completed")
            break



def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def wait_task(task):
    while task.info.state not in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
        time.sleep(1)
    if task.info.state == vim.TaskInfo.State.error:
        raise Exception(task.info.error)

def get_thumbprint(host):
    cert = ssl.get_server_certificate((host, 443))
    der = ssl.PEM_cert_to_DER_cert(cert)
    sha = hashlib.sha1(der).hexdigest()
    return ":".join(sha[i:i+2] for i in range(0, len(sha), 2)).upper()

def cloudstack_request(command, params):

    cs = config["cloudstack"]

    params["command"] = command
    params["apikey"] = cs["api_key"]
    params["response"] = "json"

    query = "&".join(
        f"{k}={urllib.parse.quote_plus(str(params[k]))}"
        for k in sorted(params)
    )

    signature = base64.b64encode(
        hmac.new(
            cs["secret_key"].encode(),
            query.lower().encode(),
            hashlib.sha1
        ).digest()
    ).decode()

    url = f"{cs['endpoint']}?{query}&signature={urllib.parse.quote_plus(signature)}"

    r = requests.get(url)

    return r.json()


def vcenter_keepalive(migrator):

    while True:

        try:
            migrator.si.CurrentTime()

        except Exception:
            log("vCenter keepalive detected expired session")

        time.sleep(60)


def format_bytes(b):
    for unit in ['B','KB','MB','GB','TB']:
        if b < 1024:
            return f"{b:.1f}{unit}"
        b/=1024


class MigrationStage:
    INIT = "init"
    BASE_COPY = "base_copy"
    DELTA = "delta"
    WAITING_FINALIZE = "waiting_finalize"
    FINAL_SYNC = "final_sync"
    CONVERTING = "converting"
    IMPORT_ROOT_DISK = "import_root_disk"
    IMPORT_DATA_DISK = "import_data_disk"
    DONE = "done"


class Migrator:
    
    def connect_vcenter(self):

        ctx = ssl._create_unverified_context()

        self.si = SmartConnect(
            host=VCENTER,
            user=VCUSER,
            pwd=VCPASS,
            sslContext=ctx
        )

        self.content = self.si.RetrieveContent()

        # start keepalive only once
        if not hasattr(self, "keepalive_started"):
            threading.Thread(
                target=vcenter_keepalive,
                args=(self,),
                daemon=True
            ).start()
            self.keepalive_started = True

    def ensure_vcenter_session(self):

        try:
            _ = self.vm.runtime.powerState
            return

        except vim.fault.NotAuthenticated:

            log("vCenter session expired, reconnecting")

            # reconnect
            self.connect_vcenter()

            # rebuild VM object using same moid
            self.vm = vim.VirtualMachine(
                self.vm_moid,
                self.si._stub
            )



    def import_vm_to_cloudstack(self, disk_path):

        cs = self.spec["target"]["cloudstack"]
        hostname = self.vm.name.replace("_", "-") #sanitizing hostname to repalce _ with - as cloudstack doesn't allow _ in hostname. We can sanitize further as needed
        params = {
            "name": hostname,
            "displayname": self.vm.name,
            "clusterid": cs["clusterid"],
            "zoneid": cs["zoneid"],
            "importsource": "shared",
            "hypervisor": "kvm",
            "storageid": cs["storageid"],
            "diskpath": os.path.basename(disk_path),
            "networkid": cs["networkid"],
            "serviceofferingid": cs["serviceofferingid"]
        }

        log(f"[+] Importing VM {self.vm.name} into CloudStack")
        result = cloudstack_request("importVm", params)
        job_id = result["importvmresponse"]["jobid"]

        log(f"[+] Waiting for CloudStack import job {job_id}")

        while True:

            job = cloudstack_request("queryAsyncJobResult", {"jobid": job_id})
            status = job["queryasyncjobresultresponse"]["jobstatus"]
            log(f"[CloudStack] VM import job {job_id} status={status}")
            if status == 1:
                vm = job["queryasyncjobresultresponse"]["jobresult"]["virtualmachine"]
                vm_id = vm["id"]
                log(f"[+] VM import complete: {vm_id}")
                return vm_id

            if job["queryasyncjobresultresponse"]["jobstatus"] == 2:
                raise Exception(f"CloudStack VM import failed: {job}")

            time.sleep(5)

    def import_data_disks(self, disk_path, storageid, diskofferingid):

        cs = self.spec["target"]["cloudstack"]

        params = {
            "name": os.path.basename(disk_path),
            "zoneid": cs["zoneid"],
            "diskofferingid": diskofferingid,
            "storageid": storageid,
            "path": os.path.basename(disk_path)
        }

        log(f"[+] Importing data disk {disk_path}")
        log(f"    disk: {disk_path}")
        log(f"    storage: {storageid}")
        log(f"    offering: {diskofferingid}")


        result = cloudstack_request("importVolume", params)

        log(f"[+] CloudStack response: {result}")

        resp = result.get("importvolumeresponse", {})

        if "jobid" not in resp:
            raise Exception(f"importVolume failed: {resp}")

        job_id = resp["jobid"]

        log(f"[+] Waiting for volume import job {job_id}")

        while True:

            job = cloudstack_request("queryAsyncJobResult", {"jobid": job_id})

            status = job["queryasyncjobresultresponse"]["jobstatus"]
            log(f"[CloudStack] Volume import job {job_id} status={status}")

            if status == 1:
                vol = job["queryasyncjobresultresponse"]["jobresult"]["volume"]
                return vol["id"]

            if status == 2:
                raise Exception(f"Volume import failed: {job}")

            time.sleep(5)


    def get_data_disk_config(self, unit):
        unit = str(unit)
        disk_cfg = self.disk_map.get(unit)
        if not disk_cfg:
            raise Exception(f"Disk unit {unit} missing in spec.disks")

        storageid = disk_cfg.get("storageid")
        diskofferingid = disk_cfg.get("diskofferingid")

        if not storageid:
            raise Exception(f"Disk unit {unit} missing storageid")
        if not diskofferingid:
            raise Exception(f"Disk unit {unit} missing diskofferingid")

        return storageid, diskofferingid


    def resolve_data_import_path(self, unit):
        unit = str(unit)
        d = self.state["disks"].get(unit, {})
        raw = d.get("path")
        if not raw:
            raise Exception(f"Missing state path for disk unit {unit}")

        qcow = raw[:-4] + ".qcow2" if raw.endswith(".raw") else f"{raw}.qcow2"
        if os.path.exists(qcow):
            return qcow
        if os.path.exists(raw):
            return raw
        raise Exception(f"No importable file for disk unit {unit}: {raw} / {qcow}")


    def stage_import_data_disk(self):
        log("Stage: import_data_disk")
        boot_unit = str(self.get_boot_disk_unit())
        vm_id = self.state.get("vm_id")
        if not vm_id:
            raise Exception("Cannot import data disks: missing vm_id in state")

        data_units = [u for u in sorted(self.state["disks"].keys(), key=int) if str(u) != boot_unit]
        if not data_units:
            log("[+] No data disks found, skipping stage")
            return

        for unit in data_units:
            storageid, diskofferingid = self.get_data_disk_config(unit)
            disk_path = self.resolve_data_import_path(unit)

            log(f"[+] Importing data disk {disk_path}")
            volume_id = self.import_data_disks(disk_path, storageid, diskofferingid)
            self.attach_volume(volume_id, vm_id)

            with self.state_lock:
                self.state["disks"][str(unit)]["volume_id"] = volume_id
                self.state["disks"][str(unit)]["attached_to_vm_id"] = vm_id
            self.save_state()

    def attach_volume(self, volume_id, vm_id):

        params = {
            "id": volume_id,
            "virtualmachineid": vm_id
        }

        log(f"[+] Attaching volume {volume_id} to VM {vm_id}")

        result = cloudstack_request("attachVolume", params)

        log(result)



    def get_v2v_boot_disk(self, boot_unit):

        self.ensure_vcenter_session()

        letters = string.ascii_lowercase

        disk_letter = letters[int(boot_unit)]

        disk_path = os.path.join(self.DATA, f"{self.migration_id}-sd{disk_letter}")

        if not os.path.exists(disk_path):
            raise Exception(f"virt-v2v disk not found: {disk_path}")

        return disk_path

    
    def get_v2v_data_disks(self, boot_unit):
        self.ensure_vcenter_session()
        disks = []

        for unit, disk in self.state["disks"].items():

            if unit == str(boot_unit):
                continue

            raw = disk["path"]

            qcow = raw.replace(".raw", ".qcow2")

            if os.path.exists(qcow):
                disks.append(qcow)
            elif os.path.exists(raw):
                disks.append(raw)

        return disks

    

    def get_disk_storage(self, unit):

        unit = str(unit)
        boot_unit = str(self.get_boot_disk_unit())

        # Boot disk uses target.cloudstack.storageid.
        if unit == boot_unit:
            return self.spec["target"]["cloudstack"]["storageid"]

        # Data disks use per-unit storage mapping from spec.disks.
        disk_cfg = self.disk_map.get(unit)

        if not disk_cfg:
            raise Exception(f"Disk unit {unit} missing in spec")

        return disk_cfg["storageid"]


    def run_qemu_convert(self, cmd, raw_file, disk_size, disk_unit):
        import pty

        start = time.time()

        master, slave = pty.openpty()
        flags = fcntl.fcntl(master, fcntl.F_GETFL)
        fcntl.fcntl(master, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        proc = subprocess.Popen(
            cmd,
            stdout=slave,
            stderr=slave,
            close_fds=True,
        )
        os.close(slave)

        qemu_pct = 0.0

        while proc.poll() is None:
            ready, _, _ = select.select([master], [], [], 1.0)

            if ready:
                try:
                    data = os.read(master, 4096).decode(errors="ignore")
                    for part in data.split("\r"):
                        if "(" in part and "/100%" in part:
                            try:
                                qemu_pct = float(part.split("(")[1].split("/")[0])
                            except (IndexError, ValueError):
                                pass
                except OSError:
                    pass

            if os.path.exists(raw_file):
                st = os.stat(raw_file)
                written = st.st_blocks * 512
                alloc_pct = min((written / disk_size) * 100, 100) if disk_size else 0
                effective_pct = max(alloc_pct, qemu_pct)

                elapsed = time.time() - start
                speed = written / elapsed / (1024**2) if elapsed > 0 else 0

                estimated_used = None
                if qemu_pct > 0:
                    ratio = max(min(qemu_pct, 100.0), 0.01) / 100.0
                    estimated_used = int(written / ratio)
                    if disk_size:
                        estimated_used = min(max(estimated_used, written), disk_size)
                    elif estimated_used < written:
                        estimated_used = written

                eta_seconds = None
                if speed > 0:
                    eta_total = estimated_used if estimated_used is not None else disk_size
                    if eta_total is not None:
                        remaining = max(eta_total - written, 0)
                        eta_seconds = int(remaining / (speed * 1024 * 1024))

                log(f"[disk{disk_unit}] copy {alloc_pct:.2f}% scan {qemu_pct:.2f}% {speed:.1f} MB/s")

                with self.state_lock:
                    disk_state = self.state["disks"].setdefault(str(disk_unit), {})
                    disk_state["qemu_progress"] = qemu_pct
                    disk_state["progress"] = effective_pct
                    disk_state["bytes_written"] = written
                    disk_state["copied_bytes"] = min(written, disk_size) if disk_size else written
                    if estimated_used is not None:
                        disk_state["estimated_used_bytes"] = estimated_used
                        disk_state["read_total_bytes"] = estimated_used
                    disk_state["speed_mb"] = round(speed, 2)
                    disk_state["speed_mbps"] = round(speed, 2)
                    disk_state["transfer_speed_mbps"] = round(speed, 2)
                    if eta_seconds is not None:
                        disk_state["eta_seconds"] = eta_seconds
                    self.state["transfer_speed_mbps"] = round(speed, 2)
                    self.state["stage"] = MigrationStage.BASE_COPY
                    self._recalculate_overall_progress_locked()

                self.save_state()

        os.close(master)
        if proc.returncode != 0:
            raise Exception(f"qemu-img convert failed with code {proc.returncode}")

        final_written = disk_size
        if os.path.exists(raw_file):
            try:
                final_written = os.stat(raw_file).st_blocks * 512
            except OSError:
                final_written = disk_size

        with self.state_lock:
            disk_state = self.state["disks"].setdefault(str(disk_unit), {})
            disk_state["qemu_progress"] = 100.0
            disk_state["progress"] = 100.0
            disk_state["bytes_written"] = final_written
            disk_state["copied_bytes"] = final_written
            disk_state["estimated_used_bytes"] = final_written
            disk_state["read_total_bytes"] = final_written
            disk_state["speed_mb"] = 0
            disk_state["speed_mbps"] = 0
            disk_state["transfer_speed_mbps"] = 0
            disk_state["eta_seconds"] = 0
            self.state["transfer_speed_mbps"] = 0
            self._recalculate_overall_progress_locked()

        self.save_state()

    def _recalculate_overall_progress_locked(self):
        progress_values = [
            d.get("progress")
            for d in self.state.get("disks", {}).values()
            if isinstance(d.get("progress"), (int, float))
        ]
        if progress_values:
            self.state["progress"] = round(sum(progress_values) / len(progress_values), 2)

    def __init__(self, vmname):
        self.connect_vcenter()
        view = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [vim.VirtualMachine], True)
        self.vm = next((vm for vm in view.view if vm.name == vmname), None)
        if not self.vm:
            raise Exception(f"VM {vmname} not found")

        self.state_lock = threading.RLock()
        self.migration_id = f"{self.vm.name}_{self.vm._moId}"
        self.vm_moid = self.vm._moId

        # ---- create VM control directory ----
        self.control_dir = os.path.join(CONTROL_DIR, self.migration_id)
        os.makedirs(self.control_dir, exist_ok=True)

        # ---- load spec file ----
        self.spec_file = os.path.join(self.control_dir, "spec.yaml")
        if not os.path.exists(self.spec_file):
            raise Exception(f"Missing spec file: {self.spec_file}")

        with open(self.spec_file) as f:
            self.spec = yaml.safe_load(f)

        # ---- determine storage mount ----
        self.disk_map = self.spec.get("disks", {})
        boot_storageid = self.spec["target"]["cloudstack"]["storageid"]
        self.DATA = ensure_storage_mounted(boot_storageid)

        # ---- state file ----
        self.state_file = os.path.join(self.control_dir, "state.json")
        self.state = self.load_state()

        # ---- other init values ----
        self.thumb = get_thumbprint(VCENTER)
        self.nbd_procs = []
        view.Destroy()

    def load_state(self):
        if os.path.exists(self.state_file):
            return json.load(open(self.state_file))
        return {
            "vm": self.vm.name,
            "stage": MigrationStage.INIT,
            "disks": {}
        }

    def save_state(self):
        with self.state_lock:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            tmp_file = f"{self.state_file}.tmp"
            with open(tmp_file, "w") as f:
                json.dump(self.state, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, self.state_file)

    def get_boot_disk_unit(self):
        self.ensure_vcenter_session()
        boot = self.vm.config.bootOptions

        if boot and boot.bootOrder:

            for device in boot.bootOrder:

                if isinstance(device, vim.vm.BootOptions.BootableDiskDevice):

                    boot_key = device.deviceKey

                    for dev in self.vm.config.hardware.device:

                        if isinstance(dev, vim.vm.device.VirtualDisk) and dev.key == boot_key:
                            unit = dev.unitNumber
                            log(f"[+] Boot disk detected via VMware boot order: unit {unit}")
                            return str(unit)

        # fallback if boot order not available
        for dev in self.vm.config.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualDisk):
                unit = dev.unitNumber
                log(f"[+] Boot disk fallback detected: unit {unit}")
                return str(unit)

        raise Exception("Unable to determine boot disk unit")

    def get_finalize_time(self):

        mig = self.spec.get("migration", {})

        finalize_at = mig.get("finalize_at")

        if not finalize_at:
            return None

        return datetime.fromisoformat(finalize_at).timestamp()

    def get_delta_sleep(self):

        mig = self.spec.get("migration", {})

        normal_interval = mig.get("delta_interval", 300)

        finalize_interval = mig.get("finalize_delta_interval", 30)

        finalize_window = mig.get("finalize_window", 600)

        finalize_time = self.get_finalize_time()

        if not finalize_time:
            return normal_interval

        remaining = finalize_time - time.time()

        if remaining <= 0:
            return 0

        if remaining <= finalize_window:
            return finalize_interval

        return normal_interval


    def check_snapshot_limit(self):

        if not self.vm.snapshot:
            return

        def count(node):
            total = 1
            for c in node.childSnapshotList:
                total += count(c)
            return total

        #total = count(self.vm.snapshot.rootSnapshotList[0])
        total = 0
        for root in self.vm.snapshot.rootSnapshotList:
            total += count(root)

        if total > 25:
            raise Exception(
                f"Snapshot count dangerously high ({total}). "
                "User must finalize migration."
            )


    def create_snapshot(self, name):

        self.ensure_vcenter_session()
        mode = config["migration"].get("snapshot_quiesce", "auto")
        log(f"Creating snapshot {name} (quiesce mode: {mode})")

        def run_snapshot(quiesce):

            task = self.vm.CreateSnapshot(
                name=name,
                memory=False,
                quiesce=quiesce
            )

            while task.info.state in [vim.TaskInfo.State.running, vim.TaskInfo.State.queued]:
                time.sleep(1)

            if task.info.state == vim.TaskInfo.State.success:
                return task.info.result

            raise Exception(task.info.error)

        tools_status = self.vm.guest.toolsStatus

        # Always non-quiesced
        if mode == "false":
            log("Using non-quiesced snapshot")
            return run_snapshot(False)

        # Force quiesced
        if mode == "true":
            log("Using quiesced snapshot")
            return run_snapshot(True)

        # AUTO mode
        if tools_status in ["toolsOk", "toolsOld"]:

            try:
                log("Trying quiesced snapshot")
                return run_snapshot(True)

            except Exception as e:
                log(f"Quiesced snapshot failed: {e}")
                log("Falling back to non-quiesced snapshot")

        else:
            log("VMware Tools not running, skipping quiesced snapshot")

        return run_snapshot(False)


    def run_delta_loop(self):

        log(f"[{self.vm.name}] Entering delta sync loop")

        finalize_file = os.path.join(self.control_dir, "FINALIZE")

        while True:

            if self.state["stage"] == MigrationStage.DONE:
                Disconnect(self.si)
                return

            now = time.time()
            finalize_time = self.get_finalize_time()

            # Scheduled finalize
            if finalize_time and now >= finalize_time:
                log(f"[{self.vm.name}] Scheduled finalize triggered")
                open(finalize_file, "a").close()

            # Manual finalize
            if os.path.exists(finalize_file):
                log(f"[{self.vm.name}] Finalize triggered")
                self.finalize()
                Disconnect(self.si)
                return

            # Run delta sync
            log(f"[{self.vm.name}] Running delta sync")
            self.delta()

            self.state["stage"] = MigrationStage.DELTA
            self.save_state()

            sleep_time = self.get_delta_sleep()

            # Ensure we don't oversleep past finalize
            if finalize_time:
                remaining = finalize_time - time.time()
                if remaining > 0:
                    sleep_time = min(sleep_time, remaining)

            if sleep_time <= 0:
                continue

            log(f"[{self.vm.name}] Sleeping {sleep_time}s")
            time.sleep(sleep_time)


    def disks(self):
        disks = []
        self.ensure_vcenter_session()
        for d in self.vm.config.hardware.device:
            if isinstance(d, vim.vm.device.VirtualDisk):
                curr = d.backing
                while hasattr(curr, 'parent') and curr.parent:
                    curr = curr.parent
                disks.append({
                    "key": d.key,
                    "unit": d.unitNumber,
                    "path": curr.fileName,
                    "capacity": d.capacityInBytes
                })
        return disks

    def ensure_cbt_enabled(self):
        self.ensure_vcenter_session()
        if not self.vm.config.changeTrackingEnabled:
            log(f"[!] CBT is NOT enabled on {self.vm.name}. Enabling now...")
            spec = vim.vm.ConfigSpec(changeTrackingEnabled=True)
            task = self.vm.ReconfigVM_Task(spec)
            wait_task(task)
            log("[+] CBT enabled successfully.")
        else:
            log("[+] CBT is already enabled. Proceeding...")

    def get_snapshot_disk_path(self, snapshot, disk_key):
        # This searches the snapshot configuration for the backing file of the specific disk
        for device in snapshot.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualDisk) and device.key == disk_key:
                return device.backing.fileName
        return None


    def start_nbd(self, disk, snapshot, export_name=None):
        self.ensure_vcenter_session()
        # FORCE RELOAD: This updates the local snapshot object with real data from vCenter
        # Without this, the object is just a shell and lacks the .name attribute
        # We retrieve the 'config' or 'name' property explicitly
        content = self.si.RetrieveContent()
        # Or simply re-fetch it if you have the MOREF
        # But usually, just accessing an attribute that forces a fetch works:
        try:
            snap_name = snapshot.name
        except AttributeError:
            # If it fails, the object is likely stale. Refresh it by calling UpdateView
            # or just use the _moId which you already have.
            snap_name = "Snapshot_Refreshed"
        
        print(f"DEBUG: Opening disk at path: {disk['path']}")
        sock = f"/tmp/nbd_{self.migration_id}_{disk['unit']}.sock"
        if os.path.exists(sock):
            os.remove(sock)
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = f"{VDDK}/lib64"

        # Then in start_nbd:
        snapshot_disk_path = self.get_snapshot_disk_path(snapshot, disk['key'])
        cmd = [
            "nbdkit",
            "-r",
            "-t", "16",          #enable 16 worker threads
            "--filter=cache",   #cache reads locally
            "--filter=retry",    #retry VDDK read failures
            "vddk",
            f"libdir={VDDK}",
            f"server={VCENTER}",
            f"user={VCUSER}",
            f"password={VCPASS}",
            f"thumbprint={self.thumb}",
            "transports=nbd",
            f"vm=moref={self.vm._moId}",
            f"file={snapshot_disk_path}",
            f"snapshot=moref={snapshot._moId}",
            "--exit-with-parent",
            "--unix", sock
        ]


        if export_name:
            cmd.extend(["-e", export_name])
        # Add this debug log in start_nbd
        print(f"DEBUG: Checking snapshot {snapshot._moId} for disk {disk['path']}")
        # Use the pyvmomi object to verify the snapshot exists
        if snapshot is None:
            raise Exception("Snapshot object is None!")
        #print(f"DEBUG: Snapshot name is {snapshot.name}")
        print(f"DEBUG: Proceeding with snapshot _moId: {snapshot._moId}")

        proc = subprocess.Popen(cmd, env=env)
        self.nbd_procs.append(proc)
        uri = f"nbd+unix:///?socket={sock}"

        for _ in range(60):
            if os.path.exists(sock):
                try:
                    out = subprocess.check_output(["nbdinfo", "--size", uri], text=True).strip()
                    log(f"NBD ready for disk {disk['unit']} size={out}")
                    return proc, sock

                except:
                    pass
            time.sleep(1)
        raise Exception("nbdkit failed to start")

    def copy_disk_base(self, disk, res_snap):

        u_str = str(disk["unit"])
        with self.state_lock:
            disk_state = self.state["disks"].setdefault(u_str, {})
            disk_state["progress"] = 0
            disk_state["qemu_progress"] = 0
            disk_state["speed_mb"] = 0
            disk_state["speed_mbps"] = 0
            disk_state["bytes_written"] = 0
            disk_state["copied_bytes"] = 0
            disk_state["capacity"] = disk["capacity"]
            disk_state["key"] = disk["key"]
            self.state["stage"] = MigrationStage.BASE_COPY
            self._recalculate_overall_progress_locked()
        self.save_state()

        proc, sock = self.start_nbd(disk, res_snap, export_name="extents:all")

        storageid = self.get_disk_storage(u_str)
        storage_path = ensure_storage_mounted(storageid)

        raw_file = f"{storage_path}/{self.migration_id}_disk{u_str}.raw"

        log(f"Disk {u_str}: staging on storage {storageid}")
        log(f"Disk {u_str}: Starting Base copy")

        uri = f"nbd+unix:///?socket={sock}"

        try:
            self.run_qemu_convert(
                [
                    "stdbuf", "-e0",
                    "qemu-img", "convert", "-p",
                    "-m", "16",
                    "-t", "none",
                    "-T", "none",
                    "-O", "raw",
                    uri,
                    raw_file
                ],
                raw_file,
                disk["capacity"],
                u_str
            )

            disk_obj = next(d for d in res_snap.config.hardware.device if d.key == disk['key'])

            with self.state_lock:
                disk_state = self.state["disks"].setdefault(u_str, {})
                disk_state["path"] = raw_file
                disk_state["changeId"] = disk_obj.backing.changeId
                disk_state["capacity"] = disk["capacity"]
                disk_state["key"] = disk["key"]
                disk_state["copied_bytes"] = disk["capacity"]
                disk_state["progress"] = 100.0
                self._recalculate_overall_progress_locked()
            self.save_state()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def base_copy(self):
        self.check_snapshot_limit()
        res_snap = self.create_snapshot(f"Migrate_Base_{self.vm.name}")
        with ThreadPoolExecutor(max_workers=4) as exe:
            futures = [exe.submit(self.copy_disk_base, disk, res_snap) for disk in self.disks()]
            for f in futures:
                f.result()
        with self.state_lock:
            self.state["active_snapshot"] = res_snap._moId
        self.save_state()

    def delta(self):
        self.ensure_vcenter_session()
        self.check_snapshot_limit()
        prev_snap_id = self.state.get("active_snapshot")
        prev_snap = self.get_snapshot_by_id(prev_snap_id)

        new_snap = self.create_snapshot(f"Migrate_Delta_{self.vm.name}_{int(time.time())}")

        with self.state_lock:
            self.state["stage"] = MigrationStage.DELTA
        self.save_state()

        for disk in self.disks():
            u_str = str(disk['unit'])
            proc, sock_path = self.start_nbd(disk, new_snap)
            h = nbd.NBD()

            try:
                h.connect_unix(sock_path)

                with self.state_lock:
                    disk_state = self.state["disks"].get(u_str, {})
                    old_change_id = disk_state.get("changeId")
                    raw_path = disk_state.get("path")

                if not old_change_id:
                    raise Exception(f"Missing previous changeId for disk unit {u_str}")
                if not raw_path:
                    raise Exception(f"Missing raw path in state for disk unit {u_str}")

                delta_written = 0
                delta_total = 0
                delta_start = time.time()
                last_state_flush = 0.0

                with open(raw_path, "r+b") as f:
                    start_offset = 0
                    while start_offset < disk["capacity"]:
                        spec = self.vm.QueryChangedDiskAreas(
                            snapshot=new_snap,
                            deviceKey=disk["key"],
                            startOffset=start_offset,
                            changeId=old_change_id
                        )

                        changed_areas = list(spec.changedArea or [])
                        if not changed_areas:
                            start_offset = spec.startOffset + spec.length
                            if start_offset >= disk["capacity"]:
                                break
                            continue

                        for area in changed_areas:
                            delta_total += area.length
                            offset = area.start
                            end = area.start + area.length

                            while offset < end:
                                chunk_size = min(64 * 1024 * 1024, end - offset)
                                t0 = time.time()

                                buf = h.pread(chunk_size, offset)

                                f.seek(offset)
                                f.write(buf)

                                elapsed = max(time.time() - t0, 1e-6)
                                chunk_mb = len(buf) / (1024 * 1024)
                                chunk_speed = chunk_mb / elapsed
                                delta_written += len(buf)

                                elapsed_total = max(time.time() - delta_start, 1e-6)
                                speed_mbps = delta_written / elapsed_total / (1024 * 1024)

                                eta_seconds = None
                                if speed_mbps > 0:
                                    remaining = max(delta_total - delta_written, 0)
                                    eta_seconds = int(remaining / (speed_mbps * 1024 * 1024))

                                log(f"[delta] disk {u_str} offset={offset} size={chunk_mb:.1f}MB speed={chunk_speed:.1f}MB/s")

                                now_ts = time.time()
                                if now_ts - last_state_flush >= 1.0:
                                    delta_progress = 0.0
                                    if delta_total > 0:
                                        delta_progress = round(min((delta_written / delta_total) * 100, 100), 2)

                                    with self.state_lock:
                                        disk_state = self.state["disks"].setdefault(u_str, {})
                                        disk_state["delta_total_bytes"] = delta_total
                                        disk_state["delta_bytes_written"] = delta_written
                                        disk_state["delta_progress"] = delta_progress
                                        disk_state["speed_mb"] = round(speed_mbps, 2)
                                        disk_state["speed_mbps"] = round(speed_mbps, 2)
                                        disk_state["transfer_speed_mbps"] = round(speed_mbps, 2)
                                        disk_state.setdefault("capacity", disk["capacity"])
                                        disk_state.setdefault("copied_bytes", disk["capacity"])
                                        if eta_seconds is not None:
                                            disk_state["eta_seconds"] = eta_seconds

                                        self.state["transfer_speed_mbps"] = round(speed_mbps, 2)
                                        self.state["stage"] = MigrationStage.DELTA
                                        self._recalculate_overall_progress_locked()

                                    self.save_state()
                                    last_state_flush = now_ts

                                offset += chunk_size

                        start_offset = spec.startOffset + spec.length
                        if start_offset >= disk["capacity"]:
                            break

                total_time = max(time.time() - delta_start, 1e-6)
                total_mb = delta_written / (1024 * 1024)
                avg_speed = total_mb / total_time
                if delta_written:
                    log(f"[delta] disk {u_str} total {total_mb:.1f}MB in {total_time:.1f}s ({avg_speed:.1f}MB/s)")
                else:
                    log(f"[delta] disk {u_str} no changed blocks")

                disk_obj = next(d for d in new_snap.config.hardware.device if d.key == disk['key'])
                with self.state_lock:
                    disk_state = self.state["disks"].setdefault(u_str, {})
                    disk_state["changeId"] = disk_obj.backing.changeId
                    disk_state["delta_total_bytes"] = delta_total
                    disk_state["delta_bytes_written"] = delta_written
                    disk_state["delta_progress"] = 100.0 if delta_total > 0 else 0.0
                    disk_state["speed_mb"] = 0
                    disk_state["speed_mbps"] = 0
                    disk_state["transfer_speed_mbps"] = 0
                    disk_state["eta_seconds"] = 0
                    disk_state.setdefault("capacity", disk["capacity"])
                    disk_state.setdefault("copied_bytes", disk["capacity"])
                    self.state["transfer_speed_mbps"] = 0
                    self.state["stage"] = MigrationStage.DELTA
                    self._recalculate_overall_progress_locked()
                self.save_state()
            finally:
                try:
                    h.close()
                except Exception:
                    pass

                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

        with self.state_lock:
            self.state["active_snapshot"] = new_snap._moId
        self.save_state()

        if prev_snap and prev_snap != new_snap:
            log(f"[+] Removing previous snapshot {prev_snap_id}")

            task = prev_snap.RemoveSnapshot_Task(removeChildren=False)

            while task.info.state not in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
                time.sleep(1)

            if task.info.state == vim.TaskInfo.State.error:
                raise task.info.error

    def finalize(self):
        self.ensure_vcenter_session()

        log(f"[+] --- Starting Finalize Phase for {self.vm.name} ---")

        if self.vm.summary.runtime.powerState != "poweredOff":

            tools_status = self.vm.guest.toolsStatus
            mode = config["migration"].get("shutdown_mode", "auto")

            if mode == "auto":

                if tools_status in ["toolsOk", "toolsOld"]:

                    log("[+] Graceful shutdown via VMware Tools")
                    self.vm.ShutdownGuest()

                    timeout = 300
                    while self.vm.summary.runtime.powerState != "poweredOff" and timeout > 0:
                        time.sleep(5)
                        timeout -= 5

                if self.vm.summary.runtime.powerState != "poweredOff":

                    log("[!] Graceful shutdown failed, forcing power off")
                    task = self.vm.PowerOff()
                    wait_task(task)

            elif mode == "force":

                log("[+] Forcing VM power off")
                task = self.vm.PowerOff()
                wait_task(task)

            elif mode == "manual":

                log(f"[+] Waiting for manual shutdown of VM '{self.vm.name}'")

                while self.vm.summary.runtime.powerState != "poweredOff":
                    time.sleep(5)

                log("[+] Manual shutdown detected")

        # 2. Final Delta Sync (while VM is OFF)
        log("[+] Performing final consistent Delta Sync...")
        self.state["stage"] = MigrationStage.FINAL_SYNC
        self.save_state()
        
        self.delta() 

        # Kill any remaining nbdkit processes before running v2v
        log("[+] Cleaning up NBD connections...")
        #subprocess.run(["pkill", "nbdkit"], check=False)
        for p in self.nbd_procs:
            p.terminate()
        for p in self.nbd_procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        # Increase the wait time to ensure the process has actually exited
        for _ in range(20):
            if not subprocess.run(["pgrep", "nbdkit"], capture_output=True).stdout:
                break
            time.sleep(1)
        # 3. Final Conversion via virt-v2v
        time.sleep(5)
        log("[+] Flushing disk buffers...")
        subprocess.run(["sync"], check=True)
        
        time.sleep(2)

        self.run_virt_v2v()

        if self.vm.snapshot:
            log("[+] Removing migration snapshots...")
            task = self.vm.RemoveAllSnapshots_Task()
            wait_task(task)

    def get_snapshot_by_id(self, snap_id):

        if not self.vm.snapshot:
            return None

        stack = list(self.vm.snapshot.rootSnapshotList)

        while stack:

            snap = stack.pop()

            if snap.snapshot._moId == snap_id:
                return snap.snapshot

            stack.extend(snap.childSnapshotList)

        return None

    def fix_windows_drivers(self, raw_path):

        log("[+] Injecting VirtIO drivers for Windows")

        cmd = [
            "virt-v2v",
            "-i", "disk", raw_path,
            "-o", "local",
            "-os", self.DATA,
            "-of", "qcow2",
            "--inject-virtio-win",
            "/usr/share/virtio-win/virtio-win.iso",
            "-on", f"{self.vm.name}"
        ]

        env = os.environ.copy()
        env["LIBGUESTFS_MEMSIZE"] = "4096"
        env["LIBGUESTFS_BACKEND"] = "direct"
        subprocess.run(cmd, env=env, check=True)


    def detect_os(self, disk_path):
        log("[+] Detecting guest OS...")

        env = os.environ.copy()
        env["LIBGUESTFS_BACKEND"] = "direct"

        try:
            out = subprocess.check_output(
                ["virt-inspector", disk_path],
                env=env,
                text=True
            ).lower()

            if "windows" in out:
                log("[+] Guest OS detected: Windows")
                return "windows"

            if "linux" in out:
                log("[+] Guest OS detected: Linux")
                return "linux"

        except subprocess.CalledProcessError as e:
            log(f"[!] OS detection failed: {e}")

        return "unknown"


    def run_virt_v2v(self):
        log(f"[+] Preparing conversion for {self.vm.name}...")
        #raw_path = self.state["disks"]["0"]["path"]

        boot_unit = self.get_boot_disk_unit()

        if boot_unit is None:
            log("[!] Boot disk not found in boot order. Falling back to lowest unit.")
            boot_unit = sorted(self.state["disks"].keys(), key=int)[0]

        raw_path = self.state["disks"][boot_unit]["path"]


        # 1. Generate Disk XML entries
        disk_xml_snippets = []
        import string
        letters = string.ascii_lowercase 
        sorted_units = sorted(self.state["disks"].keys(), key=int)
        
        for i, unit in enumerate(sorted_units):
            path = self.state["disks"][unit]["path"]
            disk_xml_snippets.append(f"""
    <disk type='file' device='disk'>
      <driver name='qemu' type='raw'/>
      <source file='{path}'/>
      <target dev='hd{letters[i]}' bus='ide'/>
    </disk>""")

        # 2. Generate XML
        xml_file = f"{self.DATA}/{self.migration_id}_source.xml"
        source_xml = f"""
<domain type='kvm'>
  <name>{self.vm.name}</name>
  <memory unit='MiB'>{int(self.vm.config.hardware.memoryMB)}</memory>
  <vcpu>{int(self.vm.config.hardware.numCPU)}</vcpu>
  <os><type arch='x86_64' machine='q35'>hvm</type></os>
  <devices>{''.join(disk_xml_snippets)}</devices>
</domain>"""
        with open(xml_file, "w") as f:
            f.write(source_xml)

        # 3. Corrected virt-v2v command (v2.0+ compatible)
        v2v_log_path = f"{self.DATA}/{self.migration_id}_v2v.log"
        log(f"[+] Starting virt-v2v. Full logs will be saved to: {v2v_log_path}")
        with self.state_lock:
            self.state["stage"] = MigrationStage.CONVERTING
        self.save_state()
        cmd = [
            "virt-v2v",
            "-v", "-x",          # Essential for debugging driver injection
            "-i", "disk", raw_path,
            "-o", "local",
            "-os", self.DATA,
            "-of", "qcow2",
            "-on", self.migration_id,
            #"--root", "first"    # Forces the tool to find the OS partition
        ]

        # 4. Execution
        env = os.environ.copy()
        env["LIBGUESTFS_BACKEND"] = "direct"
        env["LIBGUESTFS_MEMSIZE"] = "4096"

        log(f"[+] Executing virt-v2v (Automatic Driver Detection)...")
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        for line in proc.stdout:
            log(line.strip())

        proc.wait()

        if proc.returncode != 0:
            raise Exception("virt-v2v failed")

        for unit, disk in self.state["disks"].items():

            if unit == boot_unit:
                continue

            raw = disk["path"]
            qcow = raw.replace(".raw", ".qcow2")

            log(f"Converting data disk {raw}")

            subprocess.run([
                "qemu-img",
                "convert",
                "-p",
                "-m", "16",
                "-t", "none",
                "-T", "none",
                "-O",
                "qcow2",
                raw,
                qcow
            ], check=True)

        with self.state_lock:
            self.state["stage"] = MigrationStage.IMPORT_ROOT_DISK
        self.save_state()
        
def load_specs(files):

    specs = []

    for path in files:
        with open(path) as f:
            data = yaml.safe_load(f)

        if "vms" in data:
            specs.extend(data["vms"])
        else:
            specs.append(data)

    return specs


def merge_defaults(spec):

    defaults = config.get("cloudstack_defaults", {})

    target = spec.setdefault("target", {}).setdefault("cloudstack", {})

    for k, v in defaults.items():
        target.setdefault(k, v)

    return spec


def prepare_vm(spec):

    spec = merge_defaults(spec)

    vmname = spec["vm"]["name"]

    ctx = ssl._create_unverified_context()
    si = SmartConnect(
        host=VCENTER,
        user=VCUSER,
        pwd=VCPASS,
        sslContext=ctx
    )

    content = si.RetrieveContent()
    view = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.VirtualMachine], True
    )

    vm = next((v for v in view.view if v.name == vmname), None)
    if not vm:
        view.Destroy()
        Disconnect(si)
        raise Exception(f"VM {vmname} not found")

    migration_id = f"{vm.name}_{vm._moId}"
    vm_dir = os.path.join(CONTROL_DIR, migration_id)
    os.makedirs(vm_dir, exist_ok=True)

    spec_path = os.path.join(vm_dir, "spec.yaml")
    with open(spec_path, "w") as f:
        yaml.safe_dump(spec, f)

    view.Destroy()
    Disconnect(si)

    return vmname


def ensure_storage_mounted(storageid):

    mount_path = f"/mnt/{storageid}"

    if not os.path.exists(mount_path):
        raise Exception(
            f"Primary storage {storageid} not mounted at {mount_path}"
        )

    with open("/proc/mounts") as f:
        for line in f:
            if mount_path in line:
                return mount_path

    raise Exception(
        f"{mount_path} exists but is not mounted primary storage"
    )


def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--spec",
        nargs="+",
        required=True,
        help="Spec file(s) describing migrations"
    )
    return parser.parse_args()


def run_vm_safe(vmname):

    try:
        migrate_vm(vmname)
        return vmname, True, None

    except Exception as e:
        err = str(e)
        log(f"[ERROR] Migration failed for {vmname}: {err}")
        log(traceback.format_exc())
        return vmname, False, err


def main():
    args = parse_args()

    specs = load_specs(args.spec)

    vms = []

    for spec in specs:
        vmname = prepare_vm(spec)
        vms.append(vmname)

    max_parallel = config["migration"]["parallel_vms"]

    failures = []
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = [executor.submit(run_vm_safe, vmname) for vmname in vms]
        for future in as_completed(futures):
            vmname, ok, err = future.result()
            if not ok:
                failures.append((vmname, err))

    if failures:
        log(f"[ERROR] {len(failures)} VM migration(s) failed")
        for vmname, err in failures:
            log(f"[ERROR] {vmname}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
