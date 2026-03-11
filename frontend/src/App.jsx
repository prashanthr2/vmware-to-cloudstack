import { useCallback, useEffect, useMemo, useState } from "react";

import DiskTable from "./components/DiskTable";
import EnvironmentManager from "./components/EnvironmentManager";
import MigrationProgress from "./components/MigrationProgress";
import VMSelector from "./components/VMSelector";

const API_BASE = import.meta.env.VITE_API_BASE || `${window.location.protocol}//${window.location.hostname}:8000`;
const ENV_STORAGE_KEY = "vm_migrator_environments_v1";

const DEFAULT_ENV_STATE = {
  selectedVcenterId: "",
  selectedCloudstackId: "",
  vcenters: [],
  cloudstacks: [],
};

function optionLabel(item) {
  return item.name || item.displaytext || item.id || "Unknown";
}

function uniqueByVm(jobs) {
  const seen = new Set();
  const result = [];
  jobs.forEach((job) => {
    if (seen.has(job.vm_name)) {
      return;
    }
    seen.add(job.vm_name);
    result.push(job.vm_name);
  });
  return result;
}

async function apiRequest(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const text = await response.text();
  let payload = null;

  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = null;
    }
  }

  if (!response.ok) {
    const detail = payload?.detail || text || response.statusText;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }

  if (payload !== null) {
    return payload;
  }

  return text || null;
}

export default function App() {
  const [tab, setTab] = useState("new");
  const [busy, setBusy] = useState(false);
  const [inventoryBusy, setInventoryBusy] = useState(false);
  const [vmDisksLoading, setVmDisksLoading] = useState(false);

  const [vmwareVms, setVmwareVms] = useState([]);
  const [zones, setZones] = useState([]);
  const [clusters, setClusters] = useState([]);
  const [storagePools, setStoragePools] = useState([]);
  const [networks, setNetworks] = useState([]);
  const [serviceOfferings, setServiceOfferings] = useState([]);
  const [diskOfferings, setDiskOfferings] = useState([]);

  const [detectedDisks, setDetectedDisks] = useState([]);

  const [form, setForm] = useState({
    vm_name: "",
    vm_moref: "",
    zoneid: "",
    clusterid: "",
    networkid: "",
    serviceofferingid: "",
    boot_storageid: "",
    migration: {
      delta_interval: 300,
      finalize_at: "",
      finalize_delta_interval: "",
      finalize_window: "",
      shutdown_mode: "",
      snapshot_quiesce: "",
    },
  });

  const [lastSpecFile, setLastSpecFile] = useState("");

  const [jobs, setJobs] = useState([]);
  const [statusByVm, setStatusByVm] = useState({});
  const [selectedJob, setSelectedJob] = useState(null);
  const [logs, setLogs] = useState({ stdout: "", stderr: "", stdout_path: "", stderr_path: "", job_id: "" });
  const [logsBusy, setLogsBusy] = useState(false);

  const [toasts, setToasts] = useState([]);

  const [envState, setEnvState] = useState(() => {
    try {
      const raw = localStorage.getItem(ENV_STORAGE_KEY);
      if (!raw) {
        return DEFAULT_ENV_STATE;
      }
      const parsed = JSON.parse(raw);
      return {
        ...DEFAULT_ENV_STATE,
        ...parsed,
      };
    } catch {
      return DEFAULT_ENV_STATE;
    }
  });

  useEffect(() => {
    localStorage.setItem(ENV_STORAGE_KEY, JSON.stringify(envState));
  }, [envState]);

  const selectedVcenter = useMemo(
    () => envState.vcenters.find((item) => item.id === envState.selectedVcenterId) || null,
    [envState.selectedVcenterId, envState.vcenters]
  );
  const selectedCloudstack = useMemo(
    () => envState.cloudstacks.find((item) => item.id === envState.selectedCloudstackId) || null,
    [envState.cloudstacks, envState.selectedCloudstackId]
  );

  const vmwareHeaders = useMemo(() => {
    if (!selectedVcenter) {
      return {};
    }
    return {
      "x-vcenter-host": selectedVcenter.host || "",
      "x-vcenter-user": selectedVcenter.username || "",
      "x-vcenter-password": selectedVcenter.password || "",
    };
  }, [selectedVcenter]);

  const cloudstackHeaders = useMemo(() => {
    if (!selectedCloudstack) {
      return {};
    }
    return {
      "x-cloudstack-endpoint": selectedCloudstack.apiUrl || "",
      "x-cloudstack-api-key": selectedCloudstack.apiKey || "",
      "x-cloudstack-secret-key": selectedCloudstack.secretKey || "",
    };
  }, [selectedCloudstack]);

  const pushToast = useCallback((type, message) => {
    const id = `${Date.now()}-${Math.floor(Math.random() * 10000)}`;
    setToasts((prev) => [...prev, { id, type, message }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((toast) => toast.id !== id));
    }, 4500);
  }, []);

  const vmOptions = useMemo(
    () => vmwareVms.map((vm) => ({ name: vm.name, moref: vm.moref, disks: vm.disks || [] })),
    [vmwareVms]
  );

  const validationByUnit = useMemo(() => {
    const errors = {};
    detectedDisks.forEach((disk) => {
      if (!disk.storageid) {
        errors[disk.unit] = "Storage target is required.";
        return;
      }

      if (disk.diskType !== "os" && !disk.diskofferingid) {
        errors[disk.unit] = "Disk offering is required for data disks.";
      }
    });
    return errors;
  }, [detectedDisks]);

  const canStartMigration = useMemo(() => {
    const hasRequiredCoreFields =
      !!form.vm_name &&
      !!form.vm_moref &&
      !!form.zoneid &&
      !!form.clusterid &&
      !!form.networkid &&
      !!form.serviceofferingid &&
      !!form.boot_storageid;

    return hasRequiredCoreFields && detectedDisks.length > 0 && Object.keys(validationByUnit).length === 0 && !busy;
  }, [busy, detectedDisks, form, validationByUnit]);

  const mapDetectedDisks = useCallback(
    (vmDisks) => {
      const previousByUnit = Object.fromEntries(detectedDisks.map((disk) => [disk.unit, disk]));

      return vmDisks.map((disk, index) => {
        const unit = disk.unit !== null && disk.unit !== undefined ? String(disk.unit) : String(index);
        const existing = previousByUnit[unit];

        return {
          unit,
          label: disk.label || `Disk ${index + 1}`,
          sizeGb: disk.size_gb,
          sizeText: disk.size_gb ? `${disk.size_gb} GB` : "-",
          datastore: disk.datastore || "-",
          diskType: disk.disk_type || (index === 0 ? "os" : "data"),
          storageid: existing?.storageid || form.boot_storageid || "",
          diskofferingid:
            (disk.disk_type || (index === 0 ? "os" : "data")) === "os" ? "" : existing?.diskofferingid || "",
        };
      });
    },
    [detectedDisks, form.boot_storageid]
  );

  const pickValidOrFirst = useCallback((currentId, items) => {
    if (!items.length) {
      return "";
    }
    return items.some((item) => item.id === currentId) ? currentId : items[0].id;
  }, []);

  const mapInventoryDisks = useCallback((vmDisks, bootStorageId) => {
    return vmDisks.map((disk, index) => {
      const unit = disk.unit !== null && disk.unit !== undefined ? String(disk.unit) : String(index);
      return {
        unit,
        label: disk.label || `Disk ${index + 1}`,
        sizeGb: disk.size_gb,
        sizeText: disk.size_gb ? `${disk.size_gb} GB` : "-",
        datastore: disk.datastore || "-",
        diskType: disk.disk_type || (index === 0 ? "os" : "data"),
        storageid: bootStorageId || "",
        diskofferingid: "",
      };
    });
  }, []);

  const loadInventory = useCallback(async () => {
    setInventoryBusy(true);
    try {
      const requests = [
        { key: "vms", label: "VMware VMs", path: "/vmware/vms", headers: vmwareHeaders },
        { key: "zones", label: "CloudStack zones", path: "/cloudstack/zones", headers: cloudstackHeaders },
        { key: "clusters", label: "CloudStack clusters", path: "/cloudstack/clusters", headers: cloudstackHeaders },
        { key: "storage", label: "CloudStack storage", path: "/cloudstack/storage", headers: cloudstackHeaders },
        { key: "networks", label: "CloudStack networks", path: "/cloudstack/networks", headers: cloudstackHeaders },
        {
          key: "serviceOfferings",
          label: "CloudStack service offerings",
          path: "/cloudstack/serviceofferings",
          headers: cloudstackHeaders,
        },
        {
          key: "diskOfferings",
          label: "CloudStack disk offerings",
          path: "/cloudstack/diskofferings",
          headers: cloudstackHeaders,
        },
      ];

      const responses = await Promise.all(
        requests.map(async (req) => {
          try {
            const data = await apiRequest(req.path, { headers: req.headers });
            return {
              key: req.key,
              label: req.label,
              data: Array.isArray(data) ? data : [],
              error: "",
            };
          } catch (err) {
            return {
              key: req.key,
              label: req.label,
              data: [],
              error: err?.message || "request failed",
            };
          }
        })
      );

      const byKey = Object.fromEntries(responses.map((item) => [item.key, item]));
      const vms = byKey.vms?.data || [];
      const zoneList = byKey.zones?.data || [];
      const clusterList = byKey.clusters?.data || [];
      const storageList = byKey.storage?.data || [];
      const networkList = byKey.networks?.data || [];
      const serviceList = byKey.serviceOfferings?.data || [];
      const diskOfferingList = byKey.diskOfferings?.data || [];

      setVmwareVms(vms);
      setZones(zoneList);
      setClusters(clusterList);
      setStoragePools(storageList);
      setNetworks(networkList);
      setServiceOfferings(serviceList);
      setDiskOfferings(diskOfferingList);

      const failures = responses.filter((item) => item.error);
      if (failures.length) {
        const msg = failures.map((item) => `${item.label}: ${item.error}`).join(" | ");
        pushToast("error", msg);
      }

      setForm((prev) => {
        const nextVmName = vms.some((vm) => vm.name === prev.vm_name) ? prev.vm_name : vms[0]?.name || "";
        const vmForSelection = vms.find((vm) => vm.name === nextVmName);
        const nextBootStorage = pickValidOrFirst(prev.boot_storageid, storageList);

        setDetectedDisks(vmForSelection ? mapInventoryDisks(vmForSelection.disks || [], nextBootStorage) : []);

        return {
          ...prev,
          vm_name: nextVmName,
          vm_moref: vmForSelection?.moref || "",
          zoneid: pickValidOrFirst(prev.zoneid, zoneList),
          clusterid: pickValidOrFirst(prev.clusterid, clusterList),
          networkid: pickValidOrFirst(prev.networkid, networkList),
          serviceofferingid: pickValidOrFirst(prev.serviceofferingid, serviceList),
          boot_storageid: nextBootStorage,
        };
      });
    } finally {
      setInventoryBusy(false);
    }
  }, [cloudstackHeaders, mapInventoryDisks, pickValidOrFirst, pushToast, vmwareHeaders]);

  const detectVmDisks = useCallback(
    async (vmNameOverride = null) => {
      const targetVmName = vmNameOverride || form.vm_name;
      if (!targetVmName) {
        return;
      }

      setVmDisksLoading(true);
      try {
        const vms = await apiRequest("/vmware/vms", { headers: vmwareHeaders });
        setVmwareVms(vms);

        const selectedVm = vms.find((vm) => vm.name === targetVmName);
        if (!selectedVm) {
          setDetectedDisks([]);
          pushToast("error", `VM ${targetVmName} not found in vCenter.`);
          return;
        }

        setForm((prev) => ({
          ...prev,
          vm_moref: selectedVm.moref || prev.vm_moref || "",
        }));

        const mapped = mapDetectedDisks(selectedVm.disks || []);
        setDetectedDisks(mapped);
      } catch (err) {
        pushToast("error", err.message || "Failed to detect VM disks.");
      } finally {
        setVmDisksLoading(false);
      }
    },
    [form.vm_name, mapDetectedDisks, pushToast, vmwareHeaders]
  );

  const refreshJobs = useCallback(async () => {
    try {
      const list = await apiRequest("/migration/jobs?limit=200");
      setJobs(list);
    } catch (err) {
      pushToast("error", err.message || "Failed to load jobs.");
    }
  }, [pushToast]);

  const pollStatuses = useCallback(async () => {
    const vmNames = uniqueByVm(jobs);
    if (!vmNames.length) {
      return;
    }

    try {
      const responses = await Promise.all(
        vmNames.map(async (vmName) => {
          try {
            const status = await apiRequest(`/migration/status/${encodeURIComponent(vmName)}`);
            return [vmName, status];
          } catch {
            return [vmName, null];
          }
        })
      );

      const updates = {};
      responses.forEach(([vmName, payload]) => {
        if (payload) {
          updates[vmName] = payload;
        }
      });

      if (Object.keys(updates).length) {
        setStatusByVm((prev) => ({ ...prev, ...updates }));
      }
    } catch {
      // keep silent to avoid noisy polling toasts
    }
  }, [jobs]);

  const loadLogsForJob = useCallback(async (job) => {
    if (!job) {
      return;
    }

    setLogsBusy(true);
    try {
      const response = await apiRequest(
        `/migration/logs/${encodeURIComponent(job.vm_name)}?job_id=${encodeURIComponent(job.job_id)}&lines=300`
      );
      setLogs(response);
    } catch (err) {
      pushToast("error", err.message || "Failed to load logs.");
    } finally {
      setLogsBusy(false);
    }
  }, [pushToast]);

  useEffect(() => {
    loadInventory();
    refreshJobs();
  }, [loadInventory, refreshJobs]);

  useEffect(() => {
    if (!form.vm_name) {
      return;
    }
    detectVmDisks(form.vm_name);
  }, [form.vm_name, detectVmDisks]);

  useEffect(() => {
    const interval = setInterval(refreshJobs, 8000);
    return () => clearInterval(interval);
  }, [refreshJobs]);

  useEffect(() => {
    pollStatuses();
    const interval = setInterval(pollStatuses, 2000);
    return () => clearInterval(interval);
  }, [pollStatuses]);

  useEffect(() => {
    if (!selectedJob) {
      return;
    }

    loadLogsForJob(selectedJob);
    const interval = setInterval(() => loadLogsForJob(selectedJob), 2000);
    return () => clearInterval(interval);
  }, [selectedJob, loadLogsForJob]);

  const updateField = (field, value) => {
    setForm((prev) => ({ ...prev, [field]: value }));
  };

  const updateMigrationField = (field, value) => {
    setForm((prev) => ({
      ...prev,
      migration: {
        ...prev.migration,
        [field]: value,
      },
    }));
  };

  const updateDisk = (unit, field, value) => {
    setDetectedDisks((prev) => prev.map((disk) => (disk.unit === unit ? { ...disk, [field]: value } : disk)));
  };

  const buildSpecPayload = () => {
    const disks = {};
    detectedDisks.forEach((disk) => {
      if (disk.diskType === "os") {
        return;
      }
      disks[disk.unit] = {
        storageid: disk.storageid || form.boot_storageid,
        diskofferingid: disk.diskofferingid,
      };
    });

    const migration = {
      delta_interval: Number(form.migration.delta_interval) || 300,
    };

    if (form.migration.finalize_at) {
      migration.finalize_at = form.migration.finalize_at;
    }
    if (form.migration.finalize_delta_interval) {
      migration.finalize_delta_interval = Number(form.migration.finalize_delta_interval);
    }
    if (form.migration.finalize_window) {
      migration.finalize_window = Number(form.migration.finalize_window);
    }
    if (form.migration.shutdown_mode) {
      migration.shutdown_mode = form.migration.shutdown_mode;
    }
    if (form.migration.snapshot_quiesce) {
      migration.snapshot_quiesce = form.migration.snapshot_quiesce;
    }

    return {
      vm_name: form.vm_name,
      vm_moref: form.vm_moref,
      zoneid: form.zoneid,
      clusterid: form.clusterid,
      networkid: form.networkid,
      serviceofferingid: form.serviceofferingid,
      boot_storageid: form.boot_storageid,
      disks,
      migration,
    };
  };

  const createSpec = async (startAfter) => {
    if (!canStartMigration) {
      pushToast("error", "Please resolve validation errors before starting migration.");
      return;
    }

    setBusy(true);
    try {
      const specResp = await apiRequest("/migration/spec", {
        method: "POST",
        headers: vmwareHeaders,
        body: JSON.stringify(buildSpecPayload()),
      });

      setLastSpecFile(specResp.spec_file);
      pushToast("success", `Spec generated: ${specResp.spec_file}`);

      if (startAfter) {
        const startResp = await apiRequest("/migration/start", {
          method: "POST",
          body: JSON.stringify({ vm_name: form.vm_name, spec_file: specResp.spec_file }),
        });

        pushToast("success", `Migration started: ${startResp.job_id}`);
        setTab("progress");
        refreshJobs();
      }
    } catch (err) {
      pushToast("error", err.message || "Failed to generate/start migration.");
    } finally {
      setBusy(false);
    }
  };

  const finalizeVm = async (vmName) => {
    try {
      const response = await apiRequest(`/migration/finalize/${encodeURIComponent(vmName)}`, { method: "POST" });
      pushToast("success", response.message);
    } catch (err) {
      pushToast("error", err.message || "Failed to finalize migration.");
    }
  };

  const selectedVmStatus = selectedJob ? statusByVm[selectedJob.vm_name] : null;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1>VMware to CloudStack Migrator</h1>
          <p>Auto-detect VM disks, validate offerings, and monitor migration in real time.</p>
        </div>
        <div className="tab-buttons">
          <button className={tab === "new" ? "active" : ""} onClick={() => setTab("new")}>New Migration</button>
          <button className={tab === "progress" ? "active" : ""} onClick={() => setTab("progress")}>Progress</button>
        </div>
      </header>

      <div className="toast-stack">
        {toasts.map((toast) => (
          <div key={toast.id} className={`toast ${toast.type}`}>
            {toast.message}
          </div>
        ))}
      </div>

      {tab === "new" ? (
        <>
          <EnvironmentManager envState={envState} onChange={setEnvState} onToast={pushToast} />

          <VMSelector
            vmOptions={vmOptions}
            vmName={form.vm_name}
            vmMoref={form.vm_moref}
            onVmChange={(value) => updateField("vm_name", value)}
            onMorefChange={(value) => updateField("vm_moref", value)}
            onDetectDisks={() => detectVmDisks(form.vm_name)}
            loading={vmDisksLoading}
          />

          <section className="panel">
            <div className="subsection-title-row">
              <h2>Target and Strategy</h2>
              <button className="secondary" onClick={loadInventory} disabled={inventoryBusy}>
                {inventoryBusy ? "Loading..." : "Reload Inventory"}
              </button>
            </div>

            <div className="form-grid">
              <label>
                Zone
                <select value={form.zoneid} onChange={(e) => updateField("zoneid", e.target.value)}>
                  <option value="">Select zone</option>
                  {zones.map((item) => (
                    <option key={item.id} value={item.id}>{optionLabel(item)}</option>
                  ))}
                </select>
              </label>

              <label>
                Cluster
                <select value={form.clusterid} onChange={(e) => updateField("clusterid", e.target.value)}>
                  <option value="">Select cluster</option>
                  {clusters.map((item) => (
                    <option key={item.id} value={item.id}>{optionLabel(item)}</option>
                  ))}
                </select>
              </label>

              <label>
                Network
                <select value={form.networkid} onChange={(e) => updateField("networkid", e.target.value)}>
                  <option value="">Select network</option>
                  {networks.map((item) => (
                    <option key={item.id} value={item.id}>{optionLabel(item)}</option>
                  ))}
                </select>
              </label>

              <label>
                Service Offering
                <select value={form.serviceofferingid} onChange={(e) => updateField("serviceofferingid", e.target.value)}>
                  <option value="">Select service offering</option>
                  {serviceOfferings.map((item) => (
                    <option key={item.id} value={item.id}>{optionLabel(item)}</option>
                  ))}
                </select>
              </label>

              <label>
                Boot Storage
                <select value={form.boot_storageid} onChange={(e) => updateField("boot_storageid", e.target.value)}>
                  <option value="">Select boot storage</option>
                  {storagePools.map((item) => (
                    <option key={item.id} value={item.id}>{optionLabel(item)}</option>
                  ))}
                </select>
              </label>

              <label>
                Delta Interval (sec)
                <input
                  type="number"
                  min="1"
                  value={form.migration.delta_interval}
                  onChange={(e) => updateMigrationField("delta_interval", e.target.value)}
                />
              </label>

              <label>
                Finalize At (ISO)
                <input
                  value={form.migration.finalize_at}
                  onChange={(e) => updateMigrationField("finalize_at", e.target.value)}
                  placeholder="2026-03-12T23:30:00+00:00"
                />
              </label>

              <label>
                Finalize Delta Interval
                <input
                  type="number"
                  min="1"
                  value={form.migration.finalize_delta_interval}
                  onChange={(e) => updateMigrationField("finalize_delta_interval", e.target.value)}
                />
              </label>

              <label>
                Finalize Window
                <input
                  type="number"
                  min="1"
                  value={form.migration.finalize_window}
                  onChange={(e) => updateMigrationField("finalize_window", e.target.value)}
                />
              </label>

              <label>
                Shutdown Mode
                <input
                  value={form.migration.shutdown_mode}
                  onChange={(e) => updateMigrationField("shutdown_mode", e.target.value)}
                  placeholder="auto"
                />
              </label>

              <label>
                Snapshot Quiesce
                <input
                  value={form.migration.snapshot_quiesce}
                  onChange={(e) => updateMigrationField("snapshot_quiesce", e.target.value)}
                  placeholder="auto"
                />
              </label>
            </div>
          </section>

          <DiskTable
            disks={detectedDisks}
            storagePools={storagePools}
            diskOfferings={diskOfferings}
            onDiskChange={updateDisk}
            validationByUnit={validationByUnit}
          />

          <section className="panel">
            <div className="actions">
              <button disabled={busy || !form.vm_name} onClick={() => createSpec(false)}>
                Generate Spec
              </button>
              <button disabled={!canStartMigration} onClick={() => createSpec(true)}>
                Start Migration
              </button>
            </div>
            {Object.keys(validationByUnit).length > 0 ? (
              <p className="field-error">Complete required disk offerings for all data disks before starting.</p>
            ) : null}
            {lastSpecFile ? <p className="hint">Last generated spec: <code>{lastSpecFile}</code></p> : null}
          </section>
        </>
      ) : (
        <MigrationProgress
          jobs={jobs}
          statusByVm={statusByVm}
          selectedJobId={selectedJob?.job_id || ""}
          onSelectJob={(job) => {
            setSelectedJob(job);
            loadLogsForJob(job);
          }}
          onFinalize={finalizeVm}
          logsSection={
            <div className="logs-pane">
              <div className="subsection-title-row">
                <h3>Logs</h3>
                {logsBusy ? <span className="hint">Loading...</span> : null}
              </div>
              {selectedJob ? (
                <p className="hint">
                  Job <code>{selectedJob.job_id}</code> | VM <strong>{selectedJob.vm_name}</strong>
                </p>
              ) : null}
              <div className="logs-grid">
                <div>
                  <h4>STDOUT</h4>
                  <pre>{logs.stdout || "No stdout logs available."}</pre>
                </div>
                <div>
                  <h4>STDERR</h4>
                  <pre>{logs.stderr || "No stderr logs available."}</pre>
                </div>
              </div>
              {selectedVmStatus?.job_error ? <p className="field-error">{selectedVmStatus.job_error}</p> : null}
            </div>
          }
        />
      )}
    </div>
  );
}

