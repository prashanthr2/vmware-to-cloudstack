import { useCallback, useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE || `${window.location.protocol}//${window.location.hostname}:8000`;

const emptyDisk = () => ({ unit: "1", storageid: "", diskofferingid: "" });

async function apiRequest(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const text = await response.text();
  const data = text ? JSON.parse(text) : null;

  if (!response.ok) {
    const detail = data?.detail || response.statusText;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }

  return data;
}

function resourceLabel(item) {
  return item.name || item.displaytext || item.id || "Unknown";
}

export default function App() {
  const [tab, setTab] = useState("new");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const [vmwareVms, setVmwareVms] = useState([]);
  const [zones, setZones] = useState([]);
  const [clusters, setClusters] = useState([]);
  const [storagePools, setStoragePools] = useState([]);
  const [networks, setNetworks] = useState([]);
  const [serviceOfferings, setServiceOfferings] = useState([]);

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
    disks: [emptyDisk()],
  });
  const [lastSpecFile, setLastSpecFile] = useState("");

  const [jobs, setJobs] = useState([]);
  const [logs, setLogs] = useState({ stdout: "", stderr: "", stdout_path: "", stderr_path: "", job_id: "" });
  const [logsBusy, setLogsBusy] = useState(false);

  const vmOptions = useMemo(
    () => vmwareVms.map((vm) => ({ label: `${vm.name} (${vm.moref})`, value: vm.name, moref: vm.moref })),
    [vmwareVms]
  );

  const loadInventory = useCallback(async () => {
    try {
      setError("");
      const [vmList, zoneList, clusterList, storageList, networkList, offeringList] = await Promise.all([
        apiRequest("/vmware/vms"),
        apiRequest("/cloudstack/zones"),
        apiRequest("/cloudstack/clusters"),
        apiRequest("/cloudstack/storage"),
        apiRequest("/cloudstack/networks"),
        apiRequest("/cloudstack/serviceofferings"),
      ]);

      setVmwareVms(vmList);
      setZones(zoneList);
      setClusters(clusterList);
      setStoragePools(storageList);
      setNetworks(networkList);
      setServiceOfferings(offeringList);

      setForm((prev) => ({
        ...prev,
        vm_name: prev.vm_name || vmList[0]?.name || "",
        vm_moref: prev.vm_moref || vmList[0]?.moref || "",
        zoneid: prev.zoneid || zoneList[0]?.id || "",
        clusterid: prev.clusterid || clusterList[0]?.id || "",
        networkid: prev.networkid || networkList[0]?.id || "",
        serviceofferingid: prev.serviceofferingid || offeringList[0]?.id || "",
        boot_storageid: prev.boot_storageid || storageList[0]?.id || "",
        disks: prev.disks.map((disk) => ({
          ...disk,
          storageid: disk.storageid || storageList[0]?.id || "",
        })),
      }));
    } catch (loadErr) {
      setError(loadErr.message);
    }
  }, []);

  const refreshJobs = useCallback(async () => {
    try {
      const list = await apiRequest("/migration/jobs?limit=200");
      setJobs(list);
    } catch (jobsErr) {
      setError(jobsErr.message);
    }
  }, []);

  useEffect(() => {
    loadInventory();
  }, [loadInventory]);

  useEffect(() => {
    refreshJobs();
    const interval = setInterval(refreshJobs, 8000);
    return () => clearInterval(interval);
  }, [refreshJobs]);

  useEffect(() => {
    const selected = vmwareVms.find((vm) => vm.name === form.vm_name);
    if (!selected) {
      return;
    }

    setForm((prev) => {
      if (prev.vm_moref === selected.moref) {
        return prev;
      }
      return { ...prev, vm_moref: selected.moref || "" };
    });
  }, [form.vm_name, vmwareVms]);

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

  const updateDisk = (index, key, value) => {
    setForm((prev) => {
      const next = [...prev.disks];
      next[index] = { ...next[index], [key]: value };
      return { ...prev, disks: next };
    });
  };

  const addDisk = () => {
    setForm((prev) => ({
      ...prev,
      disks: [...prev.disks, { ...emptyDisk(), unit: String(prev.disks.length + 1), storageid: prev.boot_storageid || "" }],
    }));
  };

  const removeDisk = (index) => {
    setForm((prev) => {
      if (prev.disks.length <= 1) {
        return prev;
      }
      const next = prev.disks.filter((_, i) => i !== index);
      return { ...prev, disks: next };
    });
  };

  const buildSpecPayload = () => {
    const diskMap = {};
    form.disks.forEach((disk) => {
      if (!disk.unit) {
        return;
      }
      diskMap[disk.unit] = {
        storageid: disk.storageid,
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
      vm_moref: form.vm_moref || undefined,
      zoneid: form.zoneid,
      clusterid: form.clusterid,
      networkid: form.networkid,
      serviceofferingid: form.serviceofferingid,
      boot_storageid: form.boot_storageid,
      disks: diskMap,
      migration,
    };
  };

  const createSpec = async (autoStart) => {
    setBusy(true);
    setMessage("");
    setError("");

    try {
      const specResp = await apiRequest("/migration/spec", {
        method: "POST",
        body: JSON.stringify(buildSpecPayload()),
      });

      setLastSpecFile(specResp.spec_file);
      setMessage(`Spec generated at ${specResp.spec_file}`);

      if (autoStart) {
        const startResp = await apiRequest("/migration/start", {
          method: "POST",
          body: JSON.stringify({ vm_name: form.vm_name, spec_file: specResp.spec_file }),
        });
        setMessage(`Migration started. Job ${startResp.job_id}`);
        setTab("jobs");
        await refreshJobs();
      }
    } catch (submitErr) {
      setError(submitErr.message);
    } finally {
      setBusy(false);
    }
  };

  const finalizeVm = async (vmName) => {
    try {
      setError("");
      const response = await apiRequest(`/migration/finalize/${encodeURIComponent(vmName)}`, { method: "POST" });
      setMessage(`${response.message}: ${response.finalize_file}`);
    } catch (finalizeErr) {
      setError(finalizeErr.message);
    }
  };

  const openLogs = async (job) => {
    try {
      setLogsBusy(true);
      setError("");
      const result = await apiRequest(
        `/migration/logs/${encodeURIComponent(job.vm_name)}?job_id=${encodeURIComponent(job.job_id)}&lines=300`
      );
      setLogs(result);
    } catch (logsErr) {
      setError(logsErr.message);
    } finally {
      setLogsBusy(false);
    }
  };

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1>VMware to CloudStack Migrator</h1>
          <p>Build specs, start migrations, track progress, and trigger finalize from one console.</p>
        </div>
        <div className="tab-buttons">
          <button className={tab === "new" ? "active" : ""} onClick={() => setTab("new")}>New Migration</button>
          <button className={tab === "jobs" ? "active" : ""} onClick={() => setTab("jobs")}>Jobs & Logs</button>
        </div>
      </header>

      {message ? <div className="notice success">{message}</div> : null}
      {error ? <div className="notice error">{error}</div> : null}

      {tab === "new" ? (
        <section className="panel">
          <h2>Create Migration</h2>
          <div className="form-grid">
            <label>
              Source VM
              <select value={form.vm_name} onChange={(e) => updateField("vm_name", e.target.value)}>
                {vmOptions.map((vm) => (
                  <option key={`${vm.value}-${vm.moref}`} value={vm.value}>{vm.label}</option>
                ))}
              </select>
            </label>

            <label>
              VM MoRef
              <input value={form.vm_moref} onChange={(e) => updateField("vm_moref", e.target.value)} placeholder="vm-123" />
            </label>

            <label>
              Zone
              <select value={form.zoneid} onChange={(e) => updateField("zoneid", e.target.value)}>
                {zones.map((item) => (
                  <option key={item.id} value={item.id}>{resourceLabel(item)}</option>
                ))}
              </select>
            </label>

            <label>
              Cluster
              <select value={form.clusterid} onChange={(e) => updateField("clusterid", e.target.value)}>
                {clusters.map((item) => (
                  <option key={item.id} value={item.id}>{resourceLabel(item)}</option>
                ))}
              </select>
            </label>

            <label>
              Network
              <select value={form.networkid} onChange={(e) => updateField("networkid", e.target.value)}>
                {networks.map((item) => (
                  <option key={item.id} value={item.id}>{resourceLabel(item)}</option>
                ))}
              </select>
            </label>

            <label>
              Service Offering
              <select value={form.serviceofferingid} onChange={(e) => updateField("serviceofferingid", e.target.value)}>
                {serviceOfferings.map((item) => (
                  <option key={item.id} value={item.id}>{resourceLabel(item)}</option>
                ))}
              </select>
            </label>

            <label>
              Boot Storage
              <select value={form.boot_storageid} onChange={(e) => updateField("boot_storageid", e.target.value)}>
                {storagePools.map((item) => (
                  <option key={item.id} value={item.id}>{resourceLabel(item)}</option>
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

          <div className="subsection">
            <div className="subsection-title-row">
              <h3>Data Disks</h3>
              <button className="secondary" onClick={addDisk}>Add Disk</button>
            </div>
            {form.disks.map((disk, index) => (
              <div className="disk-row" key={`${disk.unit}-${index}`}>
                <input value={disk.unit} onChange={(e) => updateDisk(index, "unit", e.target.value)} placeholder="Unit" />
                <select value={disk.storageid} onChange={(e) => updateDisk(index, "storageid", e.target.value)}>
                  {storagePools.map((item) => (
                    <option key={item.id} value={item.id}>{resourceLabel(item)}</option>
                  ))}
                </select>
                <input
                  value={disk.diskofferingid}
                  onChange={(e) => updateDisk(index, "diskofferingid", e.target.value)}
                  placeholder="Disk offering ID"
                />
                <button className="danger" onClick={() => removeDisk(index)}>Remove</button>
              </div>
            ))}
          </div>

          <div className="actions">
            <button disabled={busy} onClick={() => createSpec(false)}>Generate Spec</button>
            <button disabled={busy} onClick={() => createSpec(true)}>Generate & Start</button>
            <button className="secondary" disabled={busy} onClick={loadInventory}>Reload Inventory</button>
          </div>

          {lastSpecFile ? <p className="hint">Latest spec: <code>{lastSpecFile}</code></p> : null}
        </section>
      ) : (
        <section className="panel">
          <div className="subsection-title-row">
            <h2>Running and Recent Jobs</h2>
            <button className="secondary" onClick={refreshJobs}>Refresh</button>
          </div>

          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>VM</th>
                  <th>Job</th>
                  <th>Status</th>
                  <th>Stage</th>
                  <th>Progress</th>
                  <th>Started</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job) => (
                  <tr key={job.job_id}>
                    <td>{job.vm_name}</td>
                    <td><code>{job.job_id.slice(0, 8)}</code></td>
                    <td><span className={`pill ${job.status}`}>{job.status}</span></td>
                    <td>{job.stage || "-"}</td>
                    <td>{job.progress ?? "-"}</td>
                    <td>{new Date(job.started_at).toLocaleString()}</td>
                    <td className="row-actions">
                      <button className="secondary" onClick={() => finalizeVm(job.vm_name)}>Finalize</button>
                      <button className="secondary" onClick={() => openLogs(job)}>Logs</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="logs-pane">
            <div className="subsection-title-row">
              <h3>Logs</h3>
              {logsBusy ? <span className="hint">Loading...</span> : null}
            </div>
            <p className="hint">
              Job: <code>{logs.job_id || "n/a"}</code>
            </p>
            <div className="logs-grid">
              <div>
                <h4>STDOUT</h4>
                <pre>{logs.stdout || "No stdout available."}</pre>
              </div>
              <div>
                <h4>STDERR</h4>
                <pre>{logs.stderr || "No stderr available."}</pre>
              </div>
            </div>
          </div>
        </section>
      )}
    </div>
  );
}

