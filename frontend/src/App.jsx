import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import DiskTable from "./components/DiskTable";
import EnvironmentManager from "./components/EnvironmentManager";
import MigrationProgress from "./components/MigrationProgress";
import NicTable from "./components/NicTable";
import VMSelector from "./components/VMSelector";

const API_BASE = import.meta.env.VITE_API_BASE || `${window.location.protocol}//${window.location.hostname}:8000`;
const ENV_STORAGE_KEY = "vm_migrator_environments_v1";

const DEFAULT_ENV_STATE = {
  selectedVcenterId: "",
  selectedCloudstackId: "",
  vcenters: [],
  cloudstacks: [],
};

const DEFAULT_MIGRATION = {
  delta_interval: 300,
  finalize_at: "",
  finalize_delta_interval: "",
  finalize_window: "",
  shutdown_mode: "",
  snapshot_quiesce: "",
};

function optionLabel(item) {
  return item.name || item.displaytext || item.id || "Unknown";
}

function uniqueByVm(jobs) {
  const seen = new Set();
  const result = [];
  jobs.forEach((job) => {
    if (seen.has(job.vm_name)) return;
    seen.add(job.vm_name);
    result.push(job.vm_name);
  });
  return result;
}

function pickValidOrFirst(currentId, items) {
  if (!items.length) return "";
  return items.some((item) => item.id === currentId) ? currentId : items[0].id;
}

function normalizeMigration(input = {}) {
  return {
    delta_interval: input.delta_interval ?? DEFAULT_MIGRATION.delta_interval,
    finalize_at: input.finalize_at ?? DEFAULT_MIGRATION.finalize_at,
    finalize_delta_interval: input.finalize_delta_interval ?? DEFAULT_MIGRATION.finalize_delta_interval,
    finalize_window: input.finalize_window ?? DEFAULT_MIGRATION.finalize_window,
    shutdown_mode: input.shutdown_mode ?? DEFAULT_MIGRATION.shutdown_mode,
    snapshot_quiesce: input.snapshot_quiesce ?? DEFAULT_MIGRATION.snapshot_quiesce,
  };
}

function formatTimestamp(value) {
  if (!value) return "";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString();
}

function validateDraft(draft) {
  if (!draft) {
    return { valid: false, message: "VM spec is not initialized.", diskErrors: {}, nicErrors: {} };
  }
  const core = [];
  if (!draft.vm_name) core.push("vm_name");
  if (!draft.vm_moref) core.push("vm_moref");
  if (!draft.zoneid) core.push("zoneid");
  if (!draft.clusterid) core.push("clusterid");
  if (!draft.serviceofferingid) core.push("serviceofferingid");
  if (!draft.boot_storageid) core.push("boot_storageid");

  const diskErrors = {};
  (draft.disks || []).forEach((disk) => {
    if (disk.diskType === "os") return;
    if (!disk.storageid) {
      diskErrors[disk.unit] = "Storage target is required for data disks.";
      return;
    }
    if (!disk.diskofferingid) diskErrors[disk.unit] = "Disk offering is required for data disks.";
  });

  const nicErrors = {};
  const nics = draft.nics || [];
  if (nics.length > 0) {
    nics.forEach((nic) => {
      if (!nic.networkid) nicErrors[nic.id] = "CloudStack network is required for each NIC.";
    });
  } else if (!draft.networkid) {
    core.push("networkid");
  }

  const valid = core.length === 0 && Object.keys(diskErrors).length === 0 && Object.keys(nicErrors).length === 0;
  let message = "";
  if (!valid) {
    if (core.length > 0) message = `Missing required fields: ${core.join(", ")}`;
    else if (Object.keys(diskErrors).length > 0) message = "Complete storage and disk offering for all data disks.";
    else message = "Please map all VM NICs to CloudStack networks.";
  }
  return { valid, message, diskErrors, nicErrors };
}

async function apiRequest(path, options = {}) {
  const { headers: customHeaders = {}, ...fetchOptions } = options;
  const response = await fetch(`${API_BASE}${path}`, {
    ...fetchOptions,
    headers: {
      "Content-Type": "application/json",
      ...customHeaders,
    },
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
  return payload !== null ? payload : text || null;
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

  const [selectedVmNames, setSelectedVmNames] = useState([]);
  const [activeVmName, setActiveVmName] = useState("");
  const [vmSpecsByName, setVmSpecsByName] = useState({});
  const [settingsBusy, setSettingsBusy] = useState(false);
  const [savedSettingsByVm, setSavedSettingsByVm] = useState({});
  const attemptedSettingsLoadRef = useRef(new Set());

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
      if (!raw) return DEFAULT_ENV_STATE;
      return { ...DEFAULT_ENV_STATE, ...JSON.parse(raw) };
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
    if (!selectedVcenter) return {};
    return {
      "x-vcenter-host": selectedVcenter.host || "",
      "x-vcenter-user": selectedVcenter.username || "",
      "x-vcenter-password": selectedVcenter.password || "",
    };
  }, [selectedVcenter]);

  const cloudstackHeaders = useMemo(() => {
    if (!selectedCloudstack) return {};
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
    () =>
      vmwareVms.map((vm) => ({
        name: vm.name,
        moref: vm.moref,
        disks: vm.disks || [],
        nics: vm.nics || [],
      })),
    [vmwareVms]
  );

  const defaultSelections = useMemo(
    () => ({
      zoneid: zones[0]?.id || "",
      clusterid: clusters[0]?.id || "",
      networkid: networks[0]?.id || "",
      serviceofferingid: serviceOfferings[0]?.id || "",
      boot_storageid: storagePools[0]?.id || "",
    }),
    [clusters, networks, serviceOfferings, storagePools, zones]
  );

  const mapInventoryDisks = useCallback((vmDisks, bootStorageId, previousDisks = []) => {
    const prevByUnit = Object.fromEntries((previousDisks || []).map((disk) => [disk.unit, disk]));
    return (vmDisks || []).map((disk, index) => {
      const unit = disk.unit !== null && disk.unit !== undefined ? String(disk.unit) : String(index);
      const prev = prevByUnit[unit];
      const diskType = disk.disk_type || (index === 0 ? "os" : "data");
      return {
        unit,
        label: disk.label || `Disk ${index + 1}`,
        sizeGb: disk.size_gb,
        sizeText: disk.size_gb ? `${disk.size_gb} GB` : "-",
        datastore: disk.datastore || "-",
        diskType,
        storageid: diskType === "os" ? bootStorageId || "" : prev?.storageid || bootStorageId || "",
        diskofferingid: diskType === "os" ? "" : prev?.diskofferingid || "",
      };
    });
  }, []);

  const mapInventoryNics = useCallback((vmNics, defaultNetworkId, previousNics = []) => {
    const prevByID = Object.fromEntries((previousNics || []).map((nic) => [nic.id, nic]));
    return (vmNics || []).map((nic, index) => {
      const id = String(nic.device_key ?? index);
      const prev = prevByID[id];
      return {
        id,
        source_label: nic.label || `NIC ${index + 1}`,
        source_network: nic.network || "",
        source_mac: nic.mac_address || "",
        source_device_key: nic.device_key ?? 0,
        source_index: nic.index ?? index,
        networkid: prev?.networkid || defaultNetworkId || "",
      };
    });
  }, []);

  const buildDraftForVm = useCallback(
    (vm, template = null) => {
      const base = template
        ? {
            zoneid: template.zoneid || defaultSelections.zoneid,
            clusterid: template.clusterid || defaultSelections.clusterid,
            networkid: template.networkid || defaultSelections.networkid,
            serviceofferingid: template.serviceofferingid || defaultSelections.serviceofferingid,
            boot_storageid: template.boot_storageid || defaultSelections.boot_storageid,
            migration: normalizeMigration(template.migration),
          }
        : {
            ...defaultSelections,
            migration: normalizeMigration(),
          };
      return {
        vm_name: vm.name,
        vm_moref: vm.moref || "",
        zoneid: base.zoneid,
        clusterid: base.clusterid,
        networkid: base.networkid,
        serviceofferingid: base.serviceofferingid,
        boot_storageid: base.boot_storageid,
        migration: base.migration,
        disks: mapInventoryDisks(vm.disks || [], base.boot_storageid, template?.disks || []),
        nics: mapInventoryNics(vm.nics || [], base.networkid, template?.nics || []),
      };
    },
    [defaultSelections, mapInventoryDisks, mapInventoryNics]
  );

  const mergeSavedSettingsDraft = useCallback(
    (vm, currentDraft, savedPayload) => {
      if (!vm || !currentDraft || !savedPayload) return currentDraft;
      const nonEmpty = (value, fallback) => {
        if (value === null || value === undefined) return fallback;
        const asString = String(value).trim();
        return asString === "" ? fallback : asString;
      };
      const zoneid = nonEmpty(savedPayload.zoneid, currentDraft.zoneid || defaultSelections.zoneid);
      const clusterid = nonEmpty(savedPayload.clusterid, currentDraft.clusterid || defaultSelections.clusterid);
      const networkid = nonEmpty(savedPayload.networkid, currentDraft.networkid || defaultSelections.networkid);
      const serviceofferingid = nonEmpty(
        savedPayload.serviceofferingid,
        currentDraft.serviceofferingid || defaultSelections.serviceofferingid
      );
      const bootStorage = nonEmpty(savedPayload.boot_storageid, currentDraft.boot_storageid || defaultSelections.boot_storageid);

      const savedDisks = savedPayload.disks || {};
      const mergedDisks = mapInventoryDisks(vm.disks || [], bootStorage, currentDraft.disks || []).map((disk) => {
        if (disk.diskType === "os") {
          return { ...disk, storageid: bootStorage };
        }
        const savedDisk = savedDisks[disk.unit];
        if (!savedDisk) return disk;
        return {
          ...disk,
          storageid: nonEmpty(savedDisk.storageid, disk.storageid),
          diskofferingid: nonEmpty(savedDisk.diskofferingid, disk.diskofferingid),
        };
      });

      const savedNicMappings = savedPayload.nic_mappings || {};
      const mergedNics = mapInventoryNics(vm.nics || [], networkid, currentDraft.nics || []).map((nic) => {
        const savedNic = savedNicMappings[nic.id];
        if (!savedNic) return nic;
        return {
          ...nic,
          source_label: nonEmpty(savedNic.source_label, nic.source_label),
          source_network: nonEmpty(savedNic.source_network, nic.source_network),
          source_mac: nonEmpty(savedNic.source_mac, nic.source_mac),
          source_device_key: savedNic.source_device_key ?? nic.source_device_key,
          source_index: savedNic.source_index ?? nic.source_index,
          networkid: nonEmpty(savedNic.networkid, nic.networkid || networkid),
        };
      });

      return {
        ...currentDraft,
        vm_name: vm.name,
        vm_moref: nonEmpty(savedPayload.vm_moref, vm.moref || currentDraft.vm_moref || ""),
        zoneid,
        clusterid,
        networkid,
        serviceofferingid,
        boot_storageid: bootStorage,
        migration: normalizeMigration(savedPayload.migration || currentDraft.migration || {}),
        disks: mergedDisks,
        nics: mergedNics,
      };
    },
    [defaultSelections, mapInventoryDisks, mapInventoryNics]
  );

  useEffect(() => {
    const vmNames = new Set(vmwareVms.map((vm) => vm.name));
    setSelectedVmNames((prev) => {
      const filtered = prev.filter((name) => vmNames.has(name));
      if (filtered.length > 0) return filtered;
      return vmwareVms[0] ? [vmwareVms[0].name] : [];
    });
    setVmSpecsByName((prev) => {
      const next = {};
      Object.keys(prev).forEach((name) => {
        if (vmNames.has(name)) next[name] = prev[name];
      });
      return next;
    });
  }, [vmwareVms]);

  useEffect(() => {
    if (!selectedVmNames.includes(activeVmName)) setActiveVmName(selectedVmNames[0] || "");
  }, [activeVmName, selectedVmNames]);

  useEffect(() => {
    if (!selectedVmNames.length) return;
    setVmSpecsByName((prev) => {
      const next = { ...prev };
      let changed = false;
      const templateName = selectedVmNames.find((name) => !!next[name]);
      const template = templateName ? next[templateName] : null;
      selectedVmNames.forEach((vmName) => {
        if (next[vmName]) return;
        const vm = vmwareVms.find((item) => item.name === vmName);
        if (!vm) return;
        next[vmName] = buildDraftForVm(vm, template);
        changed = true;
      });
      return changed ? next : prev;
    });
  }, [buildDraftForVm, selectedVmNames, vmwareVms]);

  const activeDraft = activeVmName ? vmSpecsByName[activeVmName] || null : null;
  const activeValidation = useMemo(() => validateDraft(activeDraft), [activeDraft]);

  const canStartMigration = useMemo(() => {
    if (busy || settingsBusy || selectedVmNames.length === 0) return false;
    return selectedVmNames.every((vmName) => validateDraft(vmSpecsByName[vmName]).valid);
  }, [busy, selectedVmNames, settingsBusy, vmSpecsByName]);

  const updateActiveDraft = useCallback(
    (updater) => {
      if (!activeVmName) return;
      setVmSpecsByName((prev) => {
        const current = prev[activeVmName];
        if (!current) return prev;
        return { ...prev, [activeVmName]: updater(current) };
      });
    },
    [activeVmName]
  );

  const loadInventory = useCallback(async () => {
    setInventoryBusy(true);
    try {
      const requests = [
        { key: "vms", label: "VMware VMs", path: "/vmware/vms", headers: vmwareHeaders },
        { key: "zones", label: "CloudStack zones", path: "/cloudstack/zones", headers: cloudstackHeaders },
        { key: "clusters", label: "CloudStack clusters", path: "/cloudstack/clusters", headers: cloudstackHeaders },
        { key: "storage", label: "CloudStack storage", path: "/cloudstack/storage", headers: cloudstackHeaders },
        { key: "networks", label: "CloudStack networks", path: "/cloudstack/networks", headers: cloudstackHeaders },
        { key: "serviceOfferings", label: "CloudStack service offerings", path: "/cloudstack/serviceofferings", headers: cloudstackHeaders },
        { key: "diskOfferings", label: "CloudStack disk offerings", path: "/cloudstack/diskofferings", headers: cloudstackHeaders },
      ];
      const responses = await Promise.all(
        requests.map(async (req) => {
          try {
            const data = await apiRequest(req.path, { headers: req.headers });
            return { key: req.key, label: req.label, data: Array.isArray(data) ? data : [], error: "" };
          } catch (err) {
            return { key: req.key, label: req.label, data: [], error: err?.message || "request failed" };
          }
        })
      );
      const byKey = Object.fromEntries(responses.map((item) => [item.key, item]));
      const vmList = byKey.vms?.data || [];
      const zoneList = byKey.zones?.data || [];
      const clusterList = byKey.clusters?.data || [];
      const storageList = byKey.storage?.data || [];
      const networkList = byKey.networks?.data || [];
      const serviceList = byKey.serviceOfferings?.data || [];
      const diskOfferingList = byKey.diskOfferings?.data || [];

      setVmwareVms(vmList);
      setZones(zoneList);
      setClusters(clusterList);
      setStoragePools(storageList);
      setNetworks(networkList);
      setServiceOfferings(serviceList);
      setDiskOfferings(diskOfferingList);

      setVmSpecsByName((prev) => {
        const next = { ...prev };
        let changed = false;
        Object.keys(next).forEach((name) => {
          const draft = next[name];
          const vm = vmList.find((item) => item.name === name);
          if (!draft || !vm) return;
          const updated = {
            ...draft,
            vm_name: vm.name,
            vm_moref: vm.moref || draft.vm_moref || "",
            zoneid: pickValidOrFirst(draft.zoneid, zoneList),
            clusterid: pickValidOrFirst(draft.clusterid, clusterList),
            networkid: pickValidOrFirst(draft.networkid, networkList),
            serviceofferingid: pickValidOrFirst(draft.serviceofferingid, serviceList),
            boot_storageid: pickValidOrFirst(draft.boot_storageid, storageList),
            migration: normalizeMigration(draft.migration),
          };
          updated.disks = mapInventoryDisks(vm.disks || [], updated.boot_storageid, draft.disks || []);
          updated.nics = mapInventoryNics(vm.nics || [], updated.networkid, draft.nics || []);
          next[name] = updated;
          changed = true;
        });
        return changed ? next : prev;
      });

      const failures = responses.filter((item) => item.error);
      if (failures.length) {
        const msg = failures.map((item) => `${item.label}: ${item.error}`).join(" | ");
        pushToast("error", msg);
      }
    } finally {
      setInventoryBusy(false);
    }
  }, [cloudstackHeaders, mapInventoryDisks, mapInventoryNics, pushToast, vmwareHeaders]);

  const refreshSelectedVmDetails = useCallback(async () => {
    if (selectedVmNames.length === 0) {
      pushToast("error", "Select at least one VM to refresh.");
      return;
    }
    setVmDisksLoading(true);
    try {
      const vms = await apiRequest("/vmware/vms", { headers: vmwareHeaders });
      setVmwareVms(vms);
      const missing = [];
      setVmSpecsByName((prev) => {
        const next = { ...prev };
        selectedVmNames.forEach((name) => {
          const vm = vms.find((item) => item.name === name);
          if (!vm) {
            missing.push(name);
            return;
          }
          const current = next[name] || buildDraftForVm(vm);
          const updated = { ...current, vm_name: vm.name, vm_moref: vm.moref || current.vm_moref || "" };
          updated.disks = mapInventoryDisks(vm.disks || [], updated.boot_storageid, current.disks || []);
          updated.nics = mapInventoryNics(vm.nics || [], updated.networkid, current.nics || []);
          next[name] = updated;
        });
        return next;
      });
      if (missing.length > 0) pushToast("error", `VMs not found: ${missing.join(", ")}`);
    } catch (err) {
      pushToast("error", err.message || "Failed to refresh selected VMs.");
    } finally {
      setVmDisksLoading(false);
    }
  }, [buildDraftForVm, mapInventoryDisks, mapInventoryNics, pushToast, selectedVmNames, vmwareHeaders]);

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
    if (!vmNames.length) return;
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
        if (payload) updates[vmName] = payload;
      });
      if (Object.keys(updates).length > 0) setStatusByVm((prev) => ({ ...prev, ...updates }));
    } catch {
      // silence polling failures
    }
  }, [jobs]);

  const loadLogsForJob = useCallback(
    async (job) => {
      if (!job) return;
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
    },
    [pushToast]
  );

  useEffect(() => {
    loadInventory();
    refreshJobs();
  }, [loadInventory, refreshJobs]);

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
    if (!selectedJob) return;
    loadLogsForJob(selectedJob);
    const interval = setInterval(() => loadLogsForJob(selectedJob), 2000);
    return () => clearInterval(interval);
  }, [selectedJob, loadLogsForJob]);

  const updateField = useCallback(
    (field, value) => {
      updateActiveDraft((draft) => {
        const next = { ...draft, [field]: value };
        if (field === "boot_storageid") {
          next.disks = (draft.disks || []).map((disk) =>
            disk.diskType === "os" ? { ...disk, storageid: value } : disk
          );
        }
        if (field === "networkid") {
          next.nics = (draft.nics || []).map((nic) => (nic.networkid ? nic : { ...nic, networkid: value }));
        }
        return next;
      });
    },
    [updateActiveDraft]
  );

  const updateMigrationField = useCallback(
    (field, value) => {
      updateActiveDraft((draft) => ({
        ...draft,
        migration: {
          ...normalizeMigration(draft.migration),
          [field]: value,
        },
      }));
    },
    [updateActiveDraft]
  );

  const updateDisk = useCallback(
    (unit, field, value) => {
      updateActiveDraft((draft) => ({
        ...draft,
        disks: (draft.disks || []).map((disk) => (disk.unit === unit ? { ...disk, [field]: value } : disk)),
      }));
    },
    [updateActiveDraft]
  );

  const updateNic = useCallback(
    (id, field, value) => {
      updateActiveDraft((draft) => ({
        ...draft,
        nics: (draft.nics || []).map((nic) => (nic.id === id ? { ...nic, [field]: value } : nic)),
      }));
    },
    [updateActiveDraft]
  );

  const toggleVmSelection = useCallback((vmName, checked) => {
    setSelectedVmNames((prev) => {
      if (checked) {
        if (prev.includes(vmName)) return prev;
        return [...prev, vmName];
      }
      return prev.filter((name) => name !== vmName);
    });
  }, []);

  const selectAllVms = useCallback(() => setSelectedVmNames(vmwareVms.map((vm) => vm.name)), [vmwareVms]);
  const clearVmSelection = useCallback(() => setSelectedVmNames([]), []);

  const buildSpecPayload = useCallback((draft) => {
    const disks = {};
    (draft.disks || []).forEach((disk) => {
      if (disk.diskType === "os") return;
      disks[disk.unit] = {
        storageid: disk.storageid || draft.boot_storageid,
        diskofferingid: disk.diskofferingid,
      };
    });

    const nicMappings = {};
    (draft.nics || []).forEach((nic) => {
      nicMappings[nic.id] = {
        source_label: nic.source_label || "",
        source_network: nic.source_network || "",
        source_mac: nic.source_mac || "",
        source_device_key: Number(nic.source_device_key) || 0,
        source_index: Number(nic.source_index) || 0,
        networkid: nic.networkid || "",
      };
    });

    const migration = {
      delta_interval: Number(draft.migration?.delta_interval) || 300,
    };
    if (draft.migration?.finalize_at) migration.finalize_at = draft.migration.finalize_at;
    if (draft.migration?.finalize_delta_interval) migration.finalize_delta_interval = Number(draft.migration.finalize_delta_interval);
    if (draft.migration?.finalize_window) migration.finalize_window = Number(draft.migration.finalize_window);
    if (draft.migration?.shutdown_mode) migration.shutdown_mode = draft.migration.shutdown_mode;
    if (draft.migration?.snapshot_quiesce) migration.snapshot_quiesce = draft.migration.snapshot_quiesce;

    return {
      vm_name: draft.vm_name,
      vm_moref: draft.vm_moref,
      zoneid: draft.zoneid,
      clusterid: draft.clusterid,
      networkid: draft.networkid,
      serviceofferingid: draft.serviceofferingid,
      boot_storageid: draft.boot_storageid,
      disks,
      nic_mappings: nicMappings,
      migration,
    };
  }, []);

  const saveSettings = useCallback(
    async (vmNames, { silent = false } = {}) => {
      if (!vmNames.length) {
        if (!silent) pushToast("error", "Select at least one VM.");
        return false;
      }
      setSettingsBusy(true);
      try {
        let count = 0;
        const updates = {};
        for (const vmName of vmNames) {
          const draft = vmSpecsByName[vmName];
          if (!draft) continue;
          const response = await apiRequest("/migration/settings", {
            method: "POST",
            headers: { ...vmwareHeaders, ...cloudstackHeaders },
            body: JSON.stringify(buildSpecPayload(draft)),
          });
          updates[vmName] = {
            saved_at: response?.saved_at || new Date().toISOString(),
            settings_file: response?.settings_file || "",
          };
          attemptedSettingsLoadRef.current.add(vmName);
          count++;
        }
        if (count > 0) {
          setSavedSettingsByVm((prev) => ({ ...prev, ...updates }));
          if (!silent) pushToast("success", `Saved settings for ${count} VM(s).`);
          return true;
        }
        if (!silent) pushToast("error", "No VM draft available to save.");
        return false;
      } catch (err) {
        if (!silent) pushToast("error", err.message || "Failed to save settings.");
        return false;
      } finally {
        setSettingsBusy(false);
      }
    },
    [buildSpecPayload, cloudstackHeaders, pushToast, vmSpecsByName, vmwareHeaders]
  );

  const loadSavedSettings = useCallback(
    async (vmNames, { silent = false, force = false } = {}) => {
      if (!vmNames.length) return;
      setSettingsBusy(true);
      try {
        const updates = {};
        const meta = {};
        let loaded = 0;
        let missing = 0;
        for (const vmName of vmNames) {
          if (!force && attemptedSettingsLoadRef.current.has(vmName)) continue;
          attemptedSettingsLoadRef.current.add(vmName);
          const vm = vmwareVms.find((item) => item.name === vmName);
          const draft = vmSpecsByName[vmName];
          const vmMoref = draft?.vm_moref || vm?.moref || "";
          try {
            const response = await apiRequest(
              `/migration/settings?vm_name=${encodeURIComponent(vmName)}&vm_moref=${encodeURIComponent(vmMoref)}`,
              { headers: vmwareHeaders }
            );
            if (!response?.settings) continue;
            updates[vmName] = response.settings;
            meta[vmName] = {
              saved_at: response.saved_at || "",
              settings_file: response.settings_file || "",
            };
            loaded++;
          } catch (err) {
            const msg = (err?.message || "").toLowerCase();
            if (msg.includes("no saved settings found") || msg.includes("not found")) {
              missing++;
              continue;
            }
            if (!silent) pushToast("error", `${vmName}: ${err?.message || "failed to load settings"}`);
          }
        }

        if (Object.keys(updates).length > 0) {
          setVmSpecsByName((prev) => {
            const next = { ...prev };
            Object.keys(updates).forEach((vmName) => {
              const vm = vmwareVms.find((item) => item.name === vmName);
              const current = next[vmName];
              if (!vm || !current) return;
              next[vmName] = mergeSavedSettingsDraft(vm, current, updates[vmName]);
            });
            return next;
          });
          setSavedSettingsByVm((prev) => ({ ...prev, ...meta }));
          if (!silent) pushToast("success", `Loaded saved settings for ${loaded} VM(s).`);
          return;
        }

        if (!silent && missing > 0) {
          pushToast("error", "No saved settings found for selected VM(s).");
        }
      } finally {
        setSettingsBusy(false);
      }
    },
    [mergeSavedSettingsDraft, pushToast, vmSpecsByName, vmwareHeaders, vmwareVms]
  );

  useEffect(() => {
    const pending = selectedVmNames.filter((name) => !attemptedSettingsLoadRef.current.has(name));
    if (pending.length === 0) return;
    loadSavedSettings(pending, { silent: true, force: false });
  }, [loadSavedSettings, selectedVmNames]);

  const createSpec = useCallback(
    async (startAfter) => {
      if (selectedVmNames.length === 0) {
        pushToast("error", "Select at least one VM.");
        return;
      }
      const invalid = selectedVmNames
        .map((vmName) => ({ vmName, validation: validateDraft(vmSpecsByName[vmName]) }))
        .find((item) => !item.validation.valid);
      if (invalid) {
        setActiveVmName(invalid.vmName);
        pushToast("error", `${invalid.vmName}: ${invalid.validation.message || "Invalid VM spec"}`);
        return;
      }

      setBusy(true);
      try {
        const saved = await saveSettings(selectedVmNames, { silent: true });
        if (!saved) throw new Error("Failed to save VM settings before spec generation.");
        const specFiles = [];
        const startedJobs = [];
        for (const vmName of selectedVmNames) {
          const draft = vmSpecsByName[vmName];
          const specResp = await apiRequest("/migration/spec", {
            method: "POST",
            headers: { ...vmwareHeaders, ...cloudstackHeaders },
            body: JSON.stringify(buildSpecPayload(draft)),
          });
          specFiles.push(specResp.spec_file);
          if (startAfter) {
            const startResp = await apiRequest("/migration/start", {
              method: "POST",
              body: JSON.stringify({ vm_name: vmName, spec_file: specResp.spec_file }),
            });
            startedJobs.push(startResp.job_id);
          }
        }
        setLastSpecFile(specFiles.join("\n"));
        pushToast("success", `Generated ${specFiles.length} spec file(s).`);
        if (startAfter) {
          pushToast("success", `Started ${startedJobs.length} migration job(s).`);
          setTab("progress");
          refreshJobs();
        }
      } catch (err) {
        pushToast("error", err.message || "Failed to generate/start migration.");
      } finally {
        setBusy(false);
      }
    },
    [buildSpecPayload, cloudstackHeaders, pushToast, refreshJobs, saveSettings, selectedVmNames, vmSpecsByName, vmwareHeaders]
  );

  const finalizeVm = useCallback(
    async (vmName) => {
      try {
        const response = await apiRequest(`/migration/finalize/${encodeURIComponent(vmName)}`, { method: "POST" });
        pushToast("success", response.message);
      } catch (err) {
        pushToast("error", err.message || "Failed to finalize migration.");
      }
    },
    [pushToast]
  );

  const selectedVmStatus = selectedJob ? statusByVm[selectedJob.vm_name] : null;
  const selectedSettingsRows = useMemo(
    () =>
      selectedVmNames.map((vmName) => {
        const draft = vmSpecsByName[vmName];
        const nics = draft?.nics || [];
        const mappedNICs = nics.filter((nic) => !!nic.networkid).length;
        const saved = savedSettingsByVm[vmName];
        return {
          vmName,
          zoneid: draft?.zoneid || "",
          clusterid: draft?.clusterid || "",
          serviceofferingid: draft?.serviceofferingid || "",
          boot_storageid: draft?.boot_storageid || "",
          delta_interval: draft?.migration?.delta_interval || "",
          mappedNICs,
          nicCount: nics.length,
          savedAt: saved?.saved_at || "",
        };
      }),
    [savedSettingsByVm, selectedVmNames, vmSpecsByName]
  );

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1>VMware to CloudStack Migrator</h1>
          <p>Select multiple VMs, define per-VM disk and NIC mappings, and run migrations in parallel.</p>
        </div>
        <div className="tab-buttons">
          <button className={tab === "new" ? "active" : ""} onClick={() => setTab("new")}>New Migration</button>
          <button className={tab === "progress" ? "active" : ""} onClick={() => setTab("progress")}>Progress</button>
        </div>
      </header>

      <div className="toast-stack">
        {toasts.map((toast) => (
          <div key={toast.id} className={`toast ${toast.type}`}>{toast.message}</div>
        ))}
      </div>

      {tab === "new" ? (
        <>
          <EnvironmentManager envState={envState} onChange={setEnvState} onToast={pushToast} />

          <VMSelector
            vmOptions={vmOptions}
            selectedVmNames={selectedVmNames}
            activeVmName={activeVmName}
            onToggleVm={toggleVmSelection}
            onSetActiveVm={setActiveVmName}
            onSelectAll={selectAllVms}
            onClearSelection={clearVmSelection}
            onRefreshSelected={refreshSelectedVmDetails}
            loading={vmDisksLoading}
          />

          {activeDraft ? (
            <>
              <section className="panel">
                <div className="subsection-title-row">
                  <h2>Target and Strategy ({activeDraft.vm_name})</h2>
                  <div className="actions compact">
                    <button className="secondary" onClick={loadInventory} disabled={inventoryBusy || settingsBusy}>
                      {inventoryBusy ? "Loading..." : "Reload Inventory"}
                    </button>
                    <button
                      className="secondary"
                      onClick={() => loadSavedSettings([activeDraft.vm_name], { silent: false, force: true })}
                      disabled={settingsBusy}
                    >
                      {settingsBusy ? "Working..." : "Load Saved (Active VM)"}
                    </button>
                    <button
                      className="secondary"
                      onClick={() => saveSettings([activeDraft.vm_name], { silent: false })}
                      disabled={settingsBusy}
                    >
                      {settingsBusy ? "Working..." : "Save Settings (Active VM)"}
                    </button>
                    <button
                      className="secondary"
                      onClick={() => saveSettings(selectedVmNames, { silent: false })}
                      disabled={settingsBusy || selectedVmNames.length === 0}
                    >
                      {settingsBusy ? "Working..." : `Save Settings (Selected ${selectedVmNames.length})`}
                    </button>
                  </div>
                </div>
                <div className="form-grid">
                  <label>VM MoRef<input value={activeDraft.vm_moref} onChange={(e) => updateField("vm_moref", e.target.value)} placeholder="vm-123" /></label>
                  <label>Zone<select value={activeDraft.zoneid} onChange={(e) => updateField("zoneid", e.target.value)}><option value="">Select zone</option>{zones.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Cluster<select value={activeDraft.clusterid} onChange={(e) => updateField("clusterid", e.target.value)}><option value="">Select cluster</option>{clusters.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Fallback Network<select value={activeDraft.networkid} onChange={(e) => updateField("networkid", e.target.value)}><option value="">Select network</option>{networks.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Service Offering<select value={activeDraft.serviceofferingid} onChange={(e) => updateField("serviceofferingid", e.target.value)}><option value="">Select service offering</option>{serviceOfferings.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Boot Storage<select value={activeDraft.boot_storageid} onChange={(e) => updateField("boot_storageid", e.target.value)}><option value="">Select boot storage</option>{storagePools.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Delta Interval (sec)<input type="number" min="1" value={activeDraft.migration.delta_interval} onChange={(e) => updateMigrationField("delta_interval", e.target.value)} /></label>
                  <label>Finalize At (ISO)<input value={activeDraft.migration.finalize_at} onChange={(e) => updateMigrationField("finalize_at", e.target.value)} placeholder="2026-03-12T23:30:00+00:00" /></label>
                  <label>Finalize Delta Interval<input type="number" min="1" value={activeDraft.migration.finalize_delta_interval} onChange={(e) => updateMigrationField("finalize_delta_interval", e.target.value)} /></label>
                  <label>Finalize Window<input type="number" min="1" value={activeDraft.migration.finalize_window} onChange={(e) => updateMigrationField("finalize_window", e.target.value)} /></label>
                  <label>Shutdown Mode<input value={activeDraft.migration.shutdown_mode} onChange={(e) => updateMigrationField("shutdown_mode", e.target.value)} placeholder="auto" /></label>
                  <label>Snapshot Quiesce<input value={activeDraft.migration.snapshot_quiesce} onChange={(e) => updateMigrationField("snapshot_quiesce", e.target.value)} placeholder="auto" /></label>
                </div>
                <p className="hint small">
                  {savedSettingsByVm[activeDraft.vm_name]?.saved_at
                    ? `Saved settings: ${formatTimestamp(savedSettingsByVm[activeDraft.vm_name].saved_at)}`
                    : "Saved settings: not saved yet for this VM"}
                </p>
              </section>

              <section className="panel">
                <h3>Selected VM Settings Summary</h3>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>VM</th>
                        <th>Zone</th>
                        <th>Cluster</th>
                        <th>Service Offering</th>
                        <th>Boot Storage</th>
                        <th>Delta (sec)</th>
                        <th>NIC Mapped</th>
                        <th>Saved</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedSettingsRows.length === 0 ? (
                        <tr>
                          <td colSpan={8}>No VM selected.</td>
                        </tr>
                      ) : (
                        selectedSettingsRows.map((row) => (
                          <tr key={row.vmName} className={row.vmName === activeVmName ? "selected-row" : ""}>
                            <td>{row.vmName}</td>
                            <td><code>{row.zoneid || "-"}</code></td>
                            <td><code>{row.clusterid || "-"}</code></td>
                            <td><code>{row.serviceofferingid || "-"}</code></td>
                            <td><code>{row.boot_storageid || "-"}</code></td>
                            <td>{row.delta_interval || "-"}</td>
                            <td>{row.mappedNICs}/{row.nicCount}</td>
                            <td>{row.savedAt ? formatTimestamp(row.savedAt) : "Not saved"}</td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </section>

              <DiskTable
                disks={activeDraft.disks || []}
                storagePools={storagePools}
                diskOfferings={diskOfferings}
                onDiskChange={updateDisk}
                validationByUnit={activeValidation.diskErrors}
              />

              <NicTable
                nics={activeDraft.nics || []}
                networks={networks}
                fallbackNetworkId={activeDraft.networkid}
                onNicChange={updateNic}
                validationByNic={activeValidation.nicErrors}
              />

              <section className="panel">
                <div className="actions">
                  <button disabled={busy || settingsBusy || selectedVmNames.length === 0} onClick={() => createSpec(false)}>Generate Spec ({selectedVmNames.length})</button>
                  <button disabled={!canStartMigration} onClick={() => createSpec(true)}>Start Migration ({selectedVmNames.length})</button>
                </div>
                {!activeValidation.valid ? <p className="field-error">{activeValidation.message}</p> : null}
                {lastSpecFile ? <p className="hint">Last generated spec(s):<br /><code className="inline-block">{lastSpecFile}</code></p> : null}
              </section>
            </>
          ) : (
            <section className="panel">
              <p className="hint">Select one or more VMs to define per-VM migration specs.</p>
            </section>
          )}
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
              {selectedJob ? <p className="hint">Job <code>{selectedJob.job_id}</code> | VM <strong>{selectedJob.vm_name}</strong></p> : null}
              <div className="logs-grid">
                <div><h4>STDOUT</h4><pre>{logs.stdout || "No stdout logs available."}</pre></div>
                <div><h4>STDERR</h4><pre>{logs.stderr || "No stderr logs available."}</pre></div>
              </div>
              {selectedVmStatus?.job_error ? <p className="field-error">{selectedVmStatus.job_error}</p> : null}
            </div>
          }
        />
      )}
    </div>
  );
}
