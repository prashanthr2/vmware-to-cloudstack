import { useCallback, useEffect, useMemo, useState } from "react";

import DiskTable from "./components/DiskTable";
import EnvironmentManager from "./components/EnvironmentManager";
import MigrationProgress from "./components/MigrationProgress";
import NicTable from "./components/NicTable";
import VMSelector from "./components/VMSelector";

const API_BASE = import.meta.env.VITE_API_BASE || `${window.location.protocol}//${window.location.hostname}:8000`;
const ENV_STORAGE_KEY = "vm_migrator_environments_v1";
const CONFIG_DEFAULT_VCENTER_ID = "config-default-vcenter";
const CONFIG_DEFAULT_CLOUDSTACK_ID = "config-default-cloudstack";

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
  start_vm_after_import: false,
};

const BOOT_TYPE_OPTIONS = [
  { value: "", label: "Default (import-detected)" },
  { value: "BIOS", label: "BIOS" },
  { value: "UEFI", label: "UEFI" },
];

const UEFI_BOOT_MODE_OPTIONS = [
  { value: "", label: "Default" },
  { value: "Legacy", label: "Legacy" },
  { value: "Secure", label: "Secure" },
];

const ROOT_DISK_CONTROLLER_OPTIONS = [
  { value: "", label: "Default (CloudStack/import-detected)" },
  { value: "virtio-scsi", label: "virtio-scsi" },
  { value: "virtio", label: "virtio" },
  { value: "sata", label: "sata" },
  { value: "ide", label: "ide" },
];

const NIC_ADAPTER_OPTIONS = [
  { value: "", label: "Default (CloudStack/import-detected)" },
  { value: "virtio", label: "virtio" },
  { value: "e1000", label: "e1000" },
  { value: "rtl8139", label: "rtl8139" },
  { value: "pcnet", label: "pcnet" },
];

function optionLabel(item) {
  return item.name || item.description || item.displaytext || item.id || "Unknown";
}

function pickValidOrFirst(currentId, items) {
  if (!items.length) return "";
  return items.some((item) => item.id === currentId) ? currentId : items[0].id;
}

function itemZoneID(item) {
  if (!item || typeof item !== "object") return "";
  return String(item.zoneid || item.zoneId || item.zone_id || "").trim();
}

function filterByZone(items, zoneID) {
  if (!Array.isArray(items) || items.length === 0) return [];
  const zone = String(zoneID || "").trim();
  if (!zone) return items;
  const scoped = items.filter((item) => itemZoneID(item) === zone);
  if (scoped.length > 0) return scoped;
  const hasExplicitZone = items.some((item) => itemZoneID(item) !== "");
  return hasExplicitZone ? scoped : items;
}

function normalizeMigration(input = {}) {
  return {
    delta_interval: input.delta_interval ?? DEFAULT_MIGRATION.delta_interval,
    finalize_at: input.finalize_at ?? DEFAULT_MIGRATION.finalize_at,
    finalize_delta_interval: input.finalize_delta_interval ?? DEFAULT_MIGRATION.finalize_delta_interval,
    finalize_window: input.finalize_window ?? DEFAULT_MIGRATION.finalize_window,
    shutdown_mode: input.shutdown_mode ?? DEFAULT_MIGRATION.shutdown_mode,
    snapshot_quiesce: input.snapshot_quiesce ?? DEFAULT_MIGRATION.snapshot_quiesce,
    start_vm_after_import: input.start_vm_after_import ?? DEFAULT_MIGRATION.start_vm_after_import,
  };
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
    const nicByNetwork = {};
    nics.forEach((nic) => {
      const networkID = (nic.networkid || "").trim();
      if (!networkID) {
        nicErrors[nic.id] = "CloudStack network is required for each NIC.";
        return;
      }
      const prevNicID = nicByNetwork[networkID];
      if (prevNicID) {
        nicErrors[nic.id] = "Each NIC must use a different CloudStack network.";
        if (!nicErrors[prevNicID]) {
          nicErrors[prevNicID] = "Each NIC must use a different CloudStack network.";
        }
        return;
      }
      nicByNetwork[networkID] = nic.id;
    });
  } else {
    core.push("nic_mappings");
  }

  const valid = core.length === 0 && Object.keys(diskErrors).length === 0 && Object.keys(nicErrors).length === 0;
  let message = "";
  if (!valid) {
    if (core.includes("nic_mappings")) message = "NIC mappings are required. No VMware NICs detected for this VM.";
    else if (core.length > 0) message = `Missing required fields: ${core.join(", ")}`;
    else if (Object.keys(diskErrors).length > 0) message = "Complete storage and disk offering for all data disks.";
    else message = "Please map all VM NICs to unique CloudStack networks.";
  }
  return { valid, message, diskErrors, nicErrors };
}

function dedupeByID(items) {
  const seen = new Set();
  return (items || []).filter((item) => {
    const id = String(item?.id || "").trim();
    if (!id || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

function mergeConfigDefaultEnvironments(current, defaults) {
  const next = {
    ...DEFAULT_ENV_STATE,
    ...current,
    vcenters: Array.isArray(current?.vcenters) ? [...current.vcenters] : [],
    cloudstacks: Array.isArray(current?.cloudstacks) ? [...current.cloudstacks] : [],
  };

  next.vcenters = next.vcenters.filter((item) => item?.id !== CONFIG_DEFAULT_VCENTER_ID);
  next.cloudstacks = next.cloudstacks.filter((item) => item?.id !== CONFIG_DEFAULT_CLOUDSTACK_ID);

  const defaultVC = defaults?.vcenter;
  if (defaultVC?.available) {
    next.vcenters.unshift({
      id: CONFIG_DEFAULT_VCENTER_ID,
      name: defaultVC.name || defaultVC.host || "Default vCenter",
      host: defaultVC.host || "",
      username: defaultVC.username || "",
      password: "",
      source: "config",
    });
  }

  const defaultCS = defaults?.cloudstack;
  if (defaultCS?.available) {
    next.cloudstacks.unshift({
      id: CONFIG_DEFAULT_CLOUDSTACK_ID,
      name: defaultCS.name || defaultCS.apiUrl || "Default CloudStack",
      apiUrl: defaultCS.apiUrl || "",
      apiKey: "",
      secretKey: "",
      source: "config",
    });
  }

  next.vcenters = dedupeByID(next.vcenters);
  next.cloudstacks = dedupeByID(next.cloudstacks);

  if (!next.vcenters.some((item) => item.id === next.selectedVcenterId)) {
    next.selectedVcenterId = next.vcenters[0]?.id || "";
  }
  if (!next.cloudstacks.some((item) => item.id === next.selectedCloudstackId)) {
    next.selectedCloudstackId = next.cloudstacks[0]?.id || "";
  }

  return next;
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
  const [osTypes, setOsTypes] = useState([]);

  const [selectedVmNames, setSelectedVmNames] = useState([]);
  const [activeVmName, setActiveVmName] = useState("");
  const [vmSpecsByName, setVmSpecsByName] = useState({});

  const [lastSpecFile, setLastSpecFile] = useState("");
  const [jobs, setJobs] = useState([]);
  const [statusByJob, setStatusByJob] = useState({});
  const [showJobHistory, setShowJobHistory] = useState(false);
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

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const defaults = await apiRequest("/environments/defaults");
        if (cancelled) return;
        setEnvState((prev) => mergeConfigDefaultEnvironments(prev, defaults));
      } catch {
        // Keep working with local env profiles if defaults endpoint is unavailable.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const selectedVcenter = useMemo(
    () => envState.vcenters.find((item) => item.id === envState.selectedVcenterId) || null,
    [envState.selectedVcenterId, envState.vcenters]
  );
  const selectedCloudstack = useMemo(
    () => envState.cloudstacks.find((item) => item.id === envState.selectedCloudstackId) || null,
    [envState.cloudstacks, envState.selectedCloudstackId]
  );

  const vmwareHeaders = useMemo(() => {
    if (!selectedVcenter || selectedVcenter.source === "config") return {};
    return {
      "x-vcenter-host": selectedVcenter.host || "",
      "x-vcenter-user": selectedVcenter.username || "",
      "x-vcenter-password": selectedVcenter.password || "",
    };
  }, [selectedVcenter]);

  const cloudstackHeaders = useMemo(() => {
    if (!selectedCloudstack || selectedCloudstack.source === "config") return {};
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
    () => {
      const zoneid = zones[0]?.id || "";
      return {
        zoneid,
        clusterid: pickValidOrFirst("", filterByZone(clusters, zoneid)),
        serviceofferingid: pickValidOrFirst("", filterByZone(serviceOfferings, zoneid)),
        boot_storageid: pickValidOrFirst("", filterByZone(storagePools, zoneid)),
      };
    },
    [clusters, serviceOfferings, storagePools, zones]
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

  const mapInventoryNics = useCallback((vmNics, previousNics = []) => {
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
        networkid: prev?.networkid || "",
      };
    });
  }, []);

  const buildDraftForVm = useCallback(
    (vm, template = null) => {
      const base = template
        ? {
            zoneid: template.zoneid || defaultSelections.zoneid,
            clusterid: "",
            serviceofferingid: template.serviceofferingid || defaultSelections.serviceofferingid,
            boot_storageid: "",
            ostypeid: template.ostypeid || "",
            boottype: template.boottype || "",
            bootmode: template.bootmode || "",
            rootdiskcontroller: template.rootdiskcontroller || "",
            nicadapter: template.nicadapter || "",
            migration: normalizeMigration(template.migration),
          }
        : {
            ...defaultSelections,
            ostypeid: "",
            boottype: "",
            bootmode: "",
            rootdiskcontroller: "",
            nicadapter: "",
            migration: normalizeMigration(),
          };
      const zoneClusters = filterByZone(clusters, base.zoneid);
      const zoneServiceOfferings = filterByZone(serviceOfferings, base.zoneid);
      const zoneStorage = filterByZone(storagePools, base.zoneid);
      const zoneNetworks = filterByZone(networks, base.zoneid);
      const zoneDiskOfferings = filterByZone(diskOfferings, base.zoneid);
      base.clusterid = pickValidOrFirst(template?.clusterid || base.clusterid, zoneClusters);
      base.serviceofferingid = pickValidOrFirst(template?.serviceofferingid || base.serviceofferingid, zoneServiceOfferings);
      base.boot_storageid = pickValidOrFirst(template?.boot_storageid || base.boot_storageid, zoneStorage);
      return {
        vm_name: vm.name,
        vm_moref: vm.moref || "",
        zoneid: base.zoneid,
        clusterid: base.clusterid,
        serviceofferingid: base.serviceofferingid,
        boot_storageid: base.boot_storageid,
        ostypeid: base.ostypeid,
        boottype: base.boottype,
        bootmode: base.boottype === "UEFI" ? base.bootmode : "",
        rootdiskcontroller: base.rootdiskcontroller,
        nicadapter: base.nicadapter,
        migration: base.migration,
        disks: mapInventoryDisks(vm.disks || [], base.boot_storageid, template?.disks || []).map((disk) => {
          if (disk.diskType === "os") return disk;
          return {
            ...disk,
            storageid: pickValidOrFirst(disk.storageid, zoneStorage),
            diskofferingid: pickValidOrFirst(disk.diskofferingid, zoneDiskOfferings),
          };
        }),
        nics: mapInventoryNics(vm.nics || [], template?.nics || []).map((nic) => ({
          ...nic,
          networkid: pickValidOrFirst(nic.networkid, zoneNetworks),
        })),
      };
    },
    [clusters, defaultSelections, diskOfferings, mapInventoryDisks, mapInventoryNics, networks, serviceOfferings, storagePools]
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
  const activeZoneID = activeDraft?.zoneid || "";
  const clustersForActiveZone = useMemo(() => filterByZone(clusters, activeZoneID), [clusters, activeZoneID]);
  const storagePoolsForActiveZone = useMemo(() => filterByZone(storagePools, activeZoneID), [storagePools, activeZoneID]);
  const networksForActiveZone = useMemo(() => filterByZone(networks, activeZoneID), [networks, activeZoneID]);
  const serviceOfferingsForActiveZone = useMemo(() => filterByZone(serviceOfferings, activeZoneID), [serviceOfferings, activeZoneID]);
  const diskOfferingsForActiveZone = useMemo(() => filterByZone(diskOfferings, activeZoneID), [diskOfferings, activeZoneID]);

  const canStartMigration = useMemo(() => {
    if (busy || selectedVmNames.length === 0) return false;
    return selectedVmNames.every((vmName) => validateDraft(vmSpecsByName[vmName]).valid);
  }, [busy, selectedVmNames, vmSpecsByName]);

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
        { key: "osTypes", label: "CloudStack guest OS types", path: "/cloudstack/ostypes", headers: cloudstackHeaders },
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
      const osTypeList = byKey.osTypes?.data || [];

      setVmwareVms(vmList);
      setZones(zoneList);
      setClusters(clusterList);
      setStoragePools(storageList);
      setNetworks(networkList);
      setServiceOfferings(serviceList);
      setDiskOfferings(diskOfferingList);
      setOsTypes(osTypeList);

      setVmSpecsByName((prev) => {
        const next = { ...prev };
        let changed = false;
        Object.keys(next).forEach((name) => {
          const draft = next[name];
          const vm = vmList.find((item) => item.name === name);
          if (!draft || !vm) return;
          const zoneid = pickValidOrFirst(draft.zoneid, zoneList);
          const zoneClusters = filterByZone(clusterList, zoneid);
          const zoneStorage = filterByZone(storageList, zoneid);
          const zoneNetworks = filterByZone(networkList, zoneid);
          const zoneServiceOfferings = filterByZone(serviceList, zoneid);
          const zoneDiskOfferings = filterByZone(diskOfferingList, zoneid);
          const updated = {
            ...draft,
            vm_name: vm.name,
            vm_moref: vm.moref || draft.vm_moref || "",
            zoneid,
            clusterid: pickValidOrFirst(draft.clusterid, zoneClusters),
            serviceofferingid: pickValidOrFirst(draft.serviceofferingid, zoneServiceOfferings),
            boot_storageid: pickValidOrFirst(draft.boot_storageid, zoneStorage),
            migration: normalizeMigration(draft.migration),
          };
          updated.disks = mapInventoryDisks(vm.disks || [], updated.boot_storageid, draft.disks || []).map((disk) => {
            if (disk.diskType === "os") return disk;
            return {
              ...disk,
              storageid: pickValidOrFirst(disk.storageid, zoneStorage),
              diskofferingid: pickValidOrFirst(disk.diskofferingid, zoneDiskOfferings),
            };
          });
          updated.nics = mapInventoryNics(vm.nics || [], draft.nics || []).map((nic) => ({
            ...nic,
            networkid: pickValidOrFirst(nic.networkid, zoneNetworks),
          }));
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
          const zoneStorage = filterByZone(storagePools, current.zoneid);
          const zoneNetworks = filterByZone(networks, current.zoneid);
          const zoneDiskOfferings = filterByZone(diskOfferings, current.zoneid);
          const updated = { ...current, vm_name: vm.name, vm_moref: vm.moref || current.vm_moref || "" };
          updated.disks = mapInventoryDisks(vm.disks || [], updated.boot_storageid, current.disks || []).map((disk) => {
            if (disk.diskType === "os") return disk;
            return {
              ...disk,
              storageid: pickValidOrFirst(disk.storageid, zoneStorage),
              diskofferingid: pickValidOrFirst(disk.diskofferingid, zoneDiskOfferings),
            };
          });
          updated.nics = mapInventoryNics(vm.nics || [], current.nics || []).map((nic) => ({
            ...nic,
            networkid: pickValidOrFirst(nic.networkid, zoneNetworks),
          }));
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
  }, [buildDraftForVm, diskOfferings, mapInventoryDisks, mapInventoryNics, networks, pushToast, selectedVmNames, storagePools, vmwareHeaders]);

  const refreshJobs = useCallback(async () => {
    try {
      const latestOnly = showJobHistory ? "false" : "true";
      const list = await apiRequest(`/migration/jobs?limit=200&latest_per_vm=${latestOnly}`);
      setJobs(list);
    } catch (err) {
      pushToast("error", err.message || "Failed to load jobs.");
    }
  }, [pushToast, showJobHistory]);

  const pollStatuses = useCallback(async () => {
    const activeJobs = (jobs || []).filter((job) => {
      const status = String(job?.status || "").toLowerCase();
      return status === "running" || status === "queued";
    });
    if (!activeJobs.length) return;
    try {
      const responses = await Promise.all(
        activeJobs.map(async (job) => {
          try {
            const status = await apiRequest(
              `/migration/status/${encodeURIComponent(job.vm_name)}?job_id=${encodeURIComponent(job.job_id)}`
            );
            return [job.job_id, status];
          } catch {
            return [job.job_id, null];
          }
        })
      );
      const updates = {};
      responses.forEach(([jobID, payload]) => {
        if (payload) updates[jobID] = payload;
      });
      if (Object.keys(updates).length > 0) setStatusByJob((prev) => ({ ...prev, ...updates }));
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
  }, [loadInventory]);

  useEffect(() => {
    refreshJobs();
  }, [refreshJobs]);

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

  useEffect(() => {
    if (!jobs.length) {
      if (selectedJob) setSelectedJob(null);
      return;
    }
    if (selectedJob && jobs.some((job) => job.job_id === selectedJob.job_id)) {
      return;
    }
    setSelectedJob(jobs[0]);
  }, [jobs, selectedJob]);

  useEffect(() => {
    const valid = new Set(jobs.map((job) => job.job_id));
    setStatusByJob((prev) => {
      const next = {};
      Object.keys(prev).forEach((jobID) => {
        if (valid.has(jobID)) next[jobID] = prev[jobID];
      });
      return next;
    });
  }, [jobs]);

  const updateField = useCallback(
    (field, value) => {
      updateActiveDraft((draft) => {
        const next = { ...draft, [field]: value };
        if (field === "zoneid") {
          const zoneClusters = filterByZone(clusters, value);
          const zoneStorage = filterByZone(storagePools, value);
          const zoneNetworks = filterByZone(networks, value);
          const zoneServiceOfferings = filterByZone(serviceOfferings, value);
          const zoneDiskOfferings = filterByZone(diskOfferings, value);
          next.clusterid = pickValidOrFirst(draft.clusterid, zoneClusters);
          next.serviceofferingid = pickValidOrFirst(draft.serviceofferingid, zoneServiceOfferings);
          next.boot_storageid = pickValidOrFirst(draft.boot_storageid, zoneStorage);
          next.disks = (draft.disks || []).map((disk) => {
            if (disk.diskType === "os") {
              return { ...disk, storageid: next.boot_storageid };
            }
            return {
              ...disk,
              storageid: pickValidOrFirst(disk.storageid, zoneStorage),
              diskofferingid: pickValidOrFirst(disk.diskofferingid, zoneDiskOfferings),
            };
          });
          next.nics = (draft.nics || []).map((nic) => ({
            ...nic,
            networkid: pickValidOrFirst(nic.networkid, zoneNetworks),
          }));
        }
        if (field === "boot_storageid") {
          next.disks = (draft.disks || []).map((disk) =>
            disk.diskType === "os" ? { ...disk, storageid: value } : disk
          );
        }
        if (field === "boottype" && value !== "UEFI") {
          next.bootmode = "";
        }
        return next;
      });
    },
    [clusters, diskOfferings, networks, serviceOfferings, storagePools, updateActiveDraft]
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
    if (draft.migration?.start_vm_after_import) migration.start_vm_after_import = true;

    return {
      vm_name: draft.vm_name,
      vm_moref: draft.vm_moref,
      zoneid: draft.zoneid,
      clusterid: draft.clusterid,
      networkid: "",
      serviceofferingid: draft.serviceofferingid,
      boot_storageid: draft.boot_storageid,
      ostypeid: draft.ostypeid || "",
      boottype: draft.boottype || "",
      bootmode: draft.boottype === "UEFI" ? draft.bootmode || "" : "",
      rootdiskcontroller: draft.rootdiskcontroller || "",
      nicadapter: draft.nicadapter || "",
      disks,
      nic_mappings: nicMappings,
      migration,
    };
  }, []);

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
    [buildSpecPayload, cloudstackHeaders, pushToast, refreshJobs, selectedVmNames, vmSpecsByName, vmwareHeaders]
  );

  const finalizeVm = useCallback(
    async (vmName, immediate = false) => {
      try {
        const suffix = immediate ? "?now=true" : "";
        const response = await apiRequest(`/migration/finalize/${encodeURIComponent(vmName)}${suffix}`, { method: "POST" });
        const alreadyRequested = Boolean(response?.already_requested);
        const alreadyImmediate = Boolean(response?.already_immediate);
        pushToast(
          "success",
          immediate
            ? (alreadyImmediate
                ? `${vmName}: FINALIZE NOW already requested.`
                : `${vmName}: FINALIZE NOW requested successfully.`)
            : (alreadyRequested
                ? `${vmName}: FINALIZE already requested.`
                : `${vmName}: FINALIZE requested successfully.`)
        );
        await pollStatuses();
        await refreshJobs();
      } catch (err) {
        pushToast("error", err.message || "Failed to finalize migration.");
      }
    },
    [pollStatuses, pushToast, refreshJobs]
  );

  const selectedVmStatus = selectedJob ? statusByJob[selectedJob.job_id] || null : null;
  const selectedSettingsRows = useMemo(
    () =>
      selectedVmNames.map((vmName) => {
        const draft = vmSpecsByName[vmName];
        const nics = draft?.nics || [];
        const disks = draft?.disks || [];
        const dataDisks = disks.filter((disk) => disk.diskType !== "os");
        const resolveName = (items, id) => {
          if (!id) return "-";
          const item = items.find((entry) => entry.id === id);
          return item ? optionLabel(item) : id;
        };
        const dataDiskDetails = dataDisks.map((disk) => ({
          unit: disk.unit,
          label: disk.label || `Disk ${disk.unit}`,
          storageName: resolveName(storagePools, disk.storageid),
        }));
        const nicDetails = nics.map((nic, index) => ({
          id: nic.id || String(index),
          label: nic.source_label || `NIC ${index + 1}`,
          networkName: resolveName(networks, nic.networkid || ""),
        }));
        return {
          vmName,
          zoneName: resolveName(zones, draft?.zoneid || ""),
          clusterName: resolveName(clusters, draft?.clusterid || ""),
          serviceOfferingName: resolveName(serviceOfferings, draft?.serviceofferingid || ""),
          bootStorageName: resolveName(storagePools, draft?.boot_storageid || ""),
          osTypeName: resolveName(osTypes, draft?.ostypeid || ""),
          bootType: draft?.boottype || "",
          bootMode: draft?.boottype === "UEFI" ? draft?.bootmode || "" : "",
          rootDiskController: draft?.rootdiskcontroller || "",
          nicAdapter: draft?.nicadapter || "",
          startVmAfterImport: Boolean(draft?.migration?.start_vm_after_import),
          delta_interval: draft?.migration?.delta_interval || "",
          finalize_at: draft?.migration?.finalize_at || "",
          dataDiskDetails,
          nicDetails,
        };
      }),
    [clusters, networks, osTypes, selectedVmNames, serviceOfferings, storagePools, vmSpecsByName, zones]
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
                  <button className="secondary" onClick={loadInventory} disabled={inventoryBusy}>
                    {inventoryBusy ? "Loading..." : "Reload Inventory"}
                  </button>
                </div>
                <div className="form-grid">
                  <label>VM MoRef<input value={activeDraft.vm_moref} onChange={(e) => updateField("vm_moref", e.target.value)} placeholder="vm-123" /></label>
                  <label>Zone<select value={activeDraft.zoneid} onChange={(e) => updateField("zoneid", e.target.value)}><option value="">Select zone</option>{zones.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Cluster<select value={activeDraft.clusterid} onChange={(e) => updateField("clusterid", e.target.value)}><option value="">Select cluster</option>{clustersForActiveZone.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Service Offering<select value={activeDraft.serviceofferingid} onChange={(e) => updateField("serviceofferingid", e.target.value)}><option value="">Select service offering</option>{serviceOfferingsForActiveZone.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Boot Storage<select value={activeDraft.boot_storageid} onChange={(e) => updateField("boot_storageid", e.target.value)}><option value="">Select boot storage</option>{storagePoolsForActiveZone.map((item) => <option key={item.id} value={item.id}>{optionLabel(item)}</option>)}</select></label>
                  <label>Guest OS Mapping<select value={activeDraft.ostypeid} onChange={(e) => updateField("ostypeid", e.target.value)}><option value="">Default / leave unchanged</option>{osTypes.map((item) => <option key={item.id} value={item.id}>{item.description || item.name || item.id}</option>)}</select></label>
                  <label>Firmware / Boot Type<select value={activeDraft.boottype} onChange={(e) => updateField("boottype", e.target.value)}>{BOOT_TYPE_OPTIONS.map((item) => <option key={item.value || "default"} value={item.value}>{item.label}</option>)}</select></label>
                  <label>UEFI Boot Mode<select value={activeDraft.bootmode} onChange={(e) => updateField("bootmode", e.target.value)} disabled={activeDraft.boottype !== "UEFI"}>{UEFI_BOOT_MODE_OPTIONS.map((item) => <option key={item.value || "default"} value={item.value}>{item.label}</option>)}</select></label>
                  <label>Root Disk Controller<select value={activeDraft.rootdiskcontroller} onChange={(e) => updateField("rootdiskcontroller", e.target.value)}>{ROOT_DISK_CONTROLLER_OPTIONS.map((item) => <option key={item.value || "default"} value={item.value}>{item.label}</option>)}</select></label>
                  <label>NIC Adapter<select value={activeDraft.nicadapter} onChange={(e) => updateField("nicadapter", e.target.value)}>{NIC_ADAPTER_OPTIONS.map((item) => <option key={item.value || "default"} value={item.value}>{item.label}</option>)}</select></label>
                  <label>Delta Interval (sec)<input type="number" min="1" value={activeDraft.migration.delta_interval} onChange={(e) => updateMigrationField("delta_interval", e.target.value)} /></label>
                  <label>Finalize At (ISO)<input value={activeDraft.migration.finalize_at} onChange={(e) => updateMigrationField("finalize_at", e.target.value)} placeholder="2026-03-12T23:30:00+00:00" /></label>
                  <label>Finalize Delta Interval<input type="number" min="1" value={activeDraft.migration.finalize_delta_interval} onChange={(e) => updateMigrationField("finalize_delta_interval", e.target.value)} /></label>
                  <label>Finalize Window<input type="number" min="1" value={activeDraft.migration.finalize_window} onChange={(e) => updateMigrationField("finalize_window", e.target.value)} /></label>
                  <label>Shutdown Mode<input value={activeDraft.migration.shutdown_mode} onChange={(e) => updateMigrationField("shutdown_mode", e.target.value)} placeholder="auto" /></label>
                  <label>Snapshot Quiesce<input value={activeDraft.migration.snapshot_quiesce} onChange={(e) => updateMigrationField("snapshot_quiesce", e.target.value)} placeholder="auto" /></label>
                  <label className="checkbox-field"><input type="checkbox" checked={Boolean(activeDraft.migration.start_vm_after_import)} onChange={(e) => updateMigrationField("start_vm_after_import", e.target.checked)} />Start imported VM after CloudStack import</label>
                </div>
              </section>

              <DiskTable
                disks={activeDraft.disks || []}
                storagePools={storagePoolsForActiveZone}
                diskOfferings={diskOfferingsForActiveZone}
                onDiskChange={updateDisk}
                validationByUnit={activeValidation.diskErrors}
              />

              <NicTable
                nics={activeDraft.nics || []}
                networks={networksForActiveZone}
                onNicChange={updateNic}
                validationByNic={activeValidation.nicErrors}
              />

              <section className="panel">
                <h3>Selected VM Settings Summary</h3>
                <p className="hint">Review these values before generating spec or starting migration.</p>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th>VM</th>
                        <th>Zone</th>
                        <th>Cluster</th>
                        <th>Service Offering</th>
                        <th>Boot Storage</th>
                        <th>Imported VM Settings</th>
                        <th>Delta (sec)</th>
                        <th>Finalize At</th>
                        <th>Data Disk Mapping</th>
                        <th>NIC Mapping</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedSettingsRows.length === 0 ? (
                        <tr>
                          <td colSpan={10}>No VM selected.</td>
                        </tr>
                      ) : (
                        selectedSettingsRows.map((row) => (
                          <tr key={row.vmName} className={row.vmName === activeVmName ? "selected-row" : ""}>
                            <td>{row.vmName}</td>
                            <td>{row.zoneName || "-"}</td>
                            <td>{row.clusterName || "-"}</td>
                            <td>{row.serviceOfferingName || "-"}</td>
                            <td>{row.bootStorageName || "-"}</td>
                            <td>
                              <div className="hint small">Guest OS -> {row.osTypeName || "Default / unchanged"}</div>
                              <div className="hint small">Boot Type -> {row.bootType || "Default"}</div>
                              {row.bootType === "UEFI" ? <div className="hint small">UEFI Mode -> {row.bootMode || "Default"}</div> : null}
                              <div className="hint small">Root Disk Controller -> {row.rootDiskController || "Default"}</div>
                              <div className="hint small">NIC Adapter -> {row.nicAdapter || "Default"}</div>
                              <div className="hint small">Start VM -> {row.startVmAfterImport ? "Yes" : "No"}</div>
                            </td>
                            <td>{row.delta_interval || "-"}</td>
                            <td>{row.finalize_at || "-"}</td>
                            <td>
                              {row.dataDiskDetails.length ? (
                                row.dataDiskDetails.map((disk) => (
                                  <div key={`${row.vmName}-disk-${disk.unit}`} className="hint small">{disk.label} -> {disk.storageName}</div>
                                ))
                              ) : (
                                <span className="hint small">No data disks</span>
                              )}
                            </td>
                            <td>
                              {row.nicDetails.length ? (
                                row.nicDetails.map((nic) => (
                                  <div key={`${row.vmName}-nic-${nic.id}`} className="hint small">{nic.label} -> {nic.networkName}</div>
                                ))
                              ) : (
                                <span className="hint small">No NICs</span>
                              )}
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </section>

              <section className="panel">
                <div className="actions">
                  <button disabled={busy || selectedVmNames.length === 0} onClick={() => createSpec(false)}>Generate Spec ({selectedVmNames.length})</button>
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
          statusByJob={statusByJob}
          showJobHistory={showJobHistory}
          onToggleShowJobHistory={setShowJobHistory}
          selectedJobId={selectedJob?.job_id || ""}
          onSelectJob={(job) => {
            setSelectedJob(job);
            loadLogsForJob(job);
          }}
          onFinalize={finalizeVm}
          onFinalizeNow={(vmName) => finalizeVm(vmName, true)}
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
