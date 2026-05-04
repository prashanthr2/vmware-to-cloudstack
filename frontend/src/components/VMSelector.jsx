import { useEffect, useMemo, useState } from "react";

const VM_PAGE_SIZE_KEY = "vm_migrator_vm_page_size_v1";
const PAGE_SIZE_OPTIONS = [25, 50, 100, 200];

function initialPageSize() {
  try {
    const raw = Number(localStorage.getItem(VM_PAGE_SIZE_KEY));
    return PAGE_SIZE_OPTIONS.includes(raw) ? raw : 50;
  } catch {
    return 50;
  }
}

export default function VMSelector({
  vmOptions,
  selectedVmNames,
  activeVmName,
  showTemplates,
  showVcls,
  onToggleVm,
  onSetActiveVm,
  onToggleShowTemplates,
  onToggleShowVcls,
  onSelectAll,
  onClearSelection,
  onRefreshSelected,
  loading,
}) {
  const [pageSize, setPageSize] = useState(initialPageSize);
  const [page, setPage] = useState(1);
  const totalPages = Math.max(1, Math.ceil(vmOptions.length / pageSize));
  const safePage = Math.min(page, totalPages);
  const startIndex = vmOptions.length === 0 ? 0 : (safePage - 1) * pageSize;
  const endIndex = Math.min(startIndex + pageSize, vmOptions.length);
  const pageVMs = useMemo(() => vmOptions.slice(startIndex, endIndex), [endIndex, startIndex, vmOptions]);

  useEffect(() => {
    if (safePage !== page) {
      setPage(safePage);
    }
  }, [page, safePage]);

  useEffect(() => {
    try {
      localStorage.setItem(VM_PAGE_SIZE_KEY, String(pageSize));
    } catch {
      // Browser storage is best-effort only.
    }
  }, [pageSize]);

  const changePageSize = (value) => {
    const next = Number(value);
    setPageSize(PAGE_SIZE_OPTIONS.includes(next) ? next : 50);
    setPage(1);
  };

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>VM Selection</h2>
        <div className="actions compact">
          <label className="checkbox-inline">
            <input type="checkbox" checked={showTemplates} onChange={(e) => onToggleShowTemplates(e.target.checked)} />
            Show Templates
          </label>
          <label className="checkbox-inline">
            <input type="checkbox" checked={showVcls} onChange={(e) => onToggleShowVcls(e.target.checked)} />
            Show vCLS VMs
          </label>
          <button className="secondary" onClick={onSelectAll} disabled={!vmOptions.length || loading}>
            Select All
          </button>
          <button className="secondary" onClick={onClearSelection} disabled={!selectedVmNames.length || loading}>
            Clear
          </button>
          <button className="secondary" onClick={onRefreshSelected} disabled={loading || !selectedVmNames.length}>
            {loading ? "Refreshing..." : "Refresh Selected VMs"}
          </button>
        </div>
      </div>
      <p className="hint">
        Selected: <strong>{selectedVmNames.length}</strong> | Inventory: <strong>{vmOptions.length}</strong>
      </p>

      <div className="vm-pagination-bar">
        <span className="hint">
          Showing <strong>{vmOptions.length === 0 ? 0 : startIndex + 1}</strong>-<strong>{endIndex}</strong> of{" "}
          <strong>{vmOptions.length}</strong>
        </span>
        <label className="pagination-size-field">
          VMs per page
          <select value={pageSize} onChange={(e) => changePageSize(e.target.value)} disabled={loading}>
            {PAGE_SIZE_OPTIONS.map((size) => (
              <option key={size} value={size}>
                {size}
              </option>
            ))}
          </select>
        </label>
        <div className="pagination-actions">
          <button className="secondary" onClick={() => setPage(1)} disabled={loading || safePage <= 1}>
            First
          </button>
          <button className="secondary" onClick={() => setPage((prev) => Math.max(1, prev - 1))} disabled={loading || safePage <= 1}>
            Previous
          </button>
          <span className="hint">
            Page <strong>{safePage}</strong> / <strong>{totalPages}</strong>
          </span>
          <button className="secondary" onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))} disabled={loading || safePage >= totalPages}>
            Next
          </button>
          <button className="secondary" onClick={() => setPage(totalPages)} disabled={loading || safePage >= totalPages}>
            Last
          </button>
        </div>
      </div>

      <div className="vm-select-grid">
        {pageVMs.map((vm) => (
          <label key={`${vm.name}-${vm.moref}`} className="vm-select-card">
            <input
              type="checkbox"
              checked={selectedVmNames.includes(vm.name)}
              onChange={(e) => onToggleVm(vm.name, e.target.checked)}
            />
            <span>{vm.name}</span>
            <code>{vm.moref}</code>
          </label>
        ))}
      </div>

      <div className="form-grid compact">
        <label>
          Active VM for Editing
          <select value={activeVmName} onChange={(e) => onSetActiveVm(e.target.value)} disabled={!selectedVmNames.length}>
            <option value="">Select active VM</option>
            {selectedVmNames.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
        </label>
      </div>
    </section>
  );
}
