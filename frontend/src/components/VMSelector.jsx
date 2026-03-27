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
  return (
    <section className="panel">
      <div className="subsection-title-row">
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

      <div className="vm-select-grid">
        {vmOptions.map((vm) => (
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
