export default function VMSelector({
  vmOptions,
  vmName,
  vmMoref,
  onVmChange,
  onMorefChange,
  onDetectDisks,
  loading,
}) {
  return (
    <section className="panel">
      <div className="subsection-title-row">
        <h2>VM Selection</h2>
        <button className="secondary" onClick={onDetectDisks} disabled={loading || !vmName}>
          {loading ? "Detecting..." : "Refresh VM Disks"}
        </button>
      </div>

      <div className="form-grid compact">
        <label>
          Source VM
          <select value={vmName} onChange={(e) => onVmChange(e.target.value)}>
            <option value="">Select VM</option>
            {vmOptions.map((vm) => (
              <option key={`${vm.name}-${vm.moref}`} value={vm.name}>
                {vm.name} ({vm.moref})
              </option>
            ))}
          </select>
        </label>

        <label>
          VM MoRef
          <input value={vmMoref} onChange={(e) => onMorefChange(e.target.value)} placeholder="vm-123" />
        </label>
      </div>

      {loading ? <p className="hint">Fetching VM disks from vCenter...</p> : null}
    </section>
  );
}
