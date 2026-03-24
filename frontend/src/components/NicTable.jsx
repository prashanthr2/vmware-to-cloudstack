function optionLabel(item) {
  return item.name || item.displaytext || item.id || "Unknown";
}

export default function NicTable({ nics, networks, fallbackNetworkId, onNicChange, validationByNic }) {
  return (
    <section className="panel">
      <h2>NIC Mapping</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Adapter</th>
              <th>Source Network</th>
              <th>MAC</th>
              <th>Target CloudStack Network</th>
            </tr>
          </thead>
          <tbody>
            {nics.length === 0 ? (
              <tr>
                <td colSpan={4}>No NICs detected for this VM.</td>
              </tr>
            ) : (
              nics.map((nic) => {
                const error = validationByNic[nic.id] || "";
                const usedByOtherNICs = new Set(
                  nics
                    .filter((item) => item.id !== nic.id)
                    .map((item) => (item.networkid || "").trim())
                    .filter((id) => id !== "")
                );
                return (
                  <tr key={nic.id}>
                    <td>
                      <strong>{nic.source_label}</strong>
                      <div className="hint">index {nic.source_index}</div>
                    </td>
                    <td>{nic.source_network || "-"}</td>
                    <td><code>{nic.source_mac || "-"}</code></td>
                    <td>
                      <select value={nic.networkid || ""} onChange={(e) => onNicChange(nic.id, "networkid", e.target.value)}>
                        <option value="">Select network</option>
                        {fallbackNetworkId ? (
                          <option
                            value={fallbackNetworkId}
                            disabled={usedByOtherNICs.has(fallbackNetworkId) && (nic.networkid || "") !== fallbackNetworkId}
                          >
                            Use fallback network
                          </option>
                        ) : null}
                        {networks.map((item) => (
                          <option
                            key={item.id}
                            value={item.id}
                            disabled={usedByOtherNICs.has(item.id) && (nic.networkid || "") !== item.id}
                          >
                            {optionLabel(item)}
                          </option>
                        ))}
                      </select>
                      {error ? <div className="field-error">{error}</div> : null}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}
