function optionLabel(item) {
  return item.name || item.displaytext || item.id || "Unknown";
}

function storagePoolLabel(item) {
  const name = optionLabel(item);
  const type = String(item?.type || item?.pooltype || item?.storagetype || "").trim();
  const path = String(item?.path || item?.sourcepath || item?.mountpoint || item?.url || "").trim();
  const extras = [];
  if (type) extras.push(type);
  if (path) extras.push(path);
  if (extras.length === 0) return name;
  return `${name} (${extras.join(" | ")})`;
}

export default function DiskTable({
  disks,
  storagePools,
  diskOfferings,
  onDiskChange,
  validationByUnit,
}) {
  return (
    <section className="panel">
      <h2>Detected Disks</h2>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Disk Name</th>
              <th>Size</th>
              <th>Type</th>
              <th>Datastore</th>
              <th>Target Storage</th>
              <th>Disk Offering (CloudStack)</th>
            </tr>
          </thead>
          <tbody>
            {disks.length === 0 ? (
              <tr>
                <td colSpan={6}>No disks detected yet.</td>
              </tr>
            ) : (
              disks.map((disk) => {
                const error = validationByUnit[disk.unit] || "";
                const isBootDisk = disk.diskType === "os";

                return (
                  <tr key={disk.unit}>
                    <td>
                      <strong>{disk.label}</strong>
                      <div className="hint">unit {disk.unit}</div>
                    </td>
                    <td>{disk.sizeText}</td>
                    <td>
                      <span className={`pill ${isBootDisk ? "queued" : "running"}`}>
                        {isBootDisk ? "OS" : "Data"}
                      </span>
                    </td>
                    <td>{disk.datastore || "-"}</td>
                    <td>
                      {isBootDisk ? (
                        <div className="hint small">Inherited from Boot Storage selection</div>
                      ) : (
                        <select
                          value={disk.storageid}
                          onChange={(e) => onDiskChange(disk.unit, "storageid", e.target.value)}
                        >
                          <option value="">Select storage</option>
                          {storagePools.map((item) => (
                            <option key={item.id} value={item.id}>
                              {storagePoolLabel(item)}
                            </option>
                          ))}
                        </select>
                      )}
                    </td>
                    <td>
                      {isBootDisk ? (
                        <div className="hint small">Inherited from selected service offering</div>
                      ) : (
                        <>
                          <select
                            value={disk.diskofferingid}
                            onChange={(e) => onDiskChange(disk.unit, "diskofferingid", e.target.value)}
                          >
                            <option value="">Select disk offering</option>
                            {diskOfferings.map((item) => (
                              <option key={item.id} value={item.id}>
                                {optionLabel(item)}
                              </option>
                            ))}
                          </select>
                          <div className="hint small">Required for data disk</div>
                        </>
                      )}
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
