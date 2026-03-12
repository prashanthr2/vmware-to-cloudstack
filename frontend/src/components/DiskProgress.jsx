function formatEta(seconds) {
  if (!seconds || seconds <= 0) {
    return "-";
  }

  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

export default function DiskProgress({ status }) {
  const diskProgress = status?.disk_progress || [];

  if (!diskProgress.length) {
    return <p className="hint">No disk-level progress data available yet.</p>;
  }

  const activeDisk = [...diskProgress]
    .filter((item) => item.progress !== null && item.progress < 100)
    .sort((a, b) => (b.speed_mbps || 0) - (a.speed_mbps || 0))[0];

  return (
    <div>
      {activeDisk ? (
        <div className="copying-banner">
          <strong>Copying {activeDisk.disk_name}</strong>
          <span>
            Transferred: {activeDisk.copied_size || "-"} / {activeDisk.used_size || "-"}
          </span>
          <span>
            Speed: {activeDisk.speed_mbps ? `${activeDisk.speed_mbps} MB/s` : "-"} | ETA: {formatEta(activeDisk.eta_seconds)}
          </span>
          <span>
            Provisioned: {activeDisk.provisioned_size || activeDisk.total_size || "-"}
          </span>
        </div>
      ) : null}

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Disk</th>
              <th>Provisioned Size</th>
              <th>Used Size (Estimated)</th>
              <th>Copied</th>
              <th>Remaining</th>
              <th>Speed</th>
              <th>ETA</th>
              <th>Disk Read Progress</th>
            </tr>
          </thead>
          <tbody>
            {diskProgress.map((item) => (
              <tr key={item.unit}>
                <td>
                  {item.disk_name}
                  <div className="hint small">{item.disk_type === "os" ? "OS" : "Data"}</div>
                </td>
                <td>{item.provisioned_size || item.total_size || "-"}</td>
                <td>{item.used_size || "-"}</td>
                <td>{item.copied_size || "-"}</td>
                <td>{item.remaining_size || "-"}</td>
                <td>{item.speed_mbps ? `${item.speed_mbps} MB/s` : "-"}</td>
                <td>{formatEta(item.eta_seconds)}</td>
                <td>{item.progress !== null && item.progress !== undefined ? `${item.progress}%` : "-"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
