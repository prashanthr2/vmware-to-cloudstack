import DiskProgress from "./DiskProgress";

function normalizeProgress(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return 0;
  }
  return Math.max(0, Math.min(100, Number(value)));
}

function formatDate(value) {
  if (!value) {
    return "-";
  }
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

export default function MigrationProgress({
  jobs,
  statusByVm,
  selectedJobId,
  onSelectJob,
  onFinalize,
  logsSection,
}) {
  return (
    <section className="panel">
      <h2>Migration Progress</h2>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>VM</th>
              <th>Job</th>
              <th>Status</th>
              <th>Stage</th>
              <th>Overall Progress</th>
              <th>Speed</th>
              <th>Started</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {jobs.length === 0 ? (
              <tr>
                <td colSpan={8}>No jobs yet.</td>
              </tr>
            ) : (
              jobs.map((job) => {
                const status = statusByVm[job.vm_name] || {};
                const progress = normalizeProgress(
                  status.overall_progress !== undefined && status.overall_progress !== null
                    ? status.overall_progress
                    : job.progress
                );

                return (
                  <tr key={job.job_id} className={selectedJobId === job.job_id ? "selected-row" : ""}>
                    <td>{job.vm_name}</td>
                    <td><code>{job.job_id.slice(0, 8)}</code></td>
                    <td><span className={`pill ${job.status}`}>{job.status}</span></td>
                    <td>{status.stage || job.stage || "-"}</td>
                    <td>
                      <div className="progress-cell">
                        <div className="progress-track">
                          <div className="progress-fill" style={{ width: `${progress}%` }} />
                        </div>
                        <span>{progress.toFixed(1)}%</span>
                      </div>
                    </td>
                    <td>{status.transfer_speed_mbps ? `${status.transfer_speed_mbps} MB/s` : "-"}</td>
                    <td>{formatDate(job.started_at)}</td>
                    <td className="row-actions">
                      <button className="secondary" onClick={() => onSelectJob(job)}>
                        Details
                      </button>
                      <button className="secondary" onClick={() => onFinalize(job.vm_name)}>
                        Finalize
                      </button>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {selectedJobId ? (
        <div className="subsection">
          <h3>Disk-Level Progress</h3>
          <DiskProgress status={jobs.length ? statusByVm[jobs.find((j) => j.job_id === selectedJobId)?.vm_name] : null} />
          {logsSection}
        </div>
      ) : null}
    </section>
  );
}
