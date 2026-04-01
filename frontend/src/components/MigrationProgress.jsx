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
  statusByJob,
  showJobHistory,
  onToggleShowJobHistory,
  selectedJobId,
  onSelectJob,
  onFinalize,
  onFinalizeNow,
  onRetry,
  logsSection,
}) {
  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Migration Progress</h2>
        <label className="checkbox-field">
          <input
            type="checkbox"
            checked={Boolean(showJobHistory)}
            onChange={(e) => onToggleShowJobHistory(e.target.checked)}
          />
          Show History
        </label>
      </div>

      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>VM</th>
              <th>Job</th>
              <th>Job Status</th>
              <th>Stage</th>
              <th>Next Stage</th>
              <th>Finalize</th>
              <th>Overall Progress</th>
              <th>Speed</th>
              <th>Started</th>
              <th>Ended</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {jobs.length === 0 ? (
              <tr>
                <td colSpan={11}>No jobs yet.</td>
              </tr>
            ) : (
              jobs.map((job) => {
                const status = statusByJob[job.job_id] || {};
                const progress = normalizeProgress(
                  status.overall_progress !== undefined && status.overall_progress !== null
                    ? status.overall_progress
                    : job.progress
                );
                const jobStatus = status.job_status || job.status || "-";
                const statusClass = typeof jobStatus === "string" ? jobStatus.toLowerCase() : "";
                const endedAt =
                  job.finished_at ||
                  ((jobStatus === "completed" || jobStatus === "failed") ? status.updated_at : null);
                const finalizeRequested = Boolean(status.finalize_requested);
                const finalizeNowRequested = Boolean(status.finalize_now_requested);
                const stage = status.stage || job.stage || "-";
                const nextStage = status.next_stage || "-";
                const statusLower = typeof jobStatus === "string" ? jobStatus.toLowerCase() : "";
                const finalizeButtonDisabled =
                  finalizeRequested || stage === "done" || stage === "final_sync";
                const finalizeNowButtonDisabled =
                  finalizeNowRequested || stage === "done" || stage === "final_sync";
                const retryDisabled = statusLower !== "failed";

                return (
                  <tr key={job.job_id} className={selectedJobId === job.job_id ? "selected-row" : ""}>
                    <td>{job.vm_name}</td>
                    <td><code>{job.job_id.slice(0, 8)}</code></td>
                    <td><span className={`pill ${statusClass}`}>{jobStatus}</span></td>
                    <td>{stage}</td>
                    <td>{nextStage}</td>
                    <td>
                      <span className={`pill ${finalizeRequested ? "completed" : "queued"}`}>
                        {finalizeRequested ? "Requested" : "Not Requested"}
                      </span>
                    </td>
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
                    <td>{formatDate(endedAt)}</td>
                    <td className="row-actions">
                      <button className="secondary" onClick={() => onSelectJob(job)}>
                        Details
                      </button>
                      <button
                        className="secondary"
                        onClick={() => onRetry(job)}
                        disabled={retryDisabled}
                        title={retryDisabled ? "Retry is available only for failed jobs" : "Retry failed migration"}
                      >
                        Retry
                      </button>
                      <button
                        className="secondary"
                        onClick={() => onFinalize(job.vm_name)}
                        disabled={finalizeButtonDisabled}
                        title={finalizeRequested ? "Finalize already requested" : "Request finalize"}
                      >
                        {finalizeRequested ? "Finalize Requested" : "Finalize"}
                      </button>
                      <button
                        className="secondary"
                        onClick={() => onFinalizeNow(job.vm_name)}
                        disabled={finalizeNowButtonDisabled}
                        title={finalizeNowRequested ? "Finalize-now already requested" : "Request finalize now"}
                      >
                        {finalizeNowRequested ? "Finalize Now Requested" : "Finalize Now"}
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
          <DiskProgress status={statusByJob[selectedJobId] || null} />
          {logsSection}
        </div>
      ) : null}
    </section>
  );
}
