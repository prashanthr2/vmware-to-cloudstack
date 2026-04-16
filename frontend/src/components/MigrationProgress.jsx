import DiskProgress from "./DiskProgress";

function normalizeProgress(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return 0;
  }
  return Math.max(0, Math.min(100, Number(value)));
}

const STAGE_LABELS = {
  connecting_vcenter: "Connecting to vCenter",
  finding_vm: "Finding VM",
  init: "Initializing",
  discovering_vmware_disks: "Discovering VMware disks",
  preparing_target_storage: "Preparing target storage",
  enabling_cbt: "Enabling CBT",
  creating_base_snapshot: "Creating base snapshot",
  base_copy: "Base copy",
  delta: "Delta sync",
  awaiting_shutdown_action: "Awaiting shutdown action",
  final_sync: "Final sync",
  converting: "Converting",
  import_root_disk: "Importing root disk",
  import_data_disk: "Importing data disks",
  done: "Done",
};

function formatStageLabel(value) {
  if (!value) {
    return "-";
  }
  return STAGE_LABELS[value] || value;
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
  onShutdownForce,
  onShutdownManual,
  onRetry,
  onRetryDebug,
  logsSection,
}) {
  const pendingJobs = jobs
    .map((job) => {
      const status = statusByJob[job.job_id] || {};
      return {
        job,
        status,
        awaitingUserAction: Boolean(status.awaiting_user_action),
      };
    })
    .filter((entry) => entry.awaitingUserAction);

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

      {pendingJobs.length > 0 ? (
        <div className="pending-actions-panel">
          <div className="pending-actions-header">
            <div>
              <h3>Pending Actions</h3>
              <p>
                {pendingJobs.length === 1
                  ? "One VM is waiting for operator input before migration can continue."
                  : `${pendingJobs.length} VMs are waiting for operator input before migration can continue.`}
              </p>
            </div>
            <span className="pill attention">Action Required</span>
          </div>
          <div className="pending-actions-list">
            {pendingJobs.map(({ job, status }) => (
              <div key={`pending-${job.job_id}`} className="pending-action-card">
                <div className="pending-action-copy">
                  <strong>{job.vm_name}</strong>
                  <span>{status.shutdown_reason || "User confirmation is required to continue finalize."}</span>
                </div>
                <div className="pending-action-buttons">
                  <button
                    className="secondary"
                    onClick={() => onSelectJob(job)}
                    title="Open this migration in the details pane"
                  >
                    View Details
                  </button>
                  <button
                    className="secondary"
                    onClick={() => onShutdownForce(job.vm_name)}
                    title="Request forced power off so migration can continue"
                  >
                    Force Power Off
                  </button>
                  <button
                    className="secondary"
                    onClick={() => onShutdownManual(job.vm_name)}
                    title="Confirm the VM has been shut down manually"
                  >
                    Manual Shutdown Done
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

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
                const awaitingUserAction = Boolean(status.awaiting_user_action);
                const rawStage = status.stage || job.stage || "-";
                const stage = formatStageLabel(rawStage);
                const nextStage = awaitingUserAction ? "Operator action required" : formatStageLabel(status.next_stage || "-");
                const statusLower = typeof jobStatus === "string" ? jobStatus.toLowerCase() : "";
                const finalizeButtonDisabled =
                  finalizeRequested || rawStage === "done" || rawStage === "final_sync" || awaitingUserAction;
                const finalizeNowButtonDisabled =
                  finalizeNowRequested || rawStage === "done" || rawStage === "final_sync" || awaitingUserAction;
                const retryDisabled = statusLower !== "failed";

                return (
                  <tr
                    key={job.job_id}
                    className={[
                      selectedJobId === job.job_id ? "selected-row" : "",
                      awaitingUserAction ? "pending-action-row" : "",
                    ].filter(Boolean).join(" ")}
                  >
                    <td>{job.vm_name}</td>
                    <td><code>{job.job_id.slice(0, 8)}</code></td>
                    <td>
                      <div className="status-stack">
                        <span className={`pill ${statusClass}`}>{jobStatus}</span>
                        {awaitingUserAction ? <span className="pill attention">Action Required</span> : null}
                      </div>
                    </td>
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
                        onClick={() => onRetryDebug(job)}
                        disabled={retryDisabled}
                        title={retryDisabled ? "Debug retry is available only for failed jobs" : "Retry failed migration with virt-v2v debug logging"}
                      >
                        Retry with Debug
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
                      {awaitingUserAction ? (
                        <>
                          <button
                            className="secondary"
                            onClick={() => onShutdownForce(job.vm_name)}
                            title="Request forced power off so migration can continue"
                          >
                            Force Power Off
                          </button>
                          <button
                            className="secondary"
                            onClick={() => onShutdownManual(job.vm_name)}
                            title="Confirm the VM has been shut down manually"
                          >
                            Manual Shutdown Done
                          </button>
                        </>
                      ) : null}
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
