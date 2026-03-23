package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/view"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
	"gopkg.in/yaml.v3"
)

type apiOptions struct {
	ConfigPath   string
	ListenAddr   string
	AllowOrigins string
	EngineBin    string
	ControlDir   string
	SpecsDir     string
	WorkDir      string
	MaxWorkers   int
}

type apiJob struct {
	JobID      string     `json:"job_id"`
	VMName     string     `json:"vm_name"`
	SpecFile   string     `json:"spec_file"`
	Status     string     `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	ReturnCode *int       `json:"return_code,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type apiServer struct {
	cfg         *appConfig
	configPath  string
	engineBin   string
	workDir     string
	controlDir  string
	specsDir    string
	allowOrigin string
	sem         chan struct{}

	mu         sync.Mutex
	jobSeq     uint64
	jobs       map[string]*apiJob
	jobsByVM   map[string][]string
	httpServer *http.Server
}

type vmwareDiskInfo struct {
	Label     string  `json:"label"`
	SizeGB    float64 `json:"size_gb"`
	Datastore string  `json:"datastore,omitempty"`
	Unit      *int    `json:"unit,omitempty"`
	DiskType  string  `json:"disk_type,omitempty"`
}

type vmwareVMInfo struct {
	Name      string           `json:"name"`
	Moref     string           `json:"moref"`
	CPU       int              `json:"cpu"`
	Memory    int              `json:"memory"`
	Disks     []vmwareDiskInfo `json:"disks"`
	Datastore []string         `json:"datastore"`
}

type diskSpecInput struct {
	StorageID      string `json:"storageid"`
	DiskOfferingID string `json:"diskofferingid"`
}

type migrationOptionsInput struct {
	DeltaInterval         int    `json:"delta_interval"`
	FinalizeAt            string `json:"finalize_at"`
	FinalizeDeltaInterval int    `json:"finalize_delta_interval"`
	FinalizeWindow        int    `json:"finalize_window"`
	ShutdownMode          string `json:"shutdown_mode"`
	SnapshotQuiesce       string `json:"snapshot_quiesce"`
}

type migrationSpecRequest struct {
	VMName            string                   `json:"vm_name"`
	VMMoref           string                   `json:"vm_moref"`
	ZoneID            string                   `json:"zoneid"`
	ClusterID         string                   `json:"clusterid"`
	NetworkID         string                   `json:"networkid"`
	ServiceOfferingID string                   `json:"serviceofferingid"`
	BootStorageID     string                   `json:"boot_storageid"`
	Disks             map[string]diskSpecInput `json:"disks"`
	Migration         migrationOptionsInput    `json:"migration"`
}

type migrationStartRequest struct {
	VMName   string `json:"vm_name"`
	SpecFile string `json:"spec_file"`
}

type vcenterRuntime struct {
	Host      string
	User      string
	Password  string
	Port      int
	VerifySSL bool
}

func cmdServe(args []string) error {
	var opts apiOptions
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.StringVar(&opts.ConfigPath, "config", defaultConfigPath(), "Global config.yaml path")
	fs.StringVar(&opts.ListenAddr, "listen", ":8000", "HTTP listen address")
	fs.StringVar(&opts.AllowOrigins, "allow-origins", os.Getenv("MIGRATOR_CORS_ORIGINS"), "CORS allowed origins (default '*')")
	fs.StringVar(&opts.EngineBin, "engine-bin", "", "Path to v2c-engine executable used for migration run jobs")
	fs.StringVar(&opts.ControlDir, "control-dir", "", "Control directory root (default config migration.control_dir or /var/lib/vm-migrator)")
	fs.StringVar(&opts.SpecsDir, "specs-dir", "", "Specs directory (default config migration.specs_dir or <control-dir>/specs)")
	fs.StringVar(&opts.WorkDir, "workdir", "", "Working directory for migration subprocess")
	fs.IntVar(&opts.MaxWorkers, "max-workers", 0, "Parallel VM migration subprocess limit")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(opts.ConfigPath) == "" {
		opts.ConfigPath = defaultConfigPath()
	}
	cfg, err := loadAppConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.VCenter.Password) == "" {
		cfg.VCenter.Password = os.Getenv("VC_PASSWORD")
	}

	engineBin := strings.TrimSpace(opts.EngineBin)
	if engineBin == "" {
		if exe, err := os.Executable(); err == nil {
			engineBin = exe
		}
	}
	if engineBin == "" {
		return errors.New("unable to resolve engine binary path")
	}

	controlDir := strings.TrimSpace(opts.ControlDir)
	if controlDir == "" {
		controlDir = strings.TrimSpace(cfg.Migration.ControlDir)
	}
	if controlDir == "" {
		controlDir = "/var/lib/vm-migrator"
	}

	specsDir := strings.TrimSpace(opts.SpecsDir)
	if specsDir == "" {
		specsDir = strings.TrimSpace(cfg.Migration.SpecsDir)
	}
	if specsDir == "" {
		specsDir = filepath.Join(controlDir, "specs")
	}

	workDir := strings.TrimSpace(opts.WorkDir)
	if workDir == "" {
		workDir = strings.TrimSpace(cfg.Migration.Workdir)
	}
	if workDir == "" {
		workDir = filepath.Dir(engineBin)
	}

	maxWorkers := opts.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = cfg.Migration.ParallelVMs
	}
	if maxWorkers <= 0 {
		maxWorkers = 3
	}

	allowOrigins := strings.TrimSpace(opts.AllowOrigins)
	if allowOrigins == "" {
		allowOrigins = "*"
	}

	s := &apiServer{
		cfg:         cfg,
		configPath:  opts.ConfigPath,
		engineBin:   engineBin,
		workDir:     workDir,
		controlDir:  controlDir,
		specsDir:    specsDir,
		allowOrigin: allowOrigins,
		sem:         make(chan struct{}, maxWorkers),
		jobs:        map[string]*apiJob{},
		jobsByVM:    map[string][]string{},
	}
	return s.serve(opts.ListenAddr)
}

func (s *apiServer) serve(listenAddr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/vmware/vms", s.handleVMwareVMs)
	mux.HandleFunc("/cloudstack/zones", s.handleCloudStackZones)
	mux.HandleFunc("/cloudstack/clusters", s.handleCloudStackClusters)
	mux.HandleFunc("/cloudstack/storage", s.handleCloudStackStorage)
	mux.HandleFunc("/cloudstack/networks", s.handleCloudStackNetworks)
	mux.HandleFunc("/cloudstack/diskofferings", s.handleCloudStackDiskOfferings)
	mux.HandleFunc("/cloudstack/serviceofferings", s.handleCloudStackServiceOfferings)
	mux.HandleFunc("/migration/spec", s.handleMigrationSpec)
	mux.HandleFunc("/migration/start", s.handleMigrationStart)
	mux.HandleFunc("/migration/jobs", s.handleMigrationJobs)
	mux.HandleFunc("/migration/status/", s.handleMigrationStatus)
	mux.HandleFunc("/migration/finalize/", s.handleMigrationFinalize)
	mux.HandleFunc("/migration/logs/", s.handleMigrationLogs)

	s.httpServer = &http.Server{
		Addr:    listenAddr,
		Handler: s.withCORS(mux),
	}

	fmt.Fprintf(os.Stderr, "[serve] API listening on %s (control_dir=%s)\n", listenAddr, s.controlDir)
	return s.httpServer.ListenAndServe()
}

func (s *apiServer) withCORS(next http.Handler) http.Handler {
	allowed := map[string]struct{}{}
	for _, part := range strings.Split(s.allowOrigin, ",") {
		p := strings.TrimSpace(part)
		if p != "" {
			allowed[p] = struct{}{}
		}
	}
	allowAny := len(allowed) == 0
	if _, ok := allowed["*"]; ok {
		allowAny = true
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		switch {
		case allowAny:
			w.Header().Set("Access-Control-Allow-Origin", "*")
		case origin != "":
			if _, ok := allowed[origin]; ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization,X-VCenter-Host,X-VCenter-User,X-VCenter-Password,X-VCenter-Port,X-VCenter-Verify-SSL,X-CloudStack-Endpoint,X-CloudStack-API-Key,X-CloudStack-Secret-Key,X-CloudStack-Timeout-Seconds")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *apiServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *apiServer) handleVMwareVMs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vc, err := s.vcenterFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	client, err := connectVCenterRuntime(ctx, vc)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("failed to connect VMware: %v", err))
		return
	}
	vms, err := listVMwareInventory(ctx, client)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("failed to list VMware VMs: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, vms)
}

func (s *apiServer) handleCloudStackZones(w http.ResponseWriter, r *http.Request) {
	s.handleCloudStackList(w, r, "listZones", "zone")
}

func (s *apiServer) handleCloudStackClusters(w http.ResponseWriter, r *http.Request) {
	s.handleCloudStackList(w, r, "listClusters", "cluster")
}

func (s *apiServer) handleCloudStackStorage(w http.ResponseWriter, r *http.Request) {
	s.handleCloudStackList(w, r, "listStoragePools", "storagepool")
}

func (s *apiServer) handleCloudStackNetworks(w http.ResponseWriter, r *http.Request) {
	s.handleCloudStackList(w, r, "listNetworks", "network")
}

func (s *apiServer) handleCloudStackDiskOfferings(w http.ResponseWriter, r *http.Request) {
	s.handleCloudStackList(w, r, "listDiskOfferings", "diskoffering")
}

func (s *apiServer) handleCloudStackServiceOfferings(w http.ResponseWriter, r *http.Request) {
	s.handleCloudStackList(w, r, "listServiceOfferings", "serviceoffering")
}

func (s *apiServer) handleCloudStackList(w http.ResponseWriter, r *http.Request, command string, key string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	cs, err := s.cloudStackClientFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	res, err := cs.request(command, nil)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("CloudStack API request failed: %v", err))
		return
	}
	root, ok := mapGetMap(res, strings.ToLower(command)+"response")
	if !ok {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	raw := root[key]
	if raw == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	items, ok := raw.([]any)
	if !ok {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *apiServer) handleMigrationSpec(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req migrationSpecRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.VMName) == "" {
		writeError(w, http.StatusBadRequest, "vm_name is required")
		return
	}
	if strings.TrimSpace(req.ZoneID) == "" || strings.TrimSpace(req.ClusterID) == "" ||
		strings.TrimSpace(req.NetworkID) == "" || strings.TrimSpace(req.ServiceOfferingID) == "" ||
		strings.TrimSpace(req.BootStorageID) == "" {
		writeError(w, http.StatusBadRequest, "zoneid, clusterid, networkid, serviceofferingid and boot_storageid are required")
		return
	}

	controlDirName, err := s.resolveControlDirName(r, req)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			writeError(w, http.StatusNotFound, fmt.Sprintf("VM '%s' not found in VMware.", req.VMName))
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	vmDir := filepath.Join(s.controlDir, controlDirName)
	if err := os.MkdirAll(vmDir, 0o755); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create control dir: %v", err))
		return
	}

	migrationBlock := map[string]any{
		"delta_interval": maxInt(req.Migration.DeltaInterval, 300),
	}
	if strings.TrimSpace(req.Migration.FinalizeAt) != "" {
		migrationBlock["finalize_at"] = req.Migration.FinalizeAt
	}
	if req.Migration.FinalizeDeltaInterval > 0 {
		migrationBlock["finalize_delta_interval"] = req.Migration.FinalizeDeltaInterval
	}
	if req.Migration.FinalizeWindow > 0 {
		migrationBlock["finalize_window"] = req.Migration.FinalizeWindow
	}
	if strings.TrimSpace(req.Migration.ShutdownMode) != "" {
		migrationBlock["shutdown_mode"] = req.Migration.ShutdownMode
	}
	if strings.TrimSpace(req.Migration.SnapshotQuiesce) != "" {
		migrationBlock["snapshot_quiesce"] = req.Migration.SnapshotQuiesce
	}

	spec := map[string]any{
		"vm": map[string]any{
			"name": req.VMName,
		},
		"migration": migrationBlock,
		"target": map[string]any{
			"cloudstack": map[string]any{
				"zoneid":            req.ZoneID,
				"clusterid":         req.ClusterID,
				"networkid":         req.NetworkID,
				"serviceofferingid": req.ServiceOfferingID,
				"storageid":         req.BootStorageID,
			},
		},
	}

	disks := map[string]map[string]string{}
	for unit, d := range req.Disks {
		disks[unit] = map[string]string{
			"storageid":      strings.TrimSpace(d.StorageID),
			"diskofferingid": strings.TrimSpace(d.DiskOfferingID),
		}
	}
	spec["disks"] = disks

	raw, err := yaml.Marshal(spec)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal spec: %v", err))
		return
	}
	specPath := filepath.Join(vmDir, "spec.yaml")
	if err := os.WriteFile(specPath, raw, 0o644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to write spec: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":    req.VMName,
		"spec_file":  specPath,
		"created_at": time.Now().UTC(),
	})
}

func (s *apiServer) handleMigrationStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req migrationStartRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.VMName) == "" {
		writeError(w, http.StatusBadRequest, "vm_name is required")
		return
	}

	specPath, err := s.resolveSpecFile(req.VMName, req.SpecFile)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	job := s.startJob(req.VMName, specPath)
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":   job.VMName,
		"job_id":    job.JobID,
		"spec_file": job.SpecFile,
		"status":    job.Status,
	})
}

func (s *apiServer) handleMigrationStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vmName := strings.TrimPrefix(r.URL.Path, "/migration/status/")
	vmName, _ = neturl.PathUnescape(vmName)
	vmName = strings.TrimSpace(vmName)
	if vmName == "" {
		writeError(w, http.StatusBadRequest, "vm name is required")
		return
	}
	st := s.loadState(vmName)
	job := s.latestJobForVM(vmName)
	if st == nil && job == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No migration state found for VM '%s'.", vmName))
		return
	}
	writeJSON(w, http.StatusOK, s.buildStatusPayload(vmName, st, job))
}

func (s *apiServer) handleMigrationJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vmName := strings.TrimSpace(r.URL.Query().Get("vm"))
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n > 1000 {
				n = 1000
			}
			limit = n
		}
	}

	s.mu.Lock()
	jobs := make([]*apiJob, 0, len(s.jobs))
	for _, j := range s.jobs {
		if vmName != "" && j.VMName != vmName {
			continue
		}
		copy := *j
		jobs = append(jobs, &copy)
	}
	s.mu.Unlock()

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})
	if limit < len(jobs) {
		jobs = jobs[:limit]
	}

	out := make([]map[string]any, 0, len(jobs))
	for _, job := range jobs {
		st := s.loadState(job.VMName)
		status := s.buildStatusPayload(job.VMName, st, job)
		out = append(out, map[string]any{
			"job_id":      job.JobID,
			"vm_name":     job.VMName,
			"status":      job.Status,
			"spec_file":   job.SpecFile,
			"started_at":  job.StartedAt,
			"finished_at": job.FinishedAt,
			"return_code": job.ReturnCode,
			"error":       emptyToNil(job.Error),
			"stage":       status["stage"],
			"progress":    status["overall_progress"],
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *apiServer) handleMigrationFinalize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vmName := strings.TrimPrefix(r.URL.Path, "/migration/finalize/")
	vmName, _ = neturl.PathUnescape(vmName)
	vmName = strings.TrimSpace(vmName)
	if vmName == "" {
		writeError(w, http.StatusBadRequest, "vm name is required")
		return
	}

	targetDir := ""
	if job := s.latestJobForVM(vmName); job != nil {
		targetDir = s.jobRuntimeDir(job.VMName, job.SpecFile)
	}
	if targetDir == "" {
		dirs := s.candidateVMDirs(vmName)
		if len(dirs) > 0 {
			targetDir = dirs[0]
		}
	}
	if targetDir == "" {
		writeError(w, http.StatusNotFound, fmt.Sprintf("Control directory not found for VM '%s'.", vmName))
		return
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	finalizePath := filepath.Join(targetDir, "FINALIZE")
	if f, err := os.OpenFile(finalizePath, os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		_ = f.Close()
	} else {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":       vmName,
		"finalize_file": finalizePath,
		"message":       "Finalize marker created",
	})
}

func (s *apiServer) handleMigrationLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vmName := strings.TrimPrefix(r.URL.Path, "/migration/logs/")
	vmName, _ = neturl.PathUnescape(vmName)
	vmName = strings.TrimSpace(vmName)
	if vmName == "" {
		writeError(w, http.StatusBadRequest, "vm name is required")
		return
	}

	lines := 200
	if raw := strings.TrimSpace(r.URL.Query().Get("lines")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			if n < 10 {
				n = 10
			}
			if n > 2000 {
				n = 2000
			}
			lines = n
		}
	}
	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	logs := s.getLogs(vmName, lines, jobID)
	writeJSON(w, http.StatusOK, logs)
}

func (s *apiServer) startJob(vmName string, specFile string) *apiJob {
	id := fmt.Sprintf("%d-%06d", time.Now().UnixNano(), atomic.AddUint64(&s.jobSeq, 1))
	job := &apiJob{
		JobID:     id,
		VMName:    vmName,
		SpecFile:  specFile,
		Status:    "queued",
		StartedAt: time.Now().UTC(),
	}

	s.mu.Lock()
	s.jobs[id] = job
	s.jobsByVM[vmName] = append(s.jobsByVM[vmName], id)
	s.mu.Unlock()

	go s.runJob(job)
	return job
}

func (s *apiServer) runJob(job *apiJob) {
	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	s.mu.Lock()
	if current, ok := s.jobs[job.JobID]; ok {
		current.Status = "running"
	}
	s.mu.Unlock()

	vmDir := s.jobRuntimeDir(job.VMName, job.SpecFile)
	_ = os.MkdirAll(vmDir, 0o755)
	stdoutPath := filepath.Join(vmDir, job.JobID+".stdout.log")
	stderrPath := filepath.Join(vmDir, job.JobID+".stderr.log")

	command := []string{
		s.engineBin,
		"run",
		"--config", s.configPath,
		"--spec", job.SpecFile,
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Dir = s.workDir

	stdoutFile, err := os.OpenFile(stdoutPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		s.finishJob(job.JobID, -1, fmt.Sprintf("failed to open stdout log: %v", err))
		return
	}
	defer stdoutFile.Close()
	stderrFile, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		s.finishJob(job.JobID, -1, fmt.Sprintf("failed to open stderr log: %v", err))
		return
	}
	defer stderrFile.Close()

	_, _ = fmt.Fprintf(stdoutFile, "$ %s\n", strings.Join(command, " "))
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile
	runErr := cmd.Run()
	exitCode := 0
	if runErr != nil {
		exitCode = 1
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			exitCode = ee.ExitCode()
		}
	}

	errText := ""
	if exitCode != 0 {
		if b, readErr := os.ReadFile(stderrPath); readErr == nil {
			txt := strings.TrimSpace(string(b))
			if len(txt) > 1000 {
				txt = txt[len(txt)-1000:]
			}
			errText = txt
		}
		if errText == "" && runErr != nil {
			errText = runErr.Error()
		}
	}
	s.finishJob(job.JobID, exitCode, errText)
}

func (s *apiServer) finishJob(jobID string, exitCode int, errText string) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return
	}
	job.FinishedAt = &now
	job.ReturnCode = &exitCode
	if exitCode == 0 {
		job.Status = "completed"
		job.Error = ""
		return
	}
	job.Status = "failed"
	job.Error = errText
}

func (s *apiServer) latestJobForVM(vmName string) *apiJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.jobsByVM[vmName]
	if len(ids) == 0 {
		return nil
	}
	job := s.jobs[ids[len(ids)-1]]
	if job == nil {
		return nil
	}
	copy := *job
	return &copy
}

func (s *apiServer) vcenterFromRequest(r *http.Request) (vcenterRuntime, error) {
	host := strings.TrimSpace(r.Header.Get("x-vcenter-host"))
	user := strings.TrimSpace(r.Header.Get("x-vcenter-user"))
	pass := strings.TrimSpace(r.Header.Get("x-vcenter-password"))
	portHeader := strings.TrimSpace(r.Header.Get("x-vcenter-port"))
	verifyHeader := strings.TrimSpace(r.Header.Get("x-vcenter-verify-ssl"))

	port := 443
	verifySSL := false
	if portHeader != "" {
		p, err := strconv.Atoi(portHeader)
		if err != nil {
			return vcenterRuntime{}, errors.New("invalid x-vcenter-port header")
		}
		port = p
	}
	if verifyHeader != "" {
		verifySSL = parseBool(verifyHeader, false)
	}

	if host == "" {
		host = strings.TrimSpace(s.cfg.VCenter.Host)
		user = firstNonEmpty(user, strings.TrimSpace(s.cfg.VCenter.User))
		pass = firstNonEmpty(pass, strings.TrimSpace(s.cfg.VCenter.Password), strings.TrimSpace(os.Getenv("VC_PASSWORD")))
	} else {
		parsedHost, parsedPort := parseHostAndPort(host)
		host = parsedHost
		if parsedPort > 0 && portHeader == "" {
			port = parsedPort
		}
		if user == "" || pass == "" {
			return vcenterRuntime{}, errors.New("selected vCenter environment is missing host, username, or password")
		}
	}

	if host == "" || user == "" || pass == "" {
		return vcenterRuntime{}, errors.New("VMware credentials are missing. Configure vcenter in config.yaml or pass x-vcenter-* headers")
	}

	return vcenterRuntime{
		Host:      host,
		User:      user,
		Password:  pass,
		Port:      port,
		VerifySSL: verifySSL,
	}, nil
}

func connectVCenterRuntime(ctx context.Context, cfg vcenterRuntime) (*govmomi.Client, error) {
	host := cfg.Host
	if cfg.Port > 0 && !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, cfg.Port)
	}
	u, err := neturl.Parse("https://" + host + "/sdk")
	if err != nil {
		return nil, err
	}
	u.User = neturl.UserPassword(cfg.User, cfg.Password)
	insecure := !cfg.VerifySSL
	return govmomi.NewClient(ctx, u, insecure)
}

func listVMwareInventory(ctx context.Context, client *govmomi.Client) ([]vmwareVMInfo, error) {
	vmgr := view.NewManager(client.Client)
	cv, err := vmgr.CreateContainerView(ctx, client.ServiceContent.RootFolder, []string{"VirtualMachine"}, true)
	if err != nil {
		return nil, err
	}
	defer cv.Destroy(ctx)

	var vms []mo.VirtualMachine
	pc := property.DefaultCollector(client.Client)
	if err := pc.Retrieve(ctx, cv.Reference(), []string{
		"name",
		"config.hardware.numCPU",
		"config.hardware.memoryMB",
		"config.hardware.device",
		"config.bootOptions",
	}, &vms); err != nil {
		return nil, err
	}

	out := make([]vmwareVMInfo, 0, len(vms))
	for _, vm := range vms {
		if vm.Config == nil || vm.Config.Hardware.Device == nil {
			continue
		}
		bootUnit := detectBootDiskUnit(vm.Config)

		disks := make([]vmwareDiskInfo, 0)
		dsSet := map[string]struct{}{}
		for _, dev := range vm.Config.Hardware.Device {
			vd, ok := dev.(*types.VirtualDisk)
			if !ok {
				continue
			}
			unit := int(vd.UnitNumber)
			capacity := int64(vd.CapacityInBytes)
			if capacity <= 0 && vd.CapacityInKB > 0 {
				capacity = int64(vd.CapacityInKB) * 1024
			}
			sizeGB := math.Round((float64(capacity)/1024.0/1024.0/1024.0)*100) / 100

			dsName := datastoreFromBacking(vd.Backing)
			if dsName != "" {
				dsSet[dsName] = struct{}{}
			}
			diskType := "data"
			if bootUnit != nil && unit == *bootUnit {
				diskType = "os"
			}
			label := "Virtual Disk"
			if vd.DeviceInfo != nil {
				label = vd.DeviceInfo.GetDescription().Label
			}
			u := unit
			disks = append(disks, vmwareDiskInfo{
				Label:     label,
				SizeGB:    sizeGB,
				Datastore: dsName,
				Unit:      &u,
				DiskType:  diskType,
			})
		}
		sort.Slice(disks, func(i, j int) bool {
			li, lj := 1<<30, 1<<30
			if disks[i].Unit != nil {
				li = *disks[i].Unit
			}
			if disks[j].Unit != nil {
				lj = *disks[j].Unit
			}
			return li < lj
		})
		dsList := make([]string, 0, len(dsSet))
		for name := range dsSet {
			dsList = append(dsList, name)
		}
		sort.Strings(dsList)

		cpu := 0
		mem := 0
		if vm.Config.Hardware.NumCPU > 0 {
			cpu = int(vm.Config.Hardware.NumCPU)
		}
		if vm.Config.Hardware.MemoryMB > 0 {
			mem = int(vm.Config.Hardware.MemoryMB)
		}
		out = append(out, vmwareVMInfo{
			Name:      vm.Name,
			Moref:     vm.Reference().Value,
			CPU:       cpu,
			Memory:    mem,
			Disks:     disks,
			Datastore: dsList,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (s *apiServer) cloudStackClientFromRequest(r *http.Request) (*cloudStackClient, error) {
	endpoint := strings.TrimSpace(r.Header.Get("x-cloudstack-endpoint"))
	apiKey := strings.TrimSpace(r.Header.Get("x-cloudstack-api-key"))
	secret := strings.TrimSpace(r.Header.Get("x-cloudstack-secret-key"))
	timeout := 30 * time.Second
	if raw := strings.TrimSpace(r.Header.Get("x-cloudstack-timeout-seconds")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, errors.New("invalid x-cloudstack-timeout-seconds header")
		}
		if n > 0 {
			timeout = time.Duration(n) * time.Second
		}
	}

	if endpoint == "" {
		endpoint = strings.TrimSpace(s.cfg.CloudStack.Endpoint)
		apiKey = firstNonEmpty(apiKey, strings.TrimSpace(s.cfg.CloudStack.APIKey))
		secret = firstNonEmpty(secret, strings.TrimSpace(s.cfg.CloudStack.SecretKey))
	} else {
		if apiKey == "" || secret == "" {
			return nil, errors.New("selected CloudStack environment is missing API key or secret key")
		}
	}
	if endpoint == "" || apiKey == "" || secret == "" {
		return nil, errors.New("CloudStack config is missing. Configure cloudstack in config.yaml or pass x-cloudstack-* headers")
	}

	return &cloudStackClient{
		Endpoint:  normalizeCloudStackEndpoint(endpoint),
		APIKey:    apiKey,
		SecretKey: secret,
		HTTP: &http.Client{
			Timeout: timeout,
		},
	}, nil
}

func normalizeCloudStackEndpoint(endpoint string) string {
	value := strings.TrimSpace(strings.TrimRight(endpoint, "/"))
	lower := strings.ToLower(value)
	switch {
	case strings.HasSuffix(lower, "/client/api"):
		return value
	case strings.HasSuffix(lower, "/client"):
		return value + "/api"
	case strings.HasSuffix(lower, "/api"):
		return value
	default:
		return value + "/client/api"
	}
}

func detectBootDiskUnit(cfg *types.VirtualMachineConfigInfo) *int {
	if cfg == nil || cfg.Hardware.Device == nil {
		return nil
	}
	keyToUnit := map[int32]int{}
	minUnit := -1
	for _, dev := range cfg.Hardware.Device {
		vd, ok := dev.(*types.VirtualDisk)
		if !ok {
			continue
		}
		unit := int(vd.UnitNumber)
		keyToUnit[vd.Key] = unit
		if minUnit < 0 || unit < minUnit {
			minUnit = unit
		}
	}
	if cfg.BootOptions != nil {
		for _, b := range cfg.BootOptions.BootOrder {
			d, ok := b.(*types.VirtualMachineBootOptionsBootableDiskDevice)
			if !ok {
				continue
			}
			if unit, ok := keyToUnit[d.DeviceKey]; ok {
				u := unit
				return &u
			}
		}
	}
	if minUnit >= 0 {
		u := minUnit
		return &u
	}
	return nil
}

func datastoreFromBacking(backing types.BaseVirtualDeviceBackingInfo) string {
	if backing == nil {
		return ""
	}
	switch b := backing.(type) {
	case *types.VirtualDiskFlatVer2BackingInfo:
		return datastoreFromFileName(b.FileName)
	case *types.VirtualDiskSparseVer2BackingInfo:
		return datastoreFromFileName(b.FileName)
	case *types.VirtualDiskSeSparseBackingInfo:
		return datastoreFromFileName(b.FileName)
	case *types.VirtualDiskRawDiskMappingVer1BackingInfo:
		return datastoreFromFileName(b.FileName)
	default:
		return ""
	}
}

func datastoreFromFileName(file string) string {
	file = strings.TrimSpace(file)
	if !strings.HasPrefix(file, "[") {
		return ""
	}
	end := strings.Index(file, "]")
	if end <= 1 {
		return ""
	}
	return strings.TrimSpace(file[1:end])
}

func (s *apiServer) resolveControlDirName(r *http.Request, req migrationSpecRequest) (string, error) {
	safeName := safeVMName(req.VMName)
	if strings.TrimSpace(req.VMMoref) != "" {
		return fmt.Sprintf("%s_%s", safeName, strings.TrimSpace(req.VMMoref)), nil
	}
	vc, err := s.vcenterFromRequest(r)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	client, err := connectVCenterRuntime(ctx, vc)
	if err != nil {
		return "", err
	}
	vms, err := listVMwareInventory(ctx, client)
	if err != nil {
		return "", err
	}
	matches := make([]vmwareVMInfo, 0)
	for _, vm := range vms {
		if vm.Name == req.VMName {
			matches = append(matches, vm)
		}
	}
	if len(matches) == 0 {
		return "", os.ErrNotExist
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("multiple VMware VMs named '%s' found. Please pass vm_moref in /migration/spec request", req.VMName)
	}
	return fmt.Sprintf("%s_%s", safeName, matches[0].Moref), nil
}

func (s *apiServer) candidateVMDirs(vmName string) []string {
	base := filepath.Clean(s.controlDir)
	safe := safeVMName(vmName)
	seen := map[string]struct{}{}
	out := make([]string, 0)
	add := func(path string) {
		if path == "" {
			return
		}
		st, err := os.Stat(path)
		if err != nil || !st.IsDir() {
			return
		}
		clean := filepath.Clean(path)
		if _, ok := seen[clean]; ok {
			return
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}

	if matches, _ := filepath.Glob(filepath.Join(base, safe+"_*")); len(matches) > 0 {
		for _, m := range matches {
			add(m)
		}
	}
	if vmName != safe {
		if matches, _ := filepath.Glob(filepath.Join(base, vmName+"_*")); len(matches) > 0 {
			for _, m := range matches {
				add(m)
			}
		}
	}
	add(filepath.Join(base, safe))
	if vmName != safe {
		add(filepath.Join(base, vmName))
	}

	sort.Slice(out, func(i, j int) bool {
		ai, aerr := os.Stat(out[i])
		aj, jerr := os.Stat(out[j])
		if aerr != nil || jerr != nil {
			return out[i] < out[j]
		}
		return ai.ModTime().After(aj.ModTime())
	})
	return out
}

func (s *apiServer) latestSpecForVM(vmName string) (string, error) {
	for _, dir := range s.candidateVMDirs(vmName) {
		candidates := []string{
			filepath.Join(dir, "spec.yaml"),
			filepath.Join(dir, "spec.latest.yaml"),
		}
		for _, c := range candidates {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				return c, nil
			}
		}

		specsDir := filepath.Join(dir, "specs")
		if entries, err := os.ReadDir(specsDir); err == nil {
			var files []string
			for _, e := range entries {
				if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".yaml") {
					continue
				}
				files = append(files, filepath.Join(specsDir, e.Name()))
			}
			sortByMtimeDesc(files)
			if len(files) > 0 {
				return files[0], nil
			}
		}
	}

	if entries, err := os.ReadDir(s.specsDir); err == nil {
		var legacy []string
		safe := safeVMName(vmName)
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(strings.ToLower(e.Name()), ".yaml") {
				continue
			}
			name := e.Name()
			if strings.HasPrefix(name, safe+"-") || name == safe+".yaml" {
				legacy = append(legacy, filepath.Join(s.specsDir, name))
			}
		}
		sortByMtimeDesc(legacy)
		if len(legacy) > 0 {
			return legacy[0], nil
		}
	}
	return "", fmt.Errorf("No spec file found for VM '%s'.", vmName)
}

func (s *apiServer) resolveSpecFile(vmName string, specFile string) (string, error) {
	specFile = strings.TrimSpace(specFile)
	if specFile == "" {
		return s.latestSpecForVM(vmName)
	}
	if st, err := os.Stat(specFile); err == nil && !st.IsDir() {
		return specFile, nil
	}
	specInSpecs := filepath.Join(s.specsDir, specFile)
	if st, err := os.Stat(specInSpecs); err == nil && !st.IsDir() {
		return specInSpecs, nil
	}
	for _, vmDir := range s.candidateVMDirs(vmName) {
		candidates := []string{
			filepath.Join(vmDir, specFile),
			filepath.Join(vmDir, "specs", specFile),
		}
		for _, c := range candidates {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				return c, nil
			}
		}
	}
	return "", fmt.Errorf("Spec file not found: %s", specFile)
}

func (s *apiServer) jobRuntimeDir(vmName string, specFile string) string {
	if rel, err := filepath.Rel(s.controlDir, specFile); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) > 0 && parts[0] != "." && parts[0] != "" {
			return filepath.Join(s.controlDir, parts[0])
		}
	}
	dirs := s.candidateVMDirs(vmName)
	if len(dirs) > 0 {
		return dirs[0]
	}
	return filepath.Join(s.controlDir, safeVMName(vmName))
}

func (s *apiServer) statePathForVM(vmName string) string {
	for _, dir := range s.candidateVMDirs(vmName) {
		p := filepath.Join(dir, "state.json")
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

func (s *apiServer) loadState(vmName string) *runState {
	statePath := s.statePathForVM(vmName)
	if statePath == "" {
		return nil
	}
	st, err := loadRunState(statePath)
	if err != nil {
		return nil
	}
	return st
}

func (s *apiServer) buildStatusPayload(vmName string, st *runState, job *apiJob) map[string]any {
	stage := ""
	progress := float64(0)
	transfer := float64(0)
	disksRaw := map[string]*runDiskState{}

	if st != nil {
		stage = st.Stage
		progress = st.Progress
		transfer = st.TransferSpeedMB
		if st.Disks != nil {
			disksRaw = st.Disks
		}
	}

	diskProgress := make([]map[string]any, 0, len(disksRaw))
	totalBytes := int64(0)
	copiedBytes := int64(0)
	speedSum := float64(0)
	speedCount := 0

	keys := make([]string, 0, len(disksRaw))
	for k := range disksRaw {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		li, ei := strconv.Atoi(keys[i])
		lj, ej := strconv.Atoi(keys[j])
		if ei == nil && ej == nil {
			return li < lj
		}
		return keys[i] < keys[j]
	})

	for _, k := range keys {
		ds := disksRaw[k]
		if ds == nil {
			continue
		}
		total := ds.Capacity
		if total < 0 {
			total = 0
		}
		copied := ds.CopiedBytes
		if copied <= 0 {
			copied = ds.BytesRead
		}
		if copied < 0 {
			copied = 0
		}
		if copied > total && total > 0 {
			copied = total
		}
		remaining := total - copied
		if remaining < 0 {
			remaining = 0
		}
		pct := ds.Progress
		if pct <= 0 && total > 0 {
			pct = math.Round((float64(copied)*10000)/float64(total)) / 100
		}

		totalBytes += total
		copiedBytes += copied
		if ds.SpeedMBps > 0 {
			speedSum += ds.SpeedMBps
			speedCount++
		}

		diskProgress = append(diskProgress, map[string]any{
			"unit":             k,
			"disk_name":        fmt.Sprintf("Disk %d", ds.Unit),
			"provisioned_size": formatBytes(total),
			"provisioned_bytes": total,
			"used_size":        formatBytes(copied),
			"used_bytes":       copied,
			"total_size":       formatBytes(total),
			"copied_size":      formatBytes(copied),
			"remaining_size":   formatBytes(remaining),
			"total_bytes":      total,
			"copied_bytes":     copied,
			"remaining_bytes":  remaining,
			"speed_mbps":       ds.SpeedMBps,
			"eta_seconds":      ds.EtaSeconds,
			"progress":         pct,
		})
	}

	overall := progress
	if overall <= 0 && totalBytes > 0 {
		overall = math.Round((float64(copiedBytes)*10000)/float64(totalBytes)) / 100
	}
	if overall <= 0 && stage == stageDone {
		overall = 100
	}
	if transfer <= 0 && speedCount > 0 {
		transfer = math.Round((speedSum/float64(speedCount))*100) / 100
	}

	payload := map[string]any{
		"vm_name":             vmName,
		"stage":               emptyToNil(stage),
		"progress":            overall,
		"overall_progress":    overall,
		"transfer_speed_mbps": transfer,
		"disks":               disksRaw,
		"disk_progress":       diskProgress,
		"updated_at":          time.Now().UTC(),
	}
	if job != nil {
		payload["job_id"] = job.JobID
		payload["job_status"] = job.Status
		payload["job_error"] = emptyToNil(job.Error)
		payload["return_code"] = job.ReturnCode
	}
	return payload
}

func (s *apiServer) getLogs(vmName string, lines int, jobID string) map[string]any {
	job := s.latestJobForVM(vmName)
	targetDir := ""
	resolvedJobID := strings.TrimSpace(jobID)

	if resolvedJobID != "" {
		s.mu.Lock()
		explicit := s.jobs[resolvedJobID]
		s.mu.Unlock()
		if explicit != nil && explicit.VMName == vmName {
			targetDir = s.jobRuntimeDir(explicit.VMName, explicit.SpecFile)
		}
	}
	if targetDir == "" && job != nil {
		targetDir = s.jobRuntimeDir(job.VMName, job.SpecFile)
		if resolvedJobID == "" {
			resolvedJobID = job.JobID
		}
	}
	if targetDir == "" {
		dirs := s.candidateVMDirs(vmName)
		if len(dirs) > 0 {
			targetDir = dirs[0]
		} else {
			targetDir = filepath.Join(s.controlDir, safeVMName(vmName))
		}
	}

	stdoutPath := ""
	stderrPath := ""
	if resolvedJobID != "" {
		out := filepath.Join(targetDir, resolvedJobID+".stdout.log")
		err := filepath.Join(targetDir, resolvedJobID+".stderr.log")
		if _, e := os.Stat(out); e == nil {
			stdoutPath = out
		}
		if _, e := os.Stat(err); e == nil {
			stderrPath = err
		}
	}
	if stdoutPath == "" {
		if files, _ := filepath.Glob(filepath.Join(targetDir, "*.stdout.log")); len(files) > 0 {
			sortByMtimeDesc(files)
			stdoutPath = files[0]
		}
	}
	if stderrPath == "" {
		if files, _ := filepath.Glob(filepath.Join(targetDir, "*.stderr.log")); len(files) > 0 {
			sortByMtimeDesc(files)
			stderrPath = files[0]
		}
	}

	return map[string]any{
		"vm_name":     vmName,
		"job_id":      emptyToNil(resolvedJobID),
		"stdout_path": emptyToNil(stdoutPath),
		"stderr_path": emptyToNil(stderrPath),
		"stdout":      tailFile(stdoutPath, lines),
		"stderr":      tailFile(stderrPath, lines),
	}
}

func decodeJSONBody(r *http.Request, out any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 2<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"detail": message})
}

func tailFile(path string, lines int) string {
	if path == "" || lines <= 0 {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	text := strings.ReplaceAll(string(b), "\r\n", "\n")
	parts := strings.Split(text, "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Trim(strings.Join(parts, "\n"), "\n")
}

func sortByMtimeDesc(paths []string) {
	sort.Slice(paths, func(i, j int) bool {
		ai, aerr := os.Stat(paths[i])
		aj, jerr := os.Stat(paths[j])
		if aerr != nil || jerr != nil {
			return paths[i] < paths[j]
		}
		return ai.ModTime().After(aj.ModTime())
	})
}

func parseHostAndPort(raw string) (string, int) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", 0
	}
	if strings.Contains(value, "://") {
		if u, err := neturl.Parse(value); err == nil {
			host := u.Hostname()
			if host != "" {
				p, _ := strconv.Atoi(u.Port())
				return host, p
			}
		}
	}
	if strings.Count(value, ":") == 1 {
		left, right, _ := strings.Cut(value, ":")
		if p, err := strconv.Atoi(right); err == nil {
			return left, p
		}
	}
	return value, 0
}

func parseBool(raw string, defaultVal bool) bool {
	if strings.TrimSpace(raw) == "" {
		return defaultVal
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultVal
	}
}

func safeVMName(vm string) string {
	vm = strings.TrimSpace(vm)
	if vm == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(vm))
	for i := 0; i < len(vm); i++ {
		ch := vm[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-' {
			b.WriteByte(ch)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func formatBytes(v int64) string {
	if v < 0 {
		v = 0
	}
	value := float64(v)
	units := []string{"B", "KB", "MB", "GB", "TB"}
	idx := 0
	for value >= 1024 && idx < len(units)-1 {
		value /= 1024
		idx++
	}
	if idx == 0 {
		return fmt.Sprintf("%d %s", int64(value), units[idx])
	}
	return fmt.Sprintf("%.1f %s", value, units[idx])
}

func emptyToNil(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func maxInt(v int, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
