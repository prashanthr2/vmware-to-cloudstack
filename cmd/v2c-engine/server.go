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
	"github.com/vmware/govmomi/object"
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
	RuntimeDir string     `json:"runtime_dir,omitempty"`

	StageSnapshot               string    `json:"-"`
	NextStageSnapshot           string    `json:"-"`
	ProgressSnapshot            float64   `json:"-"`
	TransferSpeedSnapshot       float64   `json:"-"`
	FinalizeRequestedSnapshot   bool      `json:"-"`
	FinalizeNowRequestedSnapshot bool     `json:"-"`
	UpdatedAtSnapshot           time.Time `json:"-"`
	EnvOverrides                runEnvOverrides `json:"-"`
}

type runEnvOverrides struct {
	VCenterHost      string
	VCenterUser      string
	VCenterPassword  string
	CloudEndpoint    string
	CloudAPIKey      string
	CloudSecretKey   string
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

type vmwareNICInfo struct {
	Label      string `json:"label"`
	Network    string `json:"network"`
	MACAddress string `json:"mac_address,omitempty"`
	DeviceKey  int32  `json:"device_key,omitempty"`
	Index      int    `json:"index"`
}

type vmwareVMInfo struct {
	Name       string           `json:"name"`
	Moref      string           `json:"moref"`
	CPU        int              `json:"cpu"`
	Memory     int              `json:"memory"`
	Disks      []vmwareDiskInfo `json:"disks"`
	NICs       []vmwareNICInfo  `json:"nics"`
	Datastore  []string         `json:"datastore"`
	IsTemplate bool             `json:"is_template,omitempty"`
	IsVCLS     bool             `json:"is_vcls,omitempty"`
}

type vmwareInventoryFilter struct {
	IncludeTemplates bool
	IncludeVCLS      bool
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
	StartVMAfterImport    bool   `json:"start_vm_after_import"`
}

type nicMappingInput struct {
	SourceLabel     string `json:"source_label"`
	SourceNetwork   string `json:"source_network"`
	SourceMAC       string `json:"source_mac"`
	SourceDeviceKey int32  `json:"source_device_key"`
	SourceIndex     int    `json:"source_index"`
	NetworkID       string `json:"networkid"`
}

type migrationSpecRequest struct {
	VMName            string                     `json:"vm_name"`
	VMMoref           string                     `json:"vm_moref"`
	ZoneID            string                     `json:"zoneid"`
	ClusterID         string                     `json:"clusterid"`
	NetworkID         string                     `json:"networkid"`
	ServiceOfferingID string                     `json:"serviceofferingid"`
	BootStorageID     string                     `json:"boot_storageid"`
	OSTypeID          string                     `json:"ostypeid"`
	BootType          string                     `json:"boottype"`
	BootMode          string                     `json:"bootmode"`
	RootDiskController string                    `json:"rootdiskcontroller"`
	NICAdapter        string                     `json:"nicadapter"`
	Disks             map[string]diskSpecInput   `json:"disks"`
	NICMappings       map[string]nicMappingInput `json:"nic_mappings"`
	Migration         migrationOptionsInput      `json:"migration"`
}

type migrationSettingsFile struct {
	Version int                  `json:"version"`
	SavedAt time.Time            `json:"saved_at"`
	Payload migrationSpecRequest `json:"payload"`
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
	if st, err := os.Stat(workDir); err == nil && !st.IsDir() {
		fmt.Fprintf(os.Stderr, "[serve] warning: configured workdir %q is not a directory, falling back to %q\n", workDir, controlDir)
		workDir = controlDir
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		fallbackWorkDir := controlDir
		if fallbackWorkDir == "" {
			fallbackWorkDir = filepath.Dir(engineBin)
		}
		if fallbackWorkDir != workDir {
			if mkErr := os.MkdirAll(fallbackWorkDir, 0o755); mkErr == nil {
				fmt.Fprintf(
					os.Stderr,
					"[serve] warning: configured workdir %q is unavailable (%v), falling back to %q\n",
					workDir,
					err,
					fallbackWorkDir,
				)
				workDir = fallbackWorkDir
			} else {
				return fmt.Errorf("failed to create workdir %q (%v) and fallback %q (%v)", workDir, err, fallbackWorkDir, mkErr)
			}
		} else {
			return fmt.Errorf("failed to create workdir %q: %w", workDir, err)
		}
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
	mux.HandleFunc("/environments/defaults", s.handleEnvironmentDefaults)
	mux.HandleFunc("/vmware/vms", s.handleVMwareVMs)
	mux.HandleFunc("/cloudstack/zones", s.handleCloudStackZones)
	mux.HandleFunc("/cloudstack/clusters", s.handleCloudStackClusters)
	mux.HandleFunc("/cloudstack/storage", s.handleCloudStackStorage)
	mux.HandleFunc("/cloudstack/networks", s.handleCloudStackNetworks)
	mux.HandleFunc("/cloudstack/diskofferings", s.handleCloudStackDiskOfferings)
	mux.HandleFunc("/cloudstack/serviceofferings", s.handleCloudStackServiceOfferings)
	mux.HandleFunc("/cloudstack/ostypes", s.handleCloudStackOSTypes)
	mux.HandleFunc("/migration/settings", s.handleMigrationSettings)
	mux.HandleFunc("/migration/spec", s.handleMigrationSpec)
	mux.HandleFunc("/migration/start", s.handleMigrationStart)
	mux.HandleFunc("/migration/retry/", s.handleMigrationRetry)
	mux.HandleFunc("/migration/jobs", s.handleMigrationJobs)
	mux.HandleFunc("/migration/status/", s.handleMigrationStatus)
	mux.HandleFunc("/migration/finalize/", s.handleMigrationFinalize)
	mux.HandleFunc("/migration/shutdown/", s.handleMigrationShutdown)
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

func (s *apiServer) handleEnvironmentDefaults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	vcHost := strings.TrimSpace(s.cfg.VCenter.Host)
	vcUser := strings.TrimSpace(s.cfg.VCenter.User)
	vcPass := strings.TrimSpace(s.cfg.VCenter.Password)
	vcAvailable := vcHost != "" && vcUser != "" && vcPass != ""

	csEndpointRaw := strings.TrimSpace(s.cfg.CloudStack.Endpoint)
	csEndpoint := normalizeCloudStackEndpoint(csEndpointRaw)
	csName := cloudStackEndpointName(csEndpointRaw)
	if csName == "" {
		csName = cloudStackEndpointName(csEndpoint)
	}
	csAvailable := csEndpoint != "" &&
		strings.TrimSpace(s.cfg.CloudStack.APIKey) != "" &&
		strings.TrimSpace(s.cfg.CloudStack.SecretKey) != ""

	writeJSON(w, http.StatusOK, map[string]any{
		"vcenter": map[string]any{
			"id":        "config-default-vcenter",
			"name":      vcHost,
			"host":      vcHost,
			"username":  vcUser,
			"source":    "config",
			"available": vcAvailable,
		},
		"cloudstack": map[string]any{
			"id":        "config-default-cloudstack",
			"name":      csName,
			"apiUrl":    csEndpoint,
			"source":    "config",
			"available": csAvailable,
		},
	})
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
	query := r.URL.Query()
	includeTemplates := parseBool(strings.TrimSpace(query.Get("include_templates")), false)
	includeVCLS := parseBool(strings.TrimSpace(query.Get("include_vcls")), false)
	// Backward-compatible aliases for UI query params.
	if !includeTemplates {
		includeTemplates = parseBool(strings.TrimSpace(query.Get("show_templates")), false)
	}
	if !includeVCLS {
		includeVCLS = parseBool(strings.TrimSpace(query.Get("show_vcls")), false)
	}

	vms, err := listVMwareInventory(ctx, client, vmwareInventoryFilter{
		IncludeTemplates: includeTemplates,
		IncludeVCLS:      includeVCLS,
	})
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

func (s *apiServer) handleCloudStackOSTypes(w http.ResponseWriter, r *http.Request) {
	s.handleCloudStackList(w, r, "listOsTypes", "ostype")
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
	if command == "listStoragePools" {
		items = filterNFSUpStoragePools(items)
	}
	writeJSON(w, http.StatusOK, items)
}

func filterNFSUpStoragePools(items []any) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok || m == nil {
			continue
		}
		info := storagePoolInfoFromMap(m)
		if !isNFSStoragePool(info) {
			continue
		}
		state := strings.TrimSpace(firstNonEmptyString(mapGetString(m, "state"), mapGetString(m, "status")))
		if !strings.EqualFold(state, "Up") {
			continue
		}
		out = append(out, item)
	}
	return out
}

func (s *apiServer) handleMigrationSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleMigrationSettingsGet(w, r)
	case http.MethodPost:
		s.handleMigrationSettingsSave(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *apiServer) handleMigrationSettingsGet(w http.ResponseWriter, r *http.Request) {
	vmName := strings.TrimSpace(r.URL.Query().Get("vm_name"))
	vmMoref := strings.TrimSpace(r.URL.Query().Get("vm_moref"))
	if vmName == "" {
		writeError(w, http.StatusBadRequest, "vm_name is required")
		return
	}
	settingsPath := s.settingsPathForVM(vmName, vmMoref)
	if settingsPath == "" {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No saved settings found for VM '%s'.", vmName))
		return
	}
	payload, savedAt, err := loadMigrationSettingsFile(settingsPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to read saved settings: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":       payload.VMName,
		"vm_moref":      payload.VMMoref,
		"settings":      payload,
		"settings_file": settingsPath,
		"saved_at":      savedAt,
	})
}

func validateUniqueNICNetworkMappings(nicMappings map[string]nicMappingInput) error {
	if len(nicMappings) == 0 {
		return nil
	}
	seen := map[string]string{}
	for nicID, nic := range nicMappings {
		networkID := strings.TrimSpace(nic.NetworkID)
		if networkID == "" {
			continue
		}
		if prevNIC, ok := seen[networkID]; ok {
			return fmt.Errorf("duplicate NIC network mapping: network %s is selected for NICs %s and %s", networkID, prevNIC, nicID)
		}
		seen[networkID] = nicID
	}
	return nil
}

func (s *apiServer) handleMigrationSettingsSave(w http.ResponseWriter, r *http.Request) {
	var req migrationSpecRequest
	if err := decodeJSONBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.VMName = strings.TrimSpace(req.VMName)
	req.VMMoref = strings.TrimSpace(req.VMMoref)
	if req.VMName == "" {
		writeError(w, http.StatusBadRequest, "vm_name is required")
		return
	}
	if err := validateUniqueNICNetworkMappings(req.NICMappings); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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

	settings := migrationSettingsFile{
		Version: 1,
		SavedAt: time.Now().UTC(),
		Payload: req,
	}
	raw, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to marshal settings: %v", err))
		return
	}
	settingsPath := filepath.Join(vmDir, "ui_settings.json")
	if err := os.WriteFile(settingsPath, raw, 0o644); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to write settings: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":       req.VMName,
		"vm_moref":      req.VMMoref,
		"settings_file": settingsPath,
		"saved_at":      settings.SavedAt,
	})
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
	if err := validateUniqueNICNetworkMappings(req.NICMappings); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	hasMappedNIC := false
	for _, nic := range req.NICMappings {
		if strings.TrimSpace(nic.NetworkID) != "" {
			hasMappedNIC = true
			break
		}
	}
	if strings.TrimSpace(req.ZoneID) == "" || strings.TrimSpace(req.ClusterID) == "" ||
		(strings.TrimSpace(req.NetworkID) == "" && !hasMappedNIC) || strings.TrimSpace(req.ServiceOfferingID) == "" ||
		strings.TrimSpace(req.BootStorageID) == "" {
		writeError(w, http.StatusBadRequest, "zoneid, clusterid, serviceofferingid, boot_storageid and at least one network mapping are required")
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
	if req.Migration.StartVMAfterImport {
		migrationBlock["start_vm_after_import"] = true
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
	target, _ := spec["target"].(map[string]any)
	cloud, _ := target["cloudstack"].(map[string]any)
	if strings.TrimSpace(req.OSTypeID) != "" {
		cloud["ostypeid"] = strings.TrimSpace(req.OSTypeID)
	}
	if strings.TrimSpace(req.BootType) != "" {
		cloud["boottype"] = strings.TrimSpace(req.BootType)
	}
	if strings.EqualFold(strings.TrimSpace(req.BootType), "UEFI") && strings.TrimSpace(req.BootMode) != "" {
		cloud["bootmode"] = strings.TrimSpace(req.BootMode)
	}
	if strings.TrimSpace(req.RootDiskController) != "" {
		cloud["rootdiskcontroller"] = strings.TrimSpace(req.RootDiskController)
	}
	if strings.TrimSpace(req.NICAdapter) != "" {
		cloud["nicadapter"] = strings.TrimSpace(req.NICAdapter)
	}

	if len(req.NICMappings) > 0 {
		nicMappings := map[string]map[string]any{}
		for k, v := range req.NICMappings {
			networkID := strings.TrimSpace(v.NetworkID)
			if networkID == "" {
				continue
			}
			nicMappings[k] = map[string]any{
				"source_label":      strings.TrimSpace(v.SourceLabel),
				"source_network":    strings.TrimSpace(v.SourceNetwork),
				"source_mac":        strings.TrimSpace(v.SourceMAC),
				"source_device_key": v.SourceDeviceKey,
				"source_index":      v.SourceIndex,
				"networkid":         networkID,
			}
		}
		if len(nicMappings) > 0 {
			cloud["nic_mappings"] = nicMappings
		}
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
	vc, err := s.vcenterFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cs, err := s.cloudStackClientFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	overrides := runEnvOverrides{
		VCenterHost:     vc.Host,
		VCenterUser:     vc.User,
		VCenterPassword: vc.Password,
		CloudEndpoint:   cs.Endpoint,
		CloudAPIKey:     cs.APIKey,
		CloudSecretKey:  cs.SecretKey,
	}
	job := s.startJob(req.VMName, specPath, overrides)
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
	jobID := strings.TrimSpace(r.URL.Query().Get("job_id"))
	job := (*apiJob)(nil)
	runDir := ""
	st := (*runState)(nil)

	if jobID != "" {
		j := s.getJobByID(jobID)
		if j == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("Job not found: %s", jobID))
			return
		}
		if strings.TrimSpace(j.VMName) != vmName {
			writeError(w, http.StatusBadRequest, "job_id does not belong to requested VM")
			return
		}
		job = j
		runDir = s.runtimeDirForJob(job)
		st = s.loadStateFromDir(runDir)
	} else {
		st = s.loadState(vmName)
		job = s.latestJobForVM(vmName)
		if job != nil {
			runDir = s.runtimeDirForJob(job)
			if st == nil {
				st = s.loadStateFromDir(runDir)
			}
		}
	}
	if st == nil && job == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No migration state found for VM '%s'.", vmName))
		return
	}
	writeJSON(w, http.StatusOK, s.buildStatusPayload(vmName, st, job, runDir))
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
	latestPerVM := parseBool(strings.TrimSpace(r.URL.Query().Get("latest_per_vm")), false)

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
	if latestPerVM {
		filtered := make([]*apiJob, 0, len(jobs))
		seen := map[string]struct{}{}
		for _, job := range jobs {
			if _, ok := seen[job.VMName]; ok {
				continue
			}
			seen[job.VMName] = struct{}{}
			filtered = append(filtered, job)
		}
		jobs = filtered
	}
	if limit < len(jobs) {
		jobs = jobs[:limit]
	}

	out := make([]map[string]any, 0, len(jobs))
	for _, job := range jobs {
		runDir := s.runtimeDirForJob(job)
		status := s.statusPayloadForJobListing(job, runDir)
		out = append(out, map[string]any{
			"job_id":      job.JobID,
			"vm_name":     job.VMName,
			"status":      job.Status,
			"spec_file":   job.SpecFile,
			"runtime_dir": emptyToNil(runDir),
			"started_at":  job.StartedAt,
			"finished_at": job.FinishedAt,
			"return_code": job.ReturnCode,
			"error":       emptyToNil(job.Error),
			"stage":       status["stage"],
			"next_stage":  status["next_stage"],
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
	immediate := parseBool(strings.TrimSpace(r.URL.Query().Get("now")), false)

	targetDir := ""
	if job := s.latestJobForVM(vmName); job != nil {
		targetDir = s.runtimeDirForJob(job)
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
	finalizeNowPath := filepath.Join(targetDir, "FINALIZE_NOW")
	alreadyRequested := false
	alreadyImmediate := false
	if st, err := os.Stat(finalizePath); err == nil && !st.IsDir() {
		alreadyRequested = true
	}
	if st, err := os.Stat(finalizeNowPath); err == nil && !st.IsDir() {
		alreadyImmediate = true
	}
	if f, err := os.OpenFile(finalizePath, os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		_ = f.Close()
	} else {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if immediate {
		if f, err := os.OpenFile(finalizeNowPath, os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			_ = f.Close()
		} else {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":           vmName,
		"finalize_file":     finalizePath,
		"finalize_now_file": finalizeNowPath,
		"already_requested": alreadyRequested,
		"already_immediate": alreadyImmediate,
		"immediate":         immediate,
		"message": func() string {
			if immediate && alreadyImmediate {
				return "Finalize-now marker already present"
			}
			if immediate {
				return "Finalize-now marker created"
			}
			if alreadyRequested {
				return "Finalize marker already present"
			}
			return "Finalize marker created"
		}(),
	})
}

func (s *apiServer) handleMigrationShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vmName := strings.TrimPrefix(r.URL.Path, "/migration/shutdown/")
	vmName, _ = neturl.PathUnescape(vmName)
	vmName = strings.TrimSpace(vmName)
	if vmName == "" {
		writeError(w, http.StatusBadRequest, "vm name is required")
		return
	}
	action := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("action")))
	if action == "" {
		writeError(w, http.StatusBadRequest, "action is required")
		return
	}
	if action != "force" && action != "manual" {
		writeError(w, http.StatusBadRequest, "action must be one of: force, manual")
		return
	}

	targetDir := ""
	if job := s.latestJobForVM(vmName); job != nil {
		targetDir = s.runtimeDirForJob(job)
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

	st := s.loadStateFromDir(targetDir)
	if st == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("No migration state found for VM '%s'.", vmName))
		return
	}
	if !st.ShutdownActionRequired && strings.TrimSpace(st.Stage) != stageAwaitingShutdown {
		writeError(w, http.StatusConflict, fmt.Sprintf("VM '%s' is not waiting for a shutdown action.", vmName))
		return
	}
	if strings.TrimSpace(st.VMMoref) == "" {
		writeError(w, http.StatusConflict, fmt.Sprintf("VM '%s' does not have VMware runtime information available.", vmName))
		return
	}

	forcePath := filepath.Join(targetDir, "SHUTDOWN_FORCE")
	manualPath := filepath.Join(targetDir, "SHUTDOWN_MANUAL")
	touchMarker := func(path string) error {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		return f.Close()
	}

	if action == "force" {
		if err := touchMarker(forcePath); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"vm_name": vmName,
			"action":  action,
			"message": "Forced power-off requested. The engine will power off the source VM and continue once it is confirmed off.",
		})
		return
	}

	vc, err := s.vcenterFromRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	client, err := connectVCenterRuntime(ctx, vc)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("failed to connect VMware: %v", err))
		return
	}
	defer func() {
		_ = client.Logout(context.Background())
	}()
	vm := object.NewVirtualMachine(client.Client, types.ManagedObjectReference{
		Type:  "VirtualMachine",
		Value: strings.TrimSpace(st.VMMoref),
	})
	off, err := isVMPoweredOff(ctx, vm)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("failed to query VMware power state: %v", err))
		return
	}
	if !off {
		writeError(w, http.StatusConflict, "Source VM is still powered on. Shut it down manually first, then confirm again.")
		return
	}
	if err := touchMarker(manualPath); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":     vmName,
		"action":      action,
		"powered_off": true,
		"message":     "Manual shutdown confirmed. The engine will continue with final sync.",
	})
}

func (s *apiServer) handleMigrationRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	vmName := strings.TrimPrefix(r.URL.Path, "/migration/retry/")
	vmName, _ = neturl.PathUnescape(vmName)
	vmName = strings.TrimSpace(vmName)
	if vmName == "" {
		writeError(w, http.StatusBadRequest, "vm name is required")
		return
	}

	if running := s.activeJobForVM(vmName); running != nil {
		writeError(
			w,
			http.StatusConflict,
			fmt.Sprintf("VM '%s' already has an active job (%s)", vmName, running.JobID),
		)
		return
	}

	specHint := strings.TrimSpace(r.URL.Query().Get("spec_file"))
	specPath, err := s.resolveSpecFile(vmName, specHint)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	previous := s.latestJobForVM(vmName)
	retryOf := ""
	if previous != nil {
		retryOf = previous.JobID
	}
	var overrides runEnvOverrides
	hasVCHeaders := strings.TrimSpace(r.Header.Get("x-vcenter-host")) != "" ||
		strings.TrimSpace(r.Header.Get("x-vcenter-user")) != "" ||
		strings.TrimSpace(r.Header.Get("x-vcenter-password")) != ""
	hasCSHeaders := strings.TrimSpace(r.Header.Get("x-cloudstack-endpoint")) != "" ||
		strings.TrimSpace(r.Header.Get("x-cloudstack-api-key")) != "" ||
		strings.TrimSpace(r.Header.Get("x-cloudstack-secret-key")) != ""

	if previous != nil && !hasVCHeaders && !hasCSHeaders {
		overrides = previous.EnvOverrides
	} else {
		vc, err := s.vcenterFromRequest(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		cs, err := s.cloudStackClientFromRequest(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		overrides = runEnvOverrides{
			VCenterHost:     vc.Host,
			VCenterUser:     vc.User,
			VCenterPassword: vc.Password,
			CloudEndpoint:   cs.Endpoint,
			CloudAPIKey:     cs.APIKey,
			CloudSecretKey:  cs.SecretKey,
		}
	}

	job := s.startJob(vmName, specPath, overrides)
	writeJSON(w, http.StatusOK, map[string]any{
		"vm_name":   job.VMName,
		"job_id":    job.JobID,
		"spec_file": job.SpecFile,
		"status":    job.Status,
		"retry_of":  emptyToNil(retryOf),
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

func (s *apiServer) startJob(vmName string, specFile string, overrides runEnvOverrides) *apiJob {
	id := fmt.Sprintf("%d-%06d", time.Now().UnixNano(), atomic.AddUint64(&s.jobSeq, 1))
	runtimeDir := s.jobRuntimeDir(vmName, specFile)
	job := &apiJob{
		JobID:      id,
		VMName:     vmName,
		SpecFile:   specFile,
		Status:     "queued",
		StartedAt:  time.Now().UTC(),
		RuntimeDir: runtimeDir,
		EnvOverrides: overrides,
	}

	s.mu.Lock()
	s.jobs[id] = job
	s.jobsByVM[vmName] = append(s.jobsByVM[vmName], id)
	s.mu.Unlock()

	go s.runJob(job)
	return job
}

func (s *apiServer) activeJobForVM(vmName string) *apiJob {
	vmName = strings.TrimSpace(vmName)
	if vmName == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := append([]string(nil), s.jobsByVM[vmName]...)
	for i := len(ids) - 1; i >= 0; i-- {
		job := s.jobs[ids[i]]
		if job == nil {
			continue
		}
		status := strings.ToLower(strings.TrimSpace(job.Status))
		if status == "queued" || status == "running" {
			return job
		}
	}
	return nil
}

func (s *apiServer) runJob(job *apiJob) {
	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	s.mu.Lock()
	if current, ok := s.jobs[job.JobID]; ok {
		current.Status = "running"
	}
	s.mu.Unlock()

	vmDir := s.runtimeDirForJob(job)
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
	cmd.Env = withRunEnvOverrides(os.Environ(), job.EnvOverrides)

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
	var jobCopy *apiJob

	s.mu.Lock()
	job, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return
	}
	job.FinishedAt = &now
	job.ReturnCode = &exitCode
	if exitCode == 0 {
		job.Status = "completed"
		job.Error = ""
	} else {
		job.Status = "failed"
		job.Error = errText
	}
	copy := *job
	jobCopy = &copy
	s.mu.Unlock()

	if jobCopy == nil {
		return
	}
	runDir := s.runtimeDirForJob(jobCopy)
	st := s.loadStateFromDir(runDir)
	status := s.buildStatusPayload(jobCopy.VMName, st, jobCopy, runDir)

	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok = s.jobs[jobID]
	if !ok {
		return
	}
	job.StageSnapshot = strings.TrimSpace(anyToString(status["stage"]))
	job.NextStageSnapshot = strings.TrimSpace(anyToString(status["next_stage"]))
	job.ProgressSnapshot = anyToFloat(status["overall_progress"])
	job.TransferSpeedSnapshot = anyToFloat(status["transfer_speed_mbps"])
	job.FinalizeRequestedSnapshot = anyToBool(status["finalize_requested"])
	job.FinalizeNowRequestedSnapshot = anyToBool(status["finalize_now_requested"])
	job.UpdatedAtSnapshot = now
}

func withRunEnvOverrides(base []string, o runEnvOverrides) []string {
	set := func(env []string, key, value string) []string {
		if strings.TrimSpace(key) == "" {
			return env
		}
		prefix := key + "="
		for i := range env {
			if strings.HasPrefix(env[i], prefix) {
				if strings.TrimSpace(value) == "" {
					return append(env[:i], env[i+1:]...)
				}
				env[i] = prefix + value
				return env
			}
		}
		if strings.TrimSpace(value) != "" {
			env = append(env, prefix+value)
		}
		return env
	}

	env := append([]string{}, base...)
	env = set(env, "V2C_VCENTER_HOST", o.VCenterHost)
	env = set(env, "V2C_VCENTER_USER", o.VCenterUser)
	env = set(env, "V2C_VCENTER_PASSWORD", o.VCenterPassword)
	env = set(env, "VC_PASSWORD", o.VCenterPassword)
	env = set(env, "V2C_CLOUDSTACK_ENDPOINT", o.CloudEndpoint)
	env = set(env, "V2C_CLOUDSTACK_API_KEY", o.CloudAPIKey)
	env = set(env, "V2C_CLOUDSTACK_SECRET_KEY", o.CloudSecretKey)
	return env
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

func (s *apiServer) getJobByID(jobID string) *apiJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[jobID]
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

func listVMwareInventory(ctx context.Context, client *govmomi.Client, filter vmwareInventoryFilter) ([]vmwareVMInfo, error) {
	vmgr := view.NewManager(client.Client)
	cv, err := vmgr.CreateContainerView(ctx, client.ServiceContent.RootFolder, []string{"VirtualMachine"}, true)
	if err != nil {
		return nil, err
	}
	defer cv.Destroy(ctx)

	var vms []mo.VirtualMachine
	if err := cv.Retrieve(ctx, []string{"VirtualMachine"}, []string{
		"name",
		"config.template",
		"config.hardware.numCPU",
		"config.hardware.memoryMB",
		"config.hardware.device",
		"config.bootOptions",
	}, &vms); err != nil {
		return nil, err
	}

	out := make([]vmwareVMInfo, 0, len(vms))
	for _, vm := range vms {
		isTemplate := vm.Config != nil && vm.Config.Template
		isVCLS := isVCLSVM(vm)
		if isTemplate && !filter.IncludeTemplates {
			continue
		}
		if isVCLS && !filter.IncludeVCLS {
			continue
		}
		if vm.Config == nil || vm.Config.Hardware.Device == nil {
			continue
		}
		bootUnit := detectBootDiskUnit(vm.Config)

		disks := make([]vmwareDiskInfo, 0)
		nics := make([]vmwareNICInfo, 0)
		dsSet := map[string]struct{}{}
		nicIdx := 0
		for _, dev := range vm.Config.Hardware.Device {
			vd, ok := dev.(*types.VirtualDisk)
			if ok {
				if vd.UnitNumber == nil {
					continue
				}
				unit := int(*vd.UnitNumber)
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

			nic, ok := dev.(types.BaseVirtualEthernetCard)
			if !ok {
				continue
			}
			card := nic.GetVirtualEthernetCard()
			if card == nil {
				continue
			}
			label := fmt.Sprintf("Network adapter %d", nicIdx+1)
			if card.DeviceInfo != nil {
				label = card.DeviceInfo.GetDescription().Label
			}
			nics = append(nics, vmwareNICInfo{
				Label:      label,
				Network:    networkFromNICBacking(card.Backing),
				MACAddress: card.MacAddress,
				DeviceKey:  card.Key,
				Index:      nicIdx,
			})
			nicIdx++
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
			Name:       vm.Name,
			Moref:      vm.Reference().Value,
			CPU:        cpu,
			Memory:     mem,
			Disks:      disks,
			NICs:       nics,
			Datastore:  dsList,
			IsTemplate: isTemplate,
			IsVCLS:     isVCLS,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func isVCLSVM(vm mo.VirtualMachine) bool {
	name := strings.ToLower(strings.TrimSpace(vm.Name))
	if strings.HasPrefix(name, "vcls") || strings.Contains(name, "vcls-") {
		return true
	}
	if vm.Config != nil && vm.Config.ManagedBy != nil {
		ext := strings.ToLower(strings.TrimSpace(vm.Config.ManagedBy.ExtensionKey))
		if strings.Contains(ext, "eam") || strings.Contains(ext, "vcls") {
			return true
		}
	}
	return false
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
	value := strings.TrimSpace(endpoint)
	if value == "" {
		return value
	}
	if !strings.Contains(value, "://") {
		value = "http://" + strings.TrimLeft(value, "/")
	}
	u, err := neturl.Parse(value)
	if err != nil {
		// Keep backward-compatible behavior if parse fails.
		value = strings.TrimRight(value, "/")
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

	path := strings.Trim(strings.TrimSpace(u.Path), "/")
	pathLower := strings.ToLower(path)
	switch {
	case pathLower == "" || pathLower == "client" || pathLower == "client/api":
		u.Path = "/client/api"
	case pathLower == "api":
		u.Path = "/api"
	default:
		// If user provided a custom base path, keep it and append client/api.
		u.Path = "/" + strings.TrimRight(path, "/") + "/client/api"
	}
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/")
}

func cloudStackEndpointName(endpoint string) string {
	value := strings.TrimSpace(endpoint)
	if value == "" {
		return ""
	}
	if !strings.Contains(value, "://") {
		value = "http://" + strings.TrimLeft(value, "/")
	}
	u, err := neturl.Parse(value)
	if err == nil {
		if host := strings.TrimSpace(u.Hostname()); host != "" {
			return host
		}
	}
	value = strings.TrimSpace(endpoint)
	value = strings.TrimPrefix(value, "http://")
	value = strings.TrimPrefix(value, "https://")
	if slash := strings.Index(value, "/"); slash >= 0 {
		value = value[:slash]
	}
	if colon := strings.Index(value, ":"); colon >= 0 {
		value = value[:colon]
	}
	return strings.TrimSpace(value)
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
		if vd.UnitNumber == nil {
			continue
		}
		unit := int(*vd.UnitNumber)
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

func networkFromNICBacking(backing types.BaseVirtualDeviceBackingInfo) string {
	switch b := backing.(type) {
	case *types.VirtualEthernetCardNetworkBackingInfo:
		return strings.TrimSpace(b.DeviceName)
	case *types.VirtualEthernetCardDistributedVirtualPortBackingInfo:
		if strings.TrimSpace(b.Port.PortgroupKey) != "" {
			return strings.TrimSpace(b.Port.PortgroupKey)
		}
		if strings.TrimSpace(b.Port.SwitchUuid) != "" {
			return strings.TrimSpace(b.Port.SwitchUuid)
		}
	case *types.VirtualEthernetCardOpaqueNetworkBackingInfo:
		return strings.TrimSpace(b.OpaqueNetworkId)
	}
	return ""
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
	vms, err := listVMwareInventory(ctx, client, vmwareInventoryFilter{
		IncludeTemplates: true,
		IncludeVCLS:      true,
	})
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

func (s *apiServer) settingsPathForVM(vmName string, vmMoref string) string {
	vmName = strings.TrimSpace(vmName)
	vmMoref = strings.TrimSpace(vmMoref)
	if vmName == "" {
		return ""
	}
	if vmMoref != "" {
		candidate := filepath.Join(s.controlDir, fmt.Sprintf("%s_%s", safeVMName(vmName), vmMoref), "ui_settings.json")
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate
		}
	}
	for _, dir := range s.candidateVMDirs(vmName) {
		p := filepath.Join(dir, "ui_settings.json")
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

func loadMigrationSettingsFile(path string) (migrationSpecRequest, time.Time, error) {
	var zero migrationSpecRequest
	raw, err := os.ReadFile(path)
	if err != nil {
		return zero, time.Time{}, err
	}
	var wrapped migrationSettingsFile
	if err := json.Unmarshal(raw, &wrapped); err == nil && strings.TrimSpace(wrapped.Payload.VMName) != "" {
		return wrapped.Payload, wrapped.SavedAt, nil
	}
	var payload migrationSpecRequest
	if err := json.Unmarshal(raw, &payload); err != nil {
		return zero, time.Time{}, err
	}
	return payload, time.Time{}, nil
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

func (s *apiServer) runtimeDirForJob(job *apiJob) string {
	if job == nil {
		return ""
	}
	if strings.TrimSpace(job.RuntimeDir) != "" {
		return strings.TrimSpace(job.RuntimeDir)
	}
	return s.jobRuntimeDir(job.VMName, job.SpecFile)
}

func statePathFromDir(dir string) string {
	for _, p := range []string{
		filepath.Join(dir, "state.json"),
		filepath.Join(dir, "state.engine.json"),
	} {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

func (s *apiServer) loadStateFromDir(dir string) *runState {
	if strings.TrimSpace(dir) == "" {
		return nil
	}
	statePath := statePathFromDir(dir)
	if statePath == "" {
		return nil
	}
	st, err := loadRunState(statePath)
	if err != nil {
		return nil
	}
	return st
}

func (s *apiServer) statePathForVM(vmName string) string {
	for _, dir := range s.candidateVMDirs(vmName) {
		p := statePathFromDir(dir)
		if p != "" {
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

func (s *apiServer) finalizeRequestedForVM(vmName string) bool {
	for _, dir := range s.candidateVMDirs(vmName) {
		if finalizeRequestedForDir(dir) {
			return true
		}
	}
	return false
}

func (s *apiServer) finalizeNowRequestedForVM(vmName string) bool {
	for _, dir := range s.candidateVMDirs(vmName) {
		if finalizeNowRequestedForDir(dir) {
			return true
		}
	}
	return false
}

func finalizeRequestedForDir(dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	p := filepath.Join(dir, "FINALIZE")
	pNow := filepath.Join(dir, "FINALIZE_NOW")
	if st, err := os.Stat(p); err == nil && !st.IsDir() {
		return true
	}
	if st, err := os.Stat(pNow); err == nil && !st.IsDir() {
		return true
	}
	return false
}

func finalizeNowRequestedForDir(dir string) bool {
	if strings.TrimSpace(dir) == "" {
		return false
	}
	p := filepath.Join(dir, "FINALIZE_NOW")
	if st, err := os.Stat(p); err == nil && !st.IsDir() {
		return true
	}
	return false
}

func (s *apiServer) runVirtSettingForVM(vmName string) bool {
	runVirt := false
	if s.cfg != nil {
		runVirt = s.cfg.Virt.RunVirtV2V
	}

	specPath, err := s.latestSpecForVM(vmName)
	if err != nil {
		return runVirt
	}
	specs, err := loadRunSpecs([]string{specPath})
	if err != nil || len(specs) == 0 {
		return runVirt
	}
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		if strings.TrimSpace(spec.VM.Name) == strings.TrimSpace(vmName) {
			return effectiveRunVirtV2V(s.cfg, spec)
		}
	}
	if len(specs) == 1 {
		return effectiveRunVirtV2V(s.cfg, specs[0])
	}
	return runVirt
}

func (s *apiServer) buildStatusPayload(vmName string, st *runState, job *apiJob, runtimeDir string) map[string]any {
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
	readTotalBytes := int64(0)
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
			copied = ds.BytesWritten
		}
		if copied <= 0 {
			copied = ds.BytesRead
		}
		if copied < 0 {
			copied = 0
		}
		if copied > total && total > 0 {
			copied = total
		}
		readBytes := ds.BytesRead
		if readBytes <= 0 {
			readBytes = copied
		}
		if readBytes < 0 {
			readBytes = 0
		}
		if readBytes > total && total > 0 {
			readBytes = total
		}
		remaining := total - readBytes
		if remaining < 0 {
			remaining = 0
		}
		pct := ds.Progress
		if pct <= 0 && total > 0 {
			pct = math.Round((float64(readBytes)*10000)/float64(total)) / 100
		}

		totalBytes += total
		readTotalBytes += readBytes
		if ds.SpeedMBps > 0 {
			speedSum += ds.SpeedMBps
			speedCount++
		}

		diskProgress = append(diskProgress, map[string]any{
			"unit":              k,
			"disk_name":         fmt.Sprintf("Disk %d", ds.Unit),
			"provisioned_size":  formatBytes(total),
			"provisioned_bytes": total,
			"used_size":         formatBytes(copied),
			"used_bytes":        copied,
			"total_size":        formatBytes(total),
			"copied_size":       formatBytes(copied),
			"remaining_size":    formatBytes(remaining),
			"total_bytes":       total,
			"copied_bytes":      copied,
			"remaining_bytes":   remaining,
			"speed_mbps":        ds.SpeedMBps,
			"eta_seconds":       ds.EtaSeconds,
			"progress":          pct,
		})
	}

	overall := progress
	if overall <= 0 && totalBytes > 0 {
		overall = math.Round((float64(readTotalBytes)*10000)/float64(totalBytes)) / 100
	}
	if overall <= 0 && stage == stageDone {
		overall = 100
	}
	if transfer <= 0 && speedCount > 0 {
		transfer = math.Round((speedSum/float64(speedCount))*100) / 100
	}
	finalizeRequested := false
	finalizeNowRequested := false
	if strings.TrimSpace(runtimeDir) != "" {
		finalizeRequested = finalizeRequestedForDir(runtimeDir)
		finalizeNowRequested = finalizeNowRequestedForDir(runtimeDir)
	} else {
		finalizeRequested = s.finalizeRequestedForVM(vmName)
		finalizeNowRequested = s.finalizeNowRequestedForVM(vmName)
	}
	currentStage := strings.TrimSpace(stage)
	if currentStage == "" {
		currentStage = "not_started"
	}
	nextStage := nextStageForStatus(currentStage, s.runVirtSettingForVM(vmName), finalizeRequested)

	payload := map[string]any{
		"vm_name":             vmName,
		"stage":               emptyToNil(stage),
		"next_stage":          nextStage,
		"finalize_requested":  finalizeRequested,
		"finalize_now_requested": finalizeNowRequested,
		"awaiting_user_action": st != nil && st.ShutdownActionRequired,
		"required_action": func() any {
			if st != nil && st.ShutdownActionRequired {
				return "shutdown_choice"
			}
			return nil
		}(),
		"shutdown_reason": func() any {
			if st == nil {
				return nil
			}
			return emptyToNil(strings.TrimSpace(st.ShutdownReason))
		}(),
		"shutdown_tools_status": func() any {
			if st == nil {
				return nil
			}
			return emptyToNil(strings.TrimSpace(st.ShutdownToolsStatus))
		}(),
		"shutdown_acpi_attempted": st != nil && st.ShutdownACPIAttempted,
		"available_actions": func() []string {
			if st != nil && st.ShutdownActionRequired {
				return []string{"force_poweroff", "manual_shutdown_done"}
			}
			return []string{}
		}(),
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

func (s *apiServer) statusPayloadForJobListing(job *apiJob, runtimeDir string) map[string]any {
	if job == nil {
		return map[string]any{}
	}
	if strings.TrimSpace(job.Status) == "running" || strings.TrimSpace(job.Status) == "queued" {
		st := s.loadStateFromDir(runtimeDir)
		return s.buildStatusPayload(job.VMName, st, job, runtimeDir)
	}
	if strings.TrimSpace(job.StageSnapshot) != "" {
		return map[string]any{
			"stage":                  emptyToNil(job.StageSnapshot),
			"next_stage":             emptyToNil(job.NextStageSnapshot),
			"overall_progress":       job.ProgressSnapshot,
			"transfer_speed_mbps":    job.TransferSpeedSnapshot,
			"finalize_requested":     job.FinalizeRequestedSnapshot,
			"finalize_now_requested": job.FinalizeNowRequestedSnapshot,
			"updated_at":             job.UpdatedAtSnapshot,
		}
	}
	stage := "unknown"
	progress := float64(0)
	switch strings.ToLower(strings.TrimSpace(job.Status)) {
	case "completed":
		stage = stageDone
		progress = 100
	case "failed":
		stage = "failed"
	}
	return map[string]any{
		"stage":                  stage,
		"next_stage":             "none",
		"overall_progress":       progress,
		"transfer_speed_mbps":    0,
		"finalize_requested":     false,
		"finalize_now_requested": false,
		"updated_at":             job.FinishedAt,
	}
}

func anyToFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f
	default:
		return 0
	}
}

func anyToString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

func anyToBool(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return parseBool(strings.TrimSpace(t), false)
	case int:
		return t != 0
	case int64:
		return t != 0
	case float64:
		return t != 0
	default:
		return false
	}
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
			targetDir = s.runtimeDirForJob(explicit)
		}
	}
	if targetDir == "" && job != nil {
		targetDir = s.runtimeDirForJob(job)
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
