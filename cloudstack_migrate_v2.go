package main

import (
    "context"
    "crypto/hmac"
    "crypto/sha1"
    "encoding/base64"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "net/http"
    "net/url"
    "os"
    "os/exec"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/vmware/govmomi"
    "github.com/vmware/govmomi/find"
    "github.com/vmware/govmomi/object"
    "github.com/vmware/govmomi/vim25/mo"
    "github.com/vmware/govmomi/vim25/types"
    "github.com/vmware/govmomi/vddk"
    "github.com/ncw/go-qcow2"
    "gopkg.in/yaml.v2"
)

type Config struct {
    Vcenter struct {
        Host string `yaml:"host"`
        User string `yaml:"user"`
        Password string `yaml:"password"`
    } `yaml:"vcenter"`
    Migration struct {
        VddkPath string `yaml:"vddk_path"`
        DataDir string `yaml:"data_dir"`
    } `yaml:"migration"`
    Cloudstack struct {
        Endpoint string `yaml:"endpoint"`
        ApiKey string `yaml:"api_key"`
        SecretKey string `yaml:"secret_key"`
        Defaults map[string]interface{} `yaml:"defaults"`
    } `yaml:"cloudstack"`
}

type MigrationStage string

const (
    Init MigrationStage = "init"
    BaseCopy = "base_copy"
    Delta = "delta"
    Converting = "converting"
    ImportRootDisk = "import_root_disk"
    ImportDataDisk = "import_data_disk"
    Done = "done"
)

type DiskState struct {
    Path string `json:"path"`
    Format string `json:"format"`
    ChangeId string `json:"changeId"`
    Capacity int64 `json:"capacity"`
    Key int32 `json:"key"`
    CopiedBytes int64 `json:"copied_bytes"`
    Progress float64 `json:"progress"`
    QemuProgress float64 `json:"qemu_progress"`
    SpeedMb float64 `json:"speed_mb"`
    SpeedMbps float64 `json:"speed_mbps"`
    TransferSpeedMbps float64 `json:"transfer_speed_mbps"`
    EtaSeconds int `json:"eta_seconds"`
    DeltaTotalBytes int64 `json:"delta_total_bytes"`
    DeltaBytesWritten int64 `json:"delta_bytes_written"`
    DeltaProgress float64 `json:"delta_progress"`
}

type State struct {
    Vm string `json:"vm"`
    Stage MigrationStage `json:"stage"`
    Disks map[string]DiskState `json:"disks"`
    ActiveSnapshot string `json:"active_snapshot"`
    Progress float64 `json:"progress"`
    TransferSpeedMbps float64 `json:"transfer_speed_mbps"`
}

type Disk struct {
    Unit int
    Key int32
    Capacity int64
    Path string
}

type Block struct {
    Offset int64
    Size int
}

type Migrator struct {
    vm *object.VirtualMachine
    si *vim.ServiceInstance
    ctx context.Context
    config Config
    state State
    migrationId string
    controlDir string
    spec map[string]interface{}
    dataDir string
    thumbprint string
    stateLock sync.RWMutex
}

func main() {
    specFile := flag.String("spec", "", "spec file")
    flag.Parse()

    if *specFile == "" {
        log.Fatal("spec file required")
    }

    config := loadConfig("config.yaml")
    spec := loadSpec(*specFile)

    vmName := spec["vm"].(string)

    m := &Migrator{
        ctx: context.Background(),
        config: config,
        spec: spec,
        migrationId: fmt.Sprintf("%s_%d", vmName, time.Now().Unix()),
        controlDir: filepath.Join("/var/lib/vm-migrator", fmt.Sprintf("%s_%d", vmName, time.Now().Unix())),
        dataDir: config.Migration.DataDir,
    }

    os.MkdirAll(m.controlDir, 0755)

    m.connectVcenter()
    m.findVm(vmName)
    m.loadState()
    m.thumbprint = getThumbprint(config.Vcenter.Host)

    m.migrate()
}

func loadConfig(path string) Config {
    data, err := os.ReadFile(path)
    if err != nil {
        log.Fatal(err)
    }
    var config Config
    yaml.Unmarshal(data, &config)
    return config
}

func loadSpec(path string) map[string]interface{} {
    data, err := os.ReadFile(path)
    if err != nil {
        log.Fatal(err)
    }
    var spec map[string]interface{}
    yaml.Unmarshal(data, &spec)
    return spec
}

func (m *Migrator) connectVcenter() {
    u, err := url.Parse("https://" + m.config.Vcenter.Host + "/sdk")
    if err != nil {
        log.Fatal(err)
    }
    c, err := govmomi.NewClient(m.ctx, u, true)
    if err != nil {
        log.Fatal(err)
    }
    err = c.Login(m.ctx, url.UserPassword(m.config.Vcenter.User, m.config.Vcenter.Password))
    if err != nil {
        log.Fatal(err)
    }
    m.si = c.Client.ServiceInstance
}

func (m *Migrator) findVm(name string) {
    finder := find.NewFinder(m.si.Client, true)
    vm, err := finder.VirtualMachine(m.ctx, name)
    if err != nil {
        log.Fatal(err)
    }
    m.vm = vm
}

func (m *Migrator) loadState() {
    stateFile := filepath.Join(m.controlDir, "state.json")
    if _, err := os.Stat(stateFile); os.IsNotExist(err) {
        m.state = State{
            Vm: m.vm.Name(),
            Stage: Init,
            Disks: make(map[string]DiskState),
        }
        return
    }
    data, err := os.ReadFile(stateFile)
    if err != nil {
        log.Fatal(err)
    }
    json.Unmarshal(data, &m.state)
}

func (m *Migrator) saveState() {
    m.stateLock.Lock()
    defer m.stateLock.Unlock()
    data, err := json.MarshalIndent(m.state, "", "  ")
    if err != nil {
        log.Fatal(err)
    }
    stateFile := filepath.Join(m.controlDir, "state.json")
    os.WriteFile(stateFile, data, 0644)
}

func getThumbprint(host string) string {
    // Implement thumbprint retrieval
    return ""
}

func (m *Migrator) migrate() {
    for {
        stage := m.state.Stage
        log.Printf("Stage: %s", stage)

        switch stage {
        case Init:
            m.ensureCbtEnabled()
            m.state.Stage = BaseCopy
            m.saveState()
            m.baseCopy()
            m.state.Stage = Delta
            m.saveState()
        case BaseCopy, Delta:
            m.runDeltaLoop()
        case Converting:
            m.runVirtV2v()
        case ImportRootDisk:
            bootUnit := m.getBootDiskUnit()
            bootDisk := m.getV2vBootDisk(bootUnit)
            vmId := m.importVmToCloudstack(bootDisk)
            // set vm_id
            m.state.Stage = ImportDataDisk
            m.saveState()
        case ImportDataDisk:
            m.stageImportDataDisk()
            m.state.Stage = Done
            m.saveState()
        case Done:
            log.Printf("%s migration completed", m.vm.Name())
            return
        }
    }
}

func (m *Migrator) ensureCbtEnabled() {
    // Implement CBT enable
}

func (m *Migrator) checkSnapshotLimit() {
    // Implement snapshot limit check
}

func (m *Migrator) createSnapshot(name string) *types.ManagedObjectReference {
    task, err := m.vm.CreateSnapshot(m.ctx, name, "", false, false)
    if err != nil {
        log.Fatal(err)
    }
    info, err := task.WaitForResult(m.ctx, nil)
    if err != nil {
        log.Fatal(err)
    }
    return info.Result.(types.ManagedObjectReference)
}

func (m *Migrator) disks() []Disk {
    var disks []Disk
    // Implement disk retrieval
    return disks
}

func (m *Migrator) baseCopy() {
    m.checkSnapshotLimit()
    resSnap := m.createSnapshot("Migrate_Base_" + m.vm.Name())
    disks := m.disks()
    for _, disk := range disks {
        m.copyDiskBase(disk, resSnap)
    }
    m.state.ActiveSnapshot = resSnap.Value
    m.saveState()
}

func (m *Migrator) copyDiskBase(disk Disk, snap *types.ManagedObjectReference) {
    uStr := strconv.Itoa(disk.Unit)
    m.stateLock.Lock()
    diskState := m.state.Disks[uStr]
    diskState.Progress = 0
    diskState.QemuProgress = 0
    diskState.SpeedMb = 0
    diskState.SpeedMbps = 0
    diskState.BytesWritten = 0
    diskState.CopiedBytes = 0
    diskState.Capacity = disk.Capacity
    diskState.Key = disk.Key
    m.state.Stage = BaseCopy
    m.recalculateOverallProgress()
    m.stateLock.Unlock()
    m.saveState()

    storageId := m.getDiskStorage(disk.Unit)
    storagePath := ensureStorageMounted(storageId)
    targetQcow2 := filepath.Join(storagePath, fmt.Sprintf("%s_disk%s.qcow2", m.migrationId, uStr))

    log.Printf("Disk %s: Starting direct-to-qcow2 base copy", uStr)

    // Precreate QCOW2
    m.precreateQcow2(targetQcow2, disk.Capacity)

    // Open VDDK handles
    numHandles := 4
    var handles []*vddk.Handle
    for i := 0; i < numHandles; i++ {
        config := vddk.Config{
            Libdir:     m.config.Migration.VddkPath,
            Server:     m.config.Vcenter.Host,
            User:       m.config.Vcenter.User,
            Password:   m.config.Vcenter.Password,
            Thumbprint: m.thumbprint,
        }
        h, err := vddk.Open(m.vm, disk.Path, snap.Value, config)
        if err != nil {
            log.Fatal(err)
        }
        handles = append(handles, h)
    }

    // Create QCOW2 writer
    f, err := os.Create(targetQcow2)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

    q, err := qcow2.Create(f, uint64(disk.Capacity))
    if err != nil {
        log.Fatal(err)
    }
    defer q.Close()

    // Block queue
    blockQueue := make(chan Block, 100)
    var wg sync.WaitGroup

    // Workers
    for i := 0; i < numHandles; i++ {
        wg.Add(1)
        go func(idx int) {
            defer wg.Done()
            h := handles[idx]
            for block := range blockQueue {
                data, err := h.Read(block.Offset, block.Size)
                if err != nil {
                    log.Printf("read error: %v", err)
                    continue
                }
                if isAllZero(data) {
                    continue
                }
                err = q.Write(block.Offset, data)
                if err != nil {
                    log.Printf("write error: %v", err)
                }
            }
        }(i)
    }

    // Scheduler
    chunkSize := 1024 * 1024 // 1MB
    for offset := int64(0); offset < disk.Capacity; offset += int64(chunkSize) {
        size := chunkSize
        if offset + int64(size) > disk.Capacity {
            size = int(disk.Capacity - offset)
        }
        blockQueue <- Block{Offset: offset, Size: size}
    }
    close(blockQueue)
    wg.Wait()

    // Close handles
    for _, h := range handles {
        h.Close()
    }

    // Update state
    m.stateLock.Lock()
    diskState = m.state.Disks[uStr]
    diskState.Path = targetQcow2
    diskState.Format = "qcow2"
    // diskState.ChangeId = ...
    diskState.Capacity = disk.Capacity
    diskState.Key = disk.Key
    diskState.CopiedBytes = disk.Capacity
    diskState.Progress = 100.0
    m.recalculateOverallProgress()
    m.stateLock.Unlock()
    m.saveState()

    // Run virt-v2v
    m.runV2vBase(targetQcow2, targetQcow2, disk.Capacity, disk.Unit)
}

func isAllZero(data []byte) bool {
    for _, b := range data {
        if b != 0 {
            return false
        }
    }
    return true
}

func (m *Migrator) precreateQcow2(path string, size int64) {
    cmd := exec.Command("qemu-img", "create", "-f", "qcow2", "-o", "preallocation=falloc", path, strconv.FormatInt(size, 10))
    err := cmd.Run()
    if err != nil {
        log.Fatal(err)
    }
}

func (m *Migrator) runV2vBase(sourcePath, targetQcow2 string, diskSize int64, diskUnit int) {
    outputDir := filepath.Dir(targetQcow2)
    outputName := strings.TrimSuffix(filepath.Base(targetQcow2), filepath.Ext(targetQcow2))

    cmd := exec.Command("virt-v2v", "-i", "disk", sourcePath, "-o", "local", "-os", outputDir, "-of", "qcow2", "-on", outputName)
    err := cmd.Run()
    if err != nil {
        log.Fatal(err)
    }
}

func (m *Migrator) runDeltaLoop() {
    // Implement delta loop
}

func (m *Migrator) runVirtV2v() {
    // Implement virt-v2v
}

func (m *Migrator) getBootDiskUnit() int {
    // Implement
    return 0
}

func (m *Migrator) getV2vBootDisk(unit int) string {
    // Implement
    return ""
}

func (m *Migrator) importVmToCloudstack(disk string) string {
    // Implement CloudStack API
    return ""
}

func (m *Migrator) stageImportDataDisk() {
    // Implement
}

func (m *Migrator) getDiskStorage(unit int) string {
    // Implement
    return ""
}

func ensureStorageMounted(id string) string {
    // Implement
    return ""
}

func (m *Migrator) recalculateOverallProgress() {
    var total float64
    count := 0
    for _, d := range m.state.Disks {
        if d.Progress > 0 {
            total += d.Progress
            count++
        }
    }
    if count > 0 {
        m.state.Progress = total / float64(count)
    }
}