package main

/*
#cgo linux LDFLAGS: -ldl -lpthread
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <vixDiskLib.h>

static VixError v2c_vddk_init(const char *libdir) {
    return VixDiskLib_InitEx(7, 0, NULL, NULL, NULL, libdir, NULL);
}

static VixError v2c_vddk_connect(
    char *server,
    char *user,
    char *pass,
    char *thumb,
    char *vmxSpec,
    char *snapshotRef,
    VixDiskLibConnection *conn
) {
    VixDiskLibConnectParams params;
    memset(&params, 0, sizeof(params));
    params.serverName = server;
    params.port = 443;
    params.thumbPrint = thumb;
    params.vmxSpec = vmxSpec;
    params.credType = VIXDISKLIB_CRED_UID;
    params.creds.uid.userName = user;
    params.creds.uid.password = pass;
    return VixDiskLib_ConnectEx(&params, 1, snapshotRef, "nbdssl:nbd", conn);
}

static VixError v2c_vddk_open(VixDiskLibConnection conn, const char *path, VixDiskLibHandle *handle) {
    return VixDiskLib_Open(conn, path, VIXDISKLIB_FLAG_OPEN_READ_ONLY, handle);
}

static VixError v2c_vddk_read(VixDiskLibHandle handle, uint64_t startSector, uint64_t numSectors, uint8_t *buf) {
    return VixDiskLib_Read(
        handle,
        (VixDiskLibSectorType)startSector,
        (VixDiskLibSectorType)numSectors,
        buf
    );
}

static VixError v2c_vddk_get_capacity(VixDiskLibHandle handle, uint64_t *capacitySectors) {
    VixDiskLibInfo *info = NULL;
    VixError err = VixDiskLib_GetInfo(handle, &info);
    if (err != VIX_OK || info == NULL) {
        return err;
    }
    *capacitySectors = (uint64_t)info->capacity;
    VixDiskLib_FreeInfo(info);
    return VIX_OK;
}
*/
import "C"

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"gopkg.in/yaml.v3"
)

const (
	sectorSize = 512

	nbdMagic               = 0x4e42444d41474943
	nbdOptMagic            = 0x49484156454f5054
	nbdClientFixedNewstyle = 1

	nbdOptExportName = 1

	nbdRequestMagic = 0x25609513
	nbdReplyMagic   = 0x67446698

	nbdCmdWrite = 1
	nbdCmdDisc  = 2
)

type vddkConnCfg struct {
	LibDir       string
	Server       string
	User         string
	Password     string
	Thumbprint   string
	VMMoref      string
	SnapshotMoref string
}

type vddkConnection struct {
	ptr C.VixDiskLibConnection
}

type vddkHandle struct {
	ptr C.VixDiskLibHandle
}

var (
	vddkInitOnce sync.Once
	vddkInitErr  error
)

func initVDDK(libdir string) error {
	vddkInitOnce.Do(func() {
		cLibDir := C.CString(libdir)
		defer C.free(unsafe.Pointer(cLibDir))
		err := C.v2c_vddk_init(cLibDir)
		if err != 0 {
			vddkInitErr = fmt.Errorf("VixDiskLib_InitEx failed: %s", vixErrorText(err))
		}
	})
	return vddkInitErr
}

func normalizeMoref(in string) string {
	if strings.HasPrefix(in, "moref=") {
		return in
	}
	return "moref=" + in
}

func connectVDDK(cfg vddkConnCfg) (*vddkConnection, error) {
	if err := initVDDK(cfg.LibDir); err != nil {
		return nil, err
	}
	cServer := C.CString(cfg.Server)
	cUser := C.CString(cfg.User)
	cPass := C.CString(cfg.Password)
	cThumb := C.CString(cfg.Thumbprint)
	cVM := C.CString(normalizeMoref(cfg.VMMoref))
	cSnap := C.CString(normalizeMoref(cfg.SnapshotMoref))
	defer func() {
		C.free(unsafe.Pointer(cServer))
		C.free(unsafe.Pointer(cUser))
		C.free(unsafe.Pointer(cPass))
		C.free(unsafe.Pointer(cThumb))
		C.free(unsafe.Pointer(cVM))
		C.free(unsafe.Pointer(cSnap))
	}()

	var conn C.VixDiskLibConnection
	err := C.v2c_vddk_connect(cServer, cUser, cPass, cThumb, cVM, cSnap, &conn)
	if err != 0 {
		return nil, fmt.Errorf("VixDiskLib_ConnectEx failed: %s", vixErrorText(err))
	}
	return &vddkConnection{ptr: conn}, nil
}

func (c *vddkConnection) close() {
	if c == nil || c.ptr == nil {
		return
	}
	C.VixDiskLib_Disconnect(c.ptr)
	c.ptr = nil
}

func (c *vddkConnection) open(path string) (*vddkHandle, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var h C.VixDiskLibHandle
	err := C.v2c_vddk_open(c.ptr, cPath, &h)
	if err != 0 {
		return nil, fmt.Errorf("VixDiskLib_Open failed: %s", vixErrorText(err))
	}
	return &vddkHandle{ptr: h}, nil
}

func (h *vddkHandle) close() {
	if h == nil || h.ptr == nil {
		return
	}
	C.VixDiskLib_Close(h.ptr)
	h.ptr = nil
}

func (h *vddkHandle) readAt(offset int64, length int) ([]byte, error) {
	if offset%sectorSize != 0 || length%sectorSize != 0 {
		return nil, fmt.Errorf("unaligned read offset=%d length=%d", offset, length)
	}
	if length <= 0 {
		return nil, nil
	}
	buf := make([]byte, length)
	startSector := uint64(offset / sectorSize)
	numSectors := uint64(length / sectorSize)
	err := C.v2c_vddk_read(
		h.ptr,
		C.uint64_t(startSector),
		C.uint64_t(numSectors),
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
	)
	if err != 0 {
		return nil, fmt.Errorf("VixDiskLib_Read failed at offset=%d length=%d: %s", offset, length, vixErrorText(err))
	}
	return buf, nil
}

func (h *vddkHandle) capacityBytes() (int64, error) {
	var sectors C.uint64_t
	err := C.v2c_vddk_get_capacity(h.ptr, &sectors)
	if err != 0 {
		return 0, fmt.Errorf("VixDiskLib_GetInfo failed: %s", vixErrorText(err))
	}
	capBytes := int64(sectors) * sectorSize
	if capBytes <= 0 {
		return 0, errors.New("invalid source disk capacity from VDDK")
	}
	return capBytes, nil
}

func vixErrorText(vixErr C.VixError) string {
	txt := C.VixDiskLib_GetErrorText(vixErr, nil)
	if txt == nil {
		return fmt.Sprintf("VixError(%d)", uint64(vixErr))
	}
	defer C.VixDiskLib_FreeErrorText(txt)
	return C.GoString(txt)
}

type nbdClient struct {
	conn   net.Conn
	handle uint64
	mu     sync.Mutex
}

func dialNBDUnix(path string) (*nbdClient, error) {
	c, err := net.DialTimeout("unix", path, 10*time.Second)
	if err != nil {
		return nil, err
	}
	client := &nbdClient{conn: c}
	if err := client.handshake(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return client, nil
}

func (c *nbdClient) handshake() error {
	var srvMagic uint64
	var optMagic uint64
	var hsFlags uint16
	if err := binary.Read(c.conn, binary.BigEndian, &srvMagic); err != nil {
		return err
	}
	if err := binary.Read(c.conn, binary.BigEndian, &optMagic); err != nil {
		return err
	}
	if err := binary.Read(c.conn, binary.BigEndian, &hsFlags); err != nil {
		return err
	}
	if srvMagic != nbdMagic || optMagic != nbdOptMagic {
		return fmt.Errorf("invalid nbd handshake magic")
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint32(nbdClientFixedNewstyle)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint64(nbdOptMagic)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint32(nbdOptExportName)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint32(0)); err != nil {
		return err
	}

	var exportSize uint64
	var transFlags uint16
	if err := binary.Read(c.conn, binary.BigEndian, &exportSize); err != nil {
		return err
	}
	if err := binary.Read(c.conn, binary.BigEndian, &transFlags); err != nil {
		return err
	}
	padding := make([]byte, 124)
	if _, err := io.ReadFull(c.conn, padding); err != nil {
		return err
	}
	return nil
}

func (c *nbdClient) writeAt(offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if offset < 0 {
		return fmt.Errorf("negative write offset: %d", offset)
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.handle++
	h := c.handle

	if err := binary.Write(c.conn, binary.BigEndian, uint32(nbdRequestMagic)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint16(0)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint16(nbdCmdWrite)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, h); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint64(offset)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := c.conn.Write(data); err != nil {
		return err
	}

	var replyMagic uint32
	var replyErr uint32
	var replyHandle uint64
	if err := binary.Read(c.conn, binary.BigEndian, &replyMagic); err != nil {
		return err
	}
	if err := binary.Read(c.conn, binary.BigEndian, &replyErr); err != nil {
		return err
	}
	if err := binary.Read(c.conn, binary.BigEndian, &replyHandle); err != nil {
		return err
	}
	if replyMagic != nbdReplyMagic {
		return fmt.Errorf("invalid nbd reply magic")
	}
	if replyHandle != h {
		return fmt.Errorf("nbd handle mismatch: got=%d want=%d", replyHandle, h)
	}
	if replyErr != 0 {
		return fmt.Errorf("nbd server returned error=%d", replyErr)
	}
	return nil
}

func (c *nbdClient) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	_ = binary.Write(c.conn, binary.BigEndian, uint32(nbdRequestMagic))
	_ = binary.Write(c.conn, binary.BigEndian, uint16(0))
	_ = binary.Write(c.conn, binary.BigEndian, uint16(nbdCmdDisc))
	_ = binary.Write(c.conn, binary.BigEndian, uint64(0))
	_ = binary.Write(c.conn, binary.BigEndian, uint64(0))
	_ = binary.Write(c.conn, binary.BigEndian, uint32(0))
	err := c.conn.Close()
	c.conn = nil
	return err
}

type qcow2Endpoint struct {
	path string
	sock string
	cmd  *exec.Cmd
}

func startQcow2Endpoint(path string) (*qcow2Endpoint, error) {
	sock := filepath.Join(os.TempDir(), fmt.Sprintf("v2c_qcow_%d_%d.sock", os.Getpid(), time.Now().UnixNano()))
	_ = os.Remove(sock)
	cmd := exec.Command("qemu-nbd", "--socket", sock, "--format", "qcow2", path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start qemu-nbd: %w", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			return nil, errors.New("qemu-nbd socket not ready before timeout")
		}
		if _, err := os.Stat(sock); err == nil {
			return &qcow2Endpoint{path: path, sock: sock, cmd: cmd}, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *qcow2Endpoint) close() {
	if e == nil {
		return
	}
	if e.cmd != nil && e.cmd.Process != nil {
		_ = e.cmd.Process.Kill()
		_, _ = e.cmd.Process.Wait()
	}
	if e.sock != "" {
		_ = os.Remove(e.sock)
	}
}

func createSparseQCOW2(path string, sizeBytes int64) error {
	if sizeBytes <= 0 {
		return fmt.Errorf("invalid qcow2 size: %d", sizeBytes)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	_ = os.Remove(path)
	cmd := exec.Command(
		"qemu-img", "create",
		"-f", "qcow2",
		"-o", "compat=1.1,lazy_refcounts=on",
		path,
		fmt.Sprintf("%d", sizeBytes),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runVirtV2VInPlace(path string, virtioISO string) error {
	v2vPath, err := exec.LookPath("virt-v2v-in-place")
	if err != nil {
		return fmt.Errorf("virt-v2v-in-place not found: %w", err)
	}
	args := []string{"-i", "disk", path}
	if virtioISO != "" {
		args = append(args, "--inject-virtio-win", virtioISO)
	}
	cmd := exec.Command(v2vPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

type readMetric struct {
	Sequential bool
	Latency    time.Duration
	Bytes      int
}

type adaptiveSizer struct {
	minChunk    int
	maxChunk    int
	curChunk    int
	fastLatency time.Duration
	slowLatency time.Duration
	fastMBps    float64
	slowMBps    float64
	fastStreak  int
	slowStreak  int
	mu          sync.Mutex
}

func newAdaptiveSizer(minChunk, maxChunk int, fastLatency, slowLatency time.Duration, fastMBps, slowMBps float64) *adaptiveSizer {
	if minChunk < sectorSize {
		minChunk = sectorSize
	}
	if maxChunk < minChunk {
		maxChunk = minChunk
	}
	return &adaptiveSizer{
		minChunk:    minChunk,
		maxChunk:    maxChunk,
		curChunk:    minChunk,
		fastLatency: fastLatency,
		slowLatency: slowLatency,
		fastMBps:    fastMBps,
		slowMBps:    slowMBps,
	}
}

func (a *adaptiveSizer) current() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.curChunk
}

func (a *adaptiveSizer) observe(m readMetric) {
	if m.Bytes <= 0 || m.Latency <= 0 {
		return
	}
	mbps := (float64(m.Bytes) / (1024.0 * 1024.0)) / m.Latency.Seconds()
	fast := m.Sequential && m.Latency <= a.fastLatency && mbps >= a.fastMBps
	slow := m.Latency >= a.slowLatency || mbps <= a.slowMBps

	a.mu.Lock()
	defer a.mu.Unlock()
	if fast {
		a.fastStreak++
		if a.slowStreak > 0 {
			a.slowStreak--
		}
	} else if slow {
		a.slowStreak++
		if a.fastStreak > 0 {
			a.fastStreak--
		}
	}

	if a.fastStreak >= 8 && a.curChunk < a.maxChunk {
		a.curChunk = int(math.Min(float64(a.curChunk*2), float64(a.maxChunk)))
		a.fastStreak = 0
	}
	if a.slowStreak >= 3 && a.curChunk > a.minChunk {
		a.curChunk = int(math.Max(float64(a.curChunk/2), float64(a.minChunk)))
		a.slowStreak = 0
	}
}

type block struct {
	Offset int64
	Length int
}

type blockData struct {
	Offset int64
	Data   []byte
}

type copyStats struct {
	BytesRead        int64  `json:"bytes_read"`
	BytesWritten     int64  `json:"bytes_written"`
	BytesZeroSkipped int64  `json:"bytes_zero_skipped"`
	ElapsedSec       int64  `json:"elapsed_sec"`
	Mode             string `json:"mode"`
}

type engineSpec struct {
	VDDK struct {
		LibDir     string `yaml:"libdir"`
		Server     string `yaml:"server"`
		User       string `yaml:"user"`
		Password   string `yaml:"password"`
		Thumbprint string `yaml:"thumbprint"`
		VMMoref    string `yaml:"vm_moref"`
	} `yaml:"vddk"`
	BaseCopy struct {
		SnapshotMoref string  `yaml:"snapshot_moref"`
		DiskPath      string  `yaml:"disk_path"`
		TargetQCOW2   string  `yaml:"target_qcow2"`
		DiskSizeBytes int64   `yaml:"disk_size_bytes"`
		Readers       int     `yaml:"readers"`
		QueueDepth    int     `yaml:"queue_depth"`
		MinChunkMB    int     `yaml:"min_chunk_mb"`
		MaxChunkMB    int     `yaml:"max_chunk_mb"`
		FastLatencyMS int     `yaml:"fast_latency_ms"`
		SlowLatencyMS int     `yaml:"slow_latency_ms"`
		FastMBps      float64 `yaml:"fast_mbps"`
		SlowMBps      float64 `yaml:"slow_mbps"`
		RunVirtV2V    *bool   `yaml:"run_virt_v2v"`
		VirtioISO     string  `yaml:"virtio_iso"`
	} `yaml:"base_copy"`
	DeltaSync struct {
		SnapshotMoref string `yaml:"snapshot_moref"`
		DiskPath      string `yaml:"disk_path"`
		TargetQCOW2   string `yaml:"target_qcow2"`
		RangesFile    string `yaml:"ranges_file"`
		Readers       int    `yaml:"readers"`
		QueueDepth    int    `yaml:"queue_depth"`
		ChunkMB       int    `yaml:"chunk_mb"`
	} `yaml:"delta_sync"`
}

type baseCopyOptions struct {
	VDDK             vddkConnCfg
	DiskPath         string
	TargetQCOW2      string
	DiskSizeBytes    int64
	Readers          int
	QueueDepth       int
	MinChunkMB       int
	MaxChunkMB       int
	FastLatencyMS    int
	SlowLatencyMS    int
	FastMBps         float64
	SlowMBps         float64
	RunVirtV2V       bool
	VirtioISO        string
}

func extractSpecPath(args []string) string {
	for i, a := range args {
		if a == "--spec" || a == "-spec" {
			if i+1 < len(args) {
				return args[i+1]
			}
		}
		if strings.HasPrefix(a, "--spec=") {
			return strings.TrimSpace(strings.TrimPrefix(a, "--spec="))
		}
		if strings.HasPrefix(a, "-spec=") {
			return strings.TrimSpace(strings.TrimPrefix(a, "-spec="))
		}
	}
	return ""
}

func loadEngineSpec(path string) (*engineSpec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var spec engineSpec
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("parse spec yaml: %w", err)
	}
	return &spec, nil
}

func specToBaseCopyOptions(spec *engineSpec) baseCopyOptions {
	o := baseCopyOptions{}
	o.VDDK.LibDir = spec.VDDK.LibDir
	o.VDDK.Server = spec.VDDK.Server
	o.VDDK.User = spec.VDDK.User
	o.VDDK.Password = spec.VDDK.Password
	o.VDDK.Thumbprint = spec.VDDK.Thumbprint
	o.VDDK.VMMoref = spec.VDDK.VMMoref
	o.VDDK.SnapshotMoref = spec.BaseCopy.SnapshotMoref
	o.DiskPath = spec.BaseCopy.DiskPath
	o.TargetQCOW2 = spec.BaseCopy.TargetQCOW2
	o.DiskSizeBytes = spec.BaseCopy.DiskSizeBytes
	o.Readers = spec.BaseCopy.Readers
	o.QueueDepth = spec.BaseCopy.QueueDepth
	o.MinChunkMB = spec.BaseCopy.MinChunkMB
	o.MaxChunkMB = spec.BaseCopy.MaxChunkMB
	o.FastLatencyMS = spec.BaseCopy.FastLatencyMS
	o.SlowLatencyMS = spec.BaseCopy.SlowLatencyMS
	o.FastMBps = spec.BaseCopy.FastMBps
	o.SlowMBps = spec.BaseCopy.SlowMBps
	if spec.BaseCopy.RunVirtV2V == nil {
		o.RunVirtV2V = true
	} else {
		o.RunVirtV2V = *spec.BaseCopy.RunVirtV2V
	}
	o.VirtioISO = spec.BaseCopy.VirtioISO
	return o
}

func detectSourceDiskSizeBytes(cfg vddkConnCfg, diskPath string) (int64, error) {
	conn, err := connectVDDK(cfg)
	if err != nil {
		return 0, err
	}
	defer conn.close()
	handle, err := conn.open(diskPath)
	if err != nil {
		return 0, err
	}
	defer handle.close()
	return handle.capacityBytes()
}

func (o *baseCopyOptions) normalize() {
	if o.Readers <= 0 {
		o.Readers = 4
	}
	if o.QueueDepth <= 0 {
		o.QueueDepth = 64
	}
	if o.MinChunkMB <= 0 {
		o.MinChunkMB = 1
	}
	if o.MaxChunkMB <= 0 {
		o.MaxChunkMB = 4
	}
	if o.MaxChunkMB < o.MinChunkMB {
		o.MaxChunkMB = o.MinChunkMB
	}
	if o.FastLatencyMS <= 0 {
		o.FastLatencyMS = 40
	}
	if o.SlowLatencyMS <= 0 {
		o.SlowLatencyMS = 250
	}
	if o.FastMBps <= 0 {
		o.FastMBps = 180
	}
	if o.SlowMBps <= 0 {
		o.SlowMBps = 40
	}
}

func runBaseCopy(ctx context.Context, opts baseCopyOptions) (copyStats, error) {
	opts.normalize()
	start := time.Now()

	sourceDiskBytes, err := detectSourceDiskSizeBytes(opts.VDDK, opts.DiskPath)
	if err != nil {
		return copyStats{}, fmt.Errorf("failed to detect source disk size: %w", err)
	}
	if opts.DiskSizeBytes <= 0 {
		opts.DiskSizeBytes = sourceDiskBytes
		fmt.Fprintf(os.Stderr, "[base-copy] auto-detected source disk size=%d bytes\n", opts.DiskSizeBytes)
	} else if opts.DiskSizeBytes != sourceDiskBytes {
		fmt.Fprintf(
			os.Stderr,
			"[base-copy] disk-size-bytes mismatch requested=%d detected=%d, using detected size\n",
			opts.DiskSizeBytes,
			sourceDiskBytes,
		)
		opts.DiskSizeBytes = sourceDiskBytes
	}

	if err := createSparseQCOW2(opts.TargetQCOW2, opts.DiskSizeBytes); err != nil {
		return copyStats{}, err
	}
	endpoint, err := startQcow2Endpoint(opts.TargetQCOW2)
	if err != nil {
		return copyStats{}, err
	}
	defer endpoint.close()

	writerClient, err := dialNBDUnix(endpoint.sock)
	if err != nil {
		return copyStats{}, fmt.Errorf("nbd writer connect failed: %w", err)
	}
	defer writerClient.close()

	readQ := make(chan block, opts.QueueDepth)
	writeQ := make(chan blockData, opts.QueueDepth)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sizer := newAdaptiveSizer(
		alignUp(opts.MinChunkMB*1024*1024, sectorSize),
		alignUp(opts.MaxChunkMB*1024*1024, sectorSize),
		time.Duration(opts.FastLatencyMS)*time.Millisecond,
		time.Duration(opts.SlowLatencyMS)*time.Millisecond,
		opts.FastMBps,
		opts.SlowMBps,
	)

	var bytesRead int64
	var bytesWritten int64
	var bytesZero int64
	var setErr sync.Once
	pushErr := func(e error) {
		setErr.Do(func() {
			errCh <- e
			cancel()
		})
	}

	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for item := range writeQ {
			if err := writerClient.writeAt(item.Offset, item.Data); err != nil {
				pushErr(fmt.Errorf("qcow2 write failed at offset=%d: %w", item.Offset, err))
				return
			}
			atomic.AddInt64(&bytesWritten, int64(len(item.Data)))
		}
	}()

	go func() {
		defer close(readQ)
		var offset int64
		for offset < opts.DiskSizeBytes {
			select {
			case <-ctx.Done():
				return
			default:
			}
			chunk := sizer.current()
			remaining := opts.DiskSizeBytes - offset
			if int64(chunk) > remaining {
				chunk = alignDown(int(remaining), sectorSize)
				if chunk <= 0 {
					chunk = sectorSize
				}
			}
			select {
			case <-ctx.Done():
				return
			case readQ <- block{Offset: offset, Length: chunk}:
			}
			offset += int64(chunk)
		}
	}()

	var workerWG sync.WaitGroup
	for i := 0; i < opts.Readers; i++ {
		workerWG.Add(1)
		go func(id int) {
			defer workerWG.Done()
			conn, err := connectVDDK(opts.VDDK)
			if err != nil {
				pushErr(fmt.Errorf("reader %d connect failed: %w", id, err))
				return
			}
			defer conn.close()
			handle, err := conn.open(opts.DiskPath)
			if err != nil {
				pushErr(fmt.Errorf("reader %d open failed: %w", id, err))
				return
			}
			defer handle.close()

			prevEnd := int64(-1)
			for task := range readQ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				t0 := time.Now()
				buf, err := handle.readAt(task.Offset, task.Length)
				if err != nil {
					pushErr(fmt.Errorf("reader %d read failed: %w", id, err))
					return
				}
				lat := time.Since(t0)
				seq := task.Offset == prevEnd
				prevEnd = task.Offset + int64(len(buf))
				sizer.observe(readMetric{Sequential: seq, Latency: lat, Bytes: len(buf)})
				atomic.AddInt64(&bytesRead, int64(len(buf)))

				if isAllZero(buf) {
					atomic.AddInt64(&bytesZero, int64(len(buf)))
					continue
				}
				data := make([]byte, len(buf))
				copy(data, buf)
				select {
				case <-ctx.Done():
					return
				case writeQ <- blockData{Offset: task.Offset, Data: data}:
				}
			}
		}(i + 1)
	}

	workerWG.Wait()
	close(writeQ)
	writerWG.Wait()

	select {
	case e := <-errCh:
		return copyStats{}, e
	default:
	}

	if opts.RunVirtV2V {
		if err := runVirtV2VInPlace(opts.TargetQCOW2, opts.VirtioISO); err != nil {
			return copyStats{}, fmt.Errorf("virt-v2v-in-place failed: %w", err)
		}
	}

	return copyStats{
		BytesRead:        atomic.LoadInt64(&bytesRead),
		BytesWritten:     atomic.LoadInt64(&bytesWritten),
		BytesZeroSkipped: atomic.LoadInt64(&bytesZero),
		ElapsedSec:       int64(time.Since(start).Seconds()),
		Mode:             "base_copy",
	}, nil
}

type changedRange struct {
	Start  int64 `json:"start"`
	Length int64 `json:"length"`
}

type deltaSyncOptions struct {
	VDDK             vddkConnCfg
	DiskPath         string
	TargetQCOW2      string
	RangesFile       string
	Readers          int
	QueueDepth       int
	ChunkMB          int
}

func specToDeltaSyncOptions(spec *engineSpec) deltaSyncOptions {
	o := deltaSyncOptions{}
	o.VDDK.LibDir = spec.VDDK.LibDir
	o.VDDK.Server = spec.VDDK.Server
	o.VDDK.User = spec.VDDK.User
	o.VDDK.Password = spec.VDDK.Password
	o.VDDK.Thumbprint = spec.VDDK.Thumbprint
	o.VDDK.VMMoref = spec.VDDK.VMMoref
	o.VDDK.SnapshotMoref = spec.DeltaSync.SnapshotMoref
	o.DiskPath = spec.DeltaSync.DiskPath
	o.TargetQCOW2 = spec.DeltaSync.TargetQCOW2
	o.RangesFile = spec.DeltaSync.RangesFile
	o.Readers = spec.DeltaSync.Readers
	o.QueueDepth = spec.DeltaSync.QueueDepth
	o.ChunkMB = spec.DeltaSync.ChunkMB
	return o
}

func (o *deltaSyncOptions) normalize() {
	if o.Readers <= 0 {
		o.Readers = 4
	}
	if o.QueueDepth <= 0 {
		o.QueueDepth = 64
	}
	if o.ChunkMB <= 0 {
		o.ChunkMB = 4
	}
}

func loadRanges(path string) ([]changedRange, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ranges []changedRange
	if err := json.Unmarshal(raw, &ranges); err != nil {
		return nil, err
	}
	return ranges, nil
}

func runDeltaSync(ctx context.Context, opts deltaSyncOptions) (copyStats, error) {
	opts.normalize()
	start := time.Now()

	ranges, err := loadRanges(opts.RangesFile)
	if err != nil {
		return copyStats{}, err
	}
	endpoint, err := startQcow2Endpoint(opts.TargetQCOW2)
	if err != nil {
		return copyStats{}, err
	}
	defer endpoint.close()

	writerClient, err := dialNBDUnix(endpoint.sock)
	if err != nil {
		return copyStats{}, fmt.Errorf("nbd writer connect failed: %w", err)
	}
	defer writerClient.close()

	readQ := make(chan block, opts.QueueDepth)
	writeQ := make(chan blockData, opts.QueueDepth)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var bytesRead int64
	var bytesWritten int64
	var setErr sync.Once
	pushErr := func(e error) {
		setErr.Do(func() {
			errCh <- e
			cancel()
		})
	}

	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		for item := range writeQ {
			if err := writerClient.writeAt(item.Offset, item.Data); err != nil {
				pushErr(fmt.Errorf("delta write failed at offset=%d: %w", item.Offset, err))
				return
			}
			atomic.AddInt64(&bytesWritten, int64(len(item.Data)))
		}
	}()

	go func() {
		defer close(readQ)
		chunk := alignUp(opts.ChunkMB*1024*1024, sectorSize)
		for _, r := range ranges {
			offset := alignDown(int(r.Start), sectorSize)
			end := r.Start + r.Length
			for int64(offset) < end {
				select {
				case <-ctx.Done():
					return
				default:
				}
				remain := end - int64(offset)
				l := chunk
				if int64(l) > remain {
					l = alignDown(int(remain), sectorSize)
					if l <= 0 {
						l = sectorSize
					}
				}
				select {
				case <-ctx.Done():
					return
				case readQ <- block{Offset: int64(offset), Length: l}:
				}
				offset += l
			}
		}
	}()

	var workerWG sync.WaitGroup
	for i := 0; i < opts.Readers; i++ {
		workerWG.Add(1)
		go func(id int) {
			defer workerWG.Done()
			conn, err := connectVDDK(opts.VDDK)
			if err != nil {
				pushErr(fmt.Errorf("delta reader %d connect failed: %w", id, err))
				return
			}
			defer conn.close()
			handle, err := conn.open(opts.DiskPath)
			if err != nil {
				pushErr(fmt.Errorf("delta reader %d open failed: %w", id, err))
				return
			}
			defer handle.close()

			for task := range readQ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				buf, err := handle.readAt(task.Offset, task.Length)
				if err != nil {
					pushErr(fmt.Errorf("delta reader %d read failed: %w", id, err))
					return
				}
				atomic.AddInt64(&bytesRead, int64(len(buf)))
				data := make([]byte, len(buf))
				copy(data, buf)
				select {
				case <-ctx.Done():
					return
				case writeQ <- blockData{Offset: task.Offset, Data: data}:
				}
			}
		}(i + 1)
	}

	workerWG.Wait()
	close(writeQ)
	writerWG.Wait()

	select {
	case e := <-errCh:
		return copyStats{}, e
	default:
	}

	return copyStats{
		BytesRead:    atomic.LoadInt64(&bytesRead),
		BytesWritten: atomic.LoadInt64(&bytesWritten),
		ElapsedSec:   int64(time.Since(start).Seconds()),
		Mode:         "delta_sync",
	}, nil
}

func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func alignUp(v, align int) int {
	if align <= 0 {
		return v
	}
	rem := v % align
	if rem == 0 {
		return v
	}
	return v + (align - rem)
}

func alignDown(v, align int) int {
	if align <= 0 {
		return v
	}
	return v - (v % align)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "base-copy":
		if err := cmdBaseCopy(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "base-copy failed: %v\n", err)
			os.Exit(1)
		}
	case "delta-sync":
		if err := cmdDeltaSync(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "delta-sync failed: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  v2c-engine base-copy  [flags]\n")
	fmt.Fprintf(os.Stderr, "  v2c-engine delta-sync [flags]\n")
	fmt.Fprintf(os.Stderr, "  v2c-engine base-copy --spec /path/spec.yaml\n")
	fmt.Fprintf(os.Stderr, "  v2c-engine delta-sync --spec /path/spec.yaml\n")
}

func cmdBaseCopy(args []string) error {
	specPath := extractSpecPath(args)
	var o baseCopyOptions
	o.RunVirtV2V = true
	o.VirtioISO = "/usr/share/virtio-win/virtio-win.iso"
	if specPath != "" {
		spec, err := loadEngineSpec(specPath)
		if err != nil {
			return err
		}
		o = specToBaseCopyOptions(spec)
		if o.VirtioISO == "" {
			o.VirtioISO = "/usr/share/virtio-win/virtio-win.iso"
		}
	}

	fs := flag.NewFlagSet("base-copy", flag.ContinueOnError)
	fs.StringVar(&specPath, "spec", specPath, "YAML spec file path")
	fs.StringVar(&o.VDDK.LibDir, "vddk-libdir", o.VDDK.LibDir, "VDDK install root")
	fs.StringVar(&o.VDDK.Server, "server", o.VDDK.Server, "vCenter/ESXi hostname")
	fs.StringVar(&o.VDDK.User, "user", o.VDDK.User, "vCenter username")
	fs.StringVar(&o.VDDK.Password, "password", o.VDDK.Password, "vCenter password")
	fs.StringVar(&o.VDDK.Thumbprint, "thumbprint", o.VDDK.Thumbprint, "SSL thumbprint")
	fs.StringVar(&o.VDDK.VMMoref, "vm-moref", o.VDDK.VMMoref, "VM MoRef (vm-XXX)")
	fs.StringVar(&o.VDDK.SnapshotMoref, "snapshot-moref", o.VDDK.SnapshotMoref, "Snapshot MoRef")
	fs.StringVar(&o.DiskPath, "disk-path", o.DiskPath, "Snapshot disk backing path")
	fs.StringVar(&o.TargetQCOW2, "target-qcow2", o.TargetQCOW2, "Destination QCOW2 path")
	fs.Int64Var(&o.DiskSizeBytes, "disk-size-bytes", o.DiskSizeBytes, "Disk capacity in bytes (optional, auto-detected when 0)")
	fs.IntVar(&o.Readers, "readers", o.Readers, "Number of parallel VDDK readers")
	fs.IntVar(&o.QueueDepth, "queue-depth", o.QueueDepth, "Queue depth for read/write channels")
	fs.IntVar(&o.MinChunkMB, "min-chunk-mb", o.MinChunkMB, "Adaptive minimum read chunk size in MB")
	fs.IntVar(&o.MaxChunkMB, "max-chunk-mb", o.MaxChunkMB, "Adaptive maximum read chunk size in MB")
	fs.IntVar(&o.FastLatencyMS, "fast-latency-ms", o.FastLatencyMS, "Fast latency threshold in ms")
	fs.IntVar(&o.SlowLatencyMS, "slow-latency-ms", o.SlowLatencyMS, "Slow latency threshold in ms")
	fs.Float64Var(&o.FastMBps, "fast-mbps", o.FastMBps, "Throughput considered fast")
	fs.Float64Var(&o.SlowMBps, "slow-mbps", o.SlowMBps, "Throughput considered slow")
	fs.BoolVar(&o.RunVirtV2V, "run-virt-v2v", o.RunVirtV2V, "Run virt-v2v-in-place after base copy")
	fs.StringVar(&o.VirtioISO, "virtio-iso", o.VirtioISO, "VirtIO ISO path for Windows injection")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if o.VirtioISO == "" {
		o.VirtioISO = "/usr/share/virtio-win/virtio-win.iso"
	}

	if o.VDDK.Password == "" {
		o.VDDK.Password = os.Getenv("VC_PASSWORD")
	}

	if err := validateBaseCopy(o); err != nil {
		return err
	}
	stats, err := runBaseCopy(context.Background(), o)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(stats)
}

func cmdDeltaSync(args []string) error {
	specPath := extractSpecPath(args)
	var o deltaSyncOptions
	if specPath != "" {
		spec, err := loadEngineSpec(specPath)
		if err != nil {
			return err
		}
		o = specToDeltaSyncOptions(spec)
	}

	fs := flag.NewFlagSet("delta-sync", flag.ContinueOnError)
	fs.StringVar(&specPath, "spec", specPath, "YAML spec file path")
	fs.StringVar(&o.VDDK.LibDir, "vddk-libdir", o.VDDK.LibDir, "VDDK install root")
	fs.StringVar(&o.VDDK.Server, "server", o.VDDK.Server, "vCenter/ESXi hostname")
	fs.StringVar(&o.VDDK.User, "user", o.VDDK.User, "vCenter username")
	fs.StringVar(&o.VDDK.Password, "password", o.VDDK.Password, "vCenter password")
	fs.StringVar(&o.VDDK.Thumbprint, "thumbprint", o.VDDK.Thumbprint, "SSL thumbprint")
	fs.StringVar(&o.VDDK.VMMoref, "vm-moref", o.VDDK.VMMoref, "VM MoRef (vm-XXX)")
	fs.StringVar(&o.VDDK.SnapshotMoref, "snapshot-moref", o.VDDK.SnapshotMoref, "Snapshot MoRef")
	fs.StringVar(&o.DiskPath, "disk-path", o.DiskPath, "Snapshot disk backing path")
	fs.StringVar(&o.TargetQCOW2, "target-qcow2", o.TargetQCOW2, "Destination QCOW2 path")
	fs.StringVar(&o.RangesFile, "ranges-file", o.RangesFile, "JSON file with CBT ranges")
	fs.IntVar(&o.Readers, "readers", o.Readers, "Number of parallel VDDK readers")
	fs.IntVar(&o.QueueDepth, "queue-depth", o.QueueDepth, "Queue depth for read/write channels")
	fs.IntVar(&o.ChunkMB, "chunk-mb", o.ChunkMB, "Chunk size per delta read in MB")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.VDDK.Password == "" {
		o.VDDK.Password = os.Getenv("VC_PASSWORD")
	}
	if err := validateDeltaSync(o); err != nil {
		return err
	}
	stats, err := runDeltaSync(context.Background(), o)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(stats)
}

func validateBaseCopy(o baseCopyOptions) error {
	if o.VDDK.LibDir == "" || o.VDDK.Server == "" || o.VDDK.User == "" || o.VDDK.Thumbprint == "" ||
		o.VDDK.VMMoref == "" || o.VDDK.SnapshotMoref == "" || o.DiskPath == "" || o.TargetQCOW2 == "" {
		return errors.New("missing required base-copy flags")
	}
	if o.DiskSizeBytes < 0 {
		return errors.New("disk-size-bytes must be >= 0")
	}
	if o.VDDK.Password == "" {
		return errors.New("password is required (flag or VC_PASSWORD env)")
	}
	return nil
}

func validateDeltaSync(o deltaSyncOptions) error {
	if o.VDDK.LibDir == "" || o.VDDK.Server == "" || o.VDDK.User == "" || o.VDDK.Thumbprint == "" ||
		o.VDDK.VMMoref == "" || o.VDDK.SnapshotMoref == "" || o.DiskPath == "" || o.TargetQCOW2 == "" || o.RangesFile == "" {
		return errors.New("missing required delta-sync flags")
	}
	if o.VDDK.Password == "" {
		return errors.New("password is required (flag or VC_PASSWORD env)")
	}
	return nil
}
