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

static VixError v2c_vddk_query_allocated_blocks(
    VixDiskLibHandle handle,
    uint64_t startSector,
    uint64_t numSectors,
    uint64_t chunkSize,
    uint64_t **offsets,
    uint64_t **lengths,
    uint32_t *count
) {
    VixDiskLibBlockList *blockList = NULL;
    VixError err = VixDiskLib_QueryAllocatedBlocks(
        handle,
        (VixDiskLibSectorType)startSector,
        (VixDiskLibSectorType)numSectors,
        (VixDiskLibSectorType)chunkSize,
        &blockList
    );
    if (err != VIX_OK) {
        return err;
    }

    *offsets = NULL;
    *lengths = NULL;
    *count = 0;

    if (blockList == NULL || blockList->numBlocks == 0) {
        if (blockList != NULL) {
            VixDiskLib_FreeBlockList(blockList);
        }
        return VIX_OK;
    }

    uint32_t n = blockList->numBlocks;
    uint64_t *offs = (uint64_t *)malloc(sizeof(uint64_t) * n);
    uint64_t *lens = (uint64_t *)malloc(sizeof(uint64_t) * n);
    if (offs == NULL || lens == NULL) {
        if (offs != NULL) free(offs);
        if (lens != NULL) free(lens);
        VixDiskLib_FreeBlockList(blockList);
        return (VixError)1;
    }

    for (uint32_t i = 0; i < n; ++i) {
        offs[i] = (uint64_t)blockList->blocks[i].offset;
        lens[i] = (uint64_t)blockList->blocks[i].length;
    }

    VixDiskLib_FreeBlockList(blockList);
    *offsets = offs;
    *lengths = lens;
    *count = n;
    return VIX_OK;
}
*/
import "C"

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/tls"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/vmware/govmomi"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/property"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/methods"
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
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
	nbdCmdFlush = 3

	nbdFlagSendFlush = 1 << 2
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
	ptr     C.VixDiskLibHandle
	release func()
}

type allocatedExtent struct {
	Offset int64
	Length int64
}

var (
	vddkInitOnce sync.Once
	vddkInitErr  error

	vddkOpenLimiterOnce sync.Once
	vddkOpenLimiter     chan struct{}

	hostOSReleaseOnce sync.Once
	hostOSRelease     map[string]string
)

func vddkMaxOpenHandles() int {
	if raw := strings.TrimSpace(os.Getenv("V2C_VDDK_MAX_OPEN")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			if n > 256 {
				n = 256
			}
			return n
		}
	}
	return 8
}

func getVDDKOpenLimiter() chan struct{} {
	vddkOpenLimiterOnce.Do(func() {
		vddkOpenLimiter = make(chan struct{}, vddkMaxOpenHandles())
	})
	return vddkOpenLimiter
}

func acquireVDDKOpenSlot() func() {
	limiter := getVDDKOpenLimiter()
	limiter <- struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			<-limiter
		})
	}
}

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

func connectVDDKWithRetry(cfg vddkConnCfg, attempts int, baseDelay time.Duration) (*vddkConnection, error) {
	if attempts <= 0 {
		attempts = 1
	}
	if baseDelay <= 0 {
		baseDelay = 500 * time.Millisecond
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		conn, err := connectVDDK(cfg)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if i < attempts-1 {
			time.Sleep(baseDelay * time.Duration(i+1))
		}
	}
	return nil, lastErr
}

func (c *vddkConnection) close() {
	if c == nil || c.ptr == nil {
		return
	}
	C.VixDiskLib_Disconnect(c.ptr)
	c.ptr = nil
}

func (c *vddkConnection) open(path string) (*vddkHandle, error) {
	release := acquireVDDKOpenSlot()
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var h C.VixDiskLibHandle
	err := C.v2c_vddk_open(c.ptr, cPath, &h)
	if err != 0 {
		release()
		return nil, fmt.Errorf("VixDiskLib_Open failed: %s", vixErrorText(err))
	}
	return &vddkHandle{ptr: h, release: release}, nil
}

func openVDDKWithRetry(conn *vddkConnection, path string, attempts int, baseDelay time.Duration) (*vddkHandle, error) {
	if attempts <= 0 {
		attempts = 1
	}
	if baseDelay <= 0 {
		baseDelay = 500 * time.Millisecond
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		h, err := conn.open(path)
		if err == nil {
			return h, nil
		}
		lastErr = err
		if i < attempts-1 {
			time.Sleep(baseDelay * time.Duration(i+1))
		}
	}
	return nil, lastErr
}

func (h *vddkHandle) close() {
	if h == nil {
		return
	}
	if h.ptr != nil {
		C.VixDiskLib_Close(h.ptr)
		h.ptr = nil
	}
	if h.release != nil {
		h.release()
		h.release = nil
	}
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

func (h *vddkHandle) queryAllocatedBlocks(startSector, numSectors, chunkSectors uint64) ([]allocatedExtent, error) {
	if numSectors == 0 {
		return nil, nil
	}
	if chunkSectors == 0 {
		chunkSectors = 1
	}

	var cOffsets *C.uint64_t
	var cLengths *C.uint64_t
	var cCount C.uint32_t
	err := C.v2c_vddk_query_allocated_blocks(
		h.ptr,
		C.uint64_t(startSector),
		C.uint64_t(numSectors),
		C.uint64_t(chunkSectors),
		&cOffsets,
		&cLengths,
		&cCount,
	)
	if err != 0 {
		return nil, fmt.Errorf(
			"VixDiskLib_QueryAllocatedBlocks failed start=%d sectors=%d chunk=%d: %s",
			startSector,
			numSectors,
			chunkSectors,
			vixErrorText(err),
		)
	}
	defer func() {
		if cOffsets != nil {
			C.free(unsafe.Pointer(cOffsets))
		}
		if cLengths != nil {
			C.free(unsafe.Pointer(cLengths))
		}
	}()

	n := int(cCount)
	if n == 0 {
		return nil, nil
	}

	offsetSlice := unsafe.Slice((*uint64)(unsafe.Pointer(cOffsets)), n)
	lengthSlice := unsafe.Slice((*uint64)(unsafe.Pointer(cLengths)), n)
	out := make([]allocatedExtent, 0, n)
	for i := 0; i < n; i++ {
		if lengthSlice[i] == 0 {
			continue
		}
		offBytes := int64(offsetSlice[i]) * sectorSize
		lenBytes := int64(lengthSlice[i]) * sectorSize
		if offBytes < 0 || lenBytes <= 0 {
			continue
		}
		out = append(out, allocatedExtent{Offset: offBytes, Length: lenBytes})
	}
	return out, nil
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
	canFlush bool
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
	c.canFlush = (transFlags & nbdFlagSendFlush) != 0
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

func (c *nbdClient) flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("nbd connection closed")
	}
	if !c.canFlush {
		return nil
	}

	c.handle++
	h := c.handle

	if err := binary.Write(c.conn, binary.BigEndian, uint32(nbdRequestMagic)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint16(0)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint16(nbdCmdFlush)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, h); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint64(0)); err != nil {
		return err
	}
	if err := binary.Write(c.conn, binary.BigEndian, uint32(0)); err != nil {
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
		return fmt.Errorf("nbd flush returned error=%d", replyErr)
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
	cmd.Env = sanitizedChildEnv()
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
		_ = e.cmd.Process.Signal(syscall.SIGTERM)
		waitDone := make(chan struct{})
		go func() {
			_, _ = e.cmd.Process.Wait()
			close(waitDone)
		}()
		select {
		case <-waitDone:
		case <-time.After(5 * time.Second):
			_ = e.cmd.Process.Kill()
			<-waitDone
		}
	}
	if e.sock != "" {
		_ = os.Remove(e.sock)
	}
}

func sanitizedChildEnv() []string {
	env := os.Environ()
	out := make([]string, 0, len(env))
	for _, kv := range env {
		switch {
		case strings.HasPrefix(kv, "LD_LIBRARY_PATH="):
			raw := strings.TrimPrefix(kv, "LD_LIBRARY_PATH=")
			parts := filepath.SplitList(raw)
			kept := make([]string, 0, len(parts))
			for _, part := range parts {
				p := strings.ToLower(strings.TrimSpace(part))
				if p == "" {
					continue
				}
				if strings.Contains(p, "vmware-vddk") || strings.Contains(p, "vmware-vix-disklib-distrib") {
					continue
				}
				kept = append(kept, part)
			}
			if len(kept) > 0 {
				out = append(out, "LD_LIBRARY_PATH="+strings.Join(kept, string(os.PathListSeparator)))
			}
		case strings.HasPrefix(kv, "CGO_CFLAGS="),
			strings.HasPrefix(kv, "CGO_LDFLAGS="),
			strings.HasPrefix(kv, "CGO_ENABLED="),
			strings.HasPrefix(kv, "LIBVIRT_DEFAULT_URI="):
			continue
		default:
			out = append(out, kv)
		}
	}
	return out
}

func setEnvKV(env []string, key string, value string) []string {
	prefix := key + "="
	for i, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}

func guestfsChildEnv() []string {
	env := sanitizedChildEnv()
	// Avoid libvirt socket dependency on hosts without virtqemud.
	env = setEnvKV(env, "LIBGUESTFS_BACKEND", "direct")
	return env
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
	cmd.Env = sanitizedChildEnv()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runVirtV2VInPlace(paths []string, virtioISO string) error {
	if len(paths) == 0 {
		return errors.New("no disk paths provided to virt-v2v-in-place")
	}

	if err := verifyImageBeforeV2V(paths[0], len(paths)); err != nil {
		return fmt.Errorf("pre-v2v integrity check failed: %w", err)
	}

	v2vPath, err := resolveVirtV2VInPlacePath()
	if err != nil {
		return err
	}

	runCmd := func(args []string) (string, error) {
		cmd := exec.Command(v2vPath, args...)
		cmd.Env = guestfsChildEnv()
		var buf bytes.Buffer
		cmd.Stdout = io.MultiWriter(os.Stdout, &buf)
		cmd.Stderr = io.MultiWriter(os.Stderr, &buf)
		err := cmd.Run()
		return buf.String(), err
	}

	baseArgs := append([]string{"-i", "disk"}, paths...)
	if strings.TrimSpace(virtioISO) == "" {
		_, err := runCmd(baseArgs)
		return err
	}

	withInject := append(append([]string{}, baseArgs...), "--inject-virtio-win", virtioISO)
	out, err := runCmd(withInject)
	if err == nil {
		return nil
	}

	msg := strings.ToLower(out)
	if strings.Contains(msg, "unrecognized option '--inject-virtio-win'") ||
		strings.Contains(msg, "unknown option '--inject-virtio-win'") {
		fmt.Fprintf(
			os.Stderr,
			"[virt-v2v] warning: --inject-virtio-win unsupported by this virt-v2v-in-place version, retrying without it\n",
		)
		_, retryErr := runCmd(baseArgs)
		return retryErr
	}
	return err
}

func resolveVirtV2VInPlacePath() (string, error) {
	// Optional explicit override for environments where PATH differs
	// between service shells and interactive shells.
	for _, envKey := range []string{"VIRT_V2V_IN_PLACE", "V2C_VIRT_V2V_BIN"} {
		if p := strings.TrimSpace(os.Getenv(envKey)); p != "" {
			if isExecutableFile(p) {
				return p, nil
			}
			return "", fmt.Errorf("%s is set but not executable: %s", envKey, p)
		}
	}

	if p, err := exec.LookPath("virt-v2v-in-place"); err == nil {
		return p, nil
	}

	for _, p := range []string{
		"/usr/libexec/virt-v2v-in-place",
		"/usr/bin/virt-v2v-in-place",
	} {
		if isExecutableFile(p) {
			return p, nil
		}
	}

	return "", fmt.Errorf(
		"virt-v2v-in-place not found in PATH and not present at known locations (/usr/libexec/virt-v2v-in-place, /usr/bin/virt-v2v-in-place)",
	)
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	if info.IsDir() {
		return false
	}
	return info.Mode()&0o111 != 0
}

func verifyImageBeforeV2V(path string, totalDisks int) error {
	run := func(name string, args ...string) error {
		cmd := exec.Command(name, args...)
		env := sanitizedChildEnv()
		if name == "virt-inspector" {
			env = guestfsChildEnv()
		}
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("%s %v failed: %w\n%s", name, args, err, string(out))
		}
		return nil
	}

	// Prefer explicit no-repair check when supported, but older qemu-img
	// releases do not accept "-r none".
	if err := run("qemu-img", "check", "-r", "none", path); err != nil {
		msg := strings.ToLower(err.Error())
		unsupportedNoRepair :=
			strings.Contains(msg, "unknown option value for -r") ||
				strings.Contains(msg, "invalid option") ||
				strings.Contains(msg, "unrecognized option")
		if unsupportedNoRepair {
			if err2 := run("qemu-img", "check", path); err2 != nil {
				return err2
			}
		} else {
			return err
		}
	}
	if err := run("virt-inspector", "-a", path); err != nil {
		msg := strings.ToLower(err.Error())
		osNotDetected := strings.Contains(msg, "inspection could not detect the source guest") ||
			strings.Contains(msg, "no root device found")
		if totalDisks > 1 && osNotDetected {
			log.Printf("[virt-v2v] warning: virt-inspector could not detect an OS on %s before multi-disk conversion; continuing with all disks attached", filepath.Base(path))
			return nil
		}
		return err
	}
	return nil
}

func buildVirtV2VDiskPaths(disks []vmDisk, diskStates map[string]*runDiskState, bootUnit int) ([]string, error) {
	if len(disks) == 0 {
		return nil, errors.New("no VM disks available for virt-v2v-in-place")
	}

	paths := make([]string, 0, len(disks))
	seenUnits := map[int]struct{}{}
	appendDisk := func(unit int) error {
		if _, ok := seenUnits[unit]; ok {
			return nil
		}
		ds := diskStates[strconv.Itoa(unit)]
		if ds == nil {
			return fmt.Errorf("disk state not found for unit=%d", unit)
		}
		path := strings.TrimSpace(ds.TargetQCOW2)
		if path == "" {
			return fmt.Errorf("target qcow2 path missing for unit=%d", unit)
		}
		paths = append(paths, path)
		seenUnits[unit] = struct{}{}
		return nil
	}

	if err := appendDisk(bootUnit); err != nil {
		return nil, err
	}
	for _, d := range disks {
		if err := appendDisk(d.Unit); err != nil {
			return nil, err
		}
	}
	return paths, nil
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

type appConfig struct {
	VCenter struct {
		Host     string `yaml:"host"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"vcenter"`
	Migration struct {
		VDDKPath        string `yaml:"vddk_path"`
		SnapshotQuiesce string `yaml:"snapshot_quiesce"`
		ShutdownMode    string `yaml:"shutdown_mode"`
		ParallelDisks   int    `yaml:"parallel_disks"`
		ParallelVMs     int    `yaml:"parallel_vms"`
		ControlDir      string `yaml:"control_dir"`
		SpecsDir        string `yaml:"specs_dir"`
		Workdir         string `yaml:"workdir"`
	} `yaml:"migration"`
	CloudStack struct {
		Endpoint  string `yaml:"endpoint"`
		APIKey    string `yaml:"api_key"`
		SecretKey string `yaml:"secret_key"`
	} `yaml:"cloudstack"`
	CloudStackDefaults struct {
		ZoneID            string `yaml:"zoneid"`
		ClusterID         string `yaml:"clusterid"`
		StorageID         string `yaml:"storageid"`
		NetworkID         string `yaml:"networkid"`
		ServiceOfferingID string `yaml:"serviceofferingid"`
		DiskOfferingID    string `yaml:"diskofferingid"`
		OSTypeID          string `yaml:"ostypeid"`
		BootType          string `yaml:"boottype"`
		BootMode          string `yaml:"bootmode"`
		RootDiskController string `yaml:"rootdiskcontroller"`
		NICAdapter        string `yaml:"nicadapter"`
	} `yaml:"cloudstack_defaults"`
	Virt struct {
		RunVirtV2V bool   `yaml:"run_virt_v2v"`
		VirtioISO  string `yaml:"virtio_iso"`
	} `yaml:"virt"`
}

type runSpec struct {
	VM struct {
		Name string `yaml:"name"`
	} `yaml:"vm"`
	Migration struct {
		DeltaInterval        int    `yaml:"delta_interval"`
		FinalizeDeltaInterval int   `yaml:"finalize_delta_interval"`
		FinalizeWindow       int    `yaml:"finalize_window"`
		FinalizeAt           string `yaml:"finalize_at"`
		Readers              int    `yaml:"readers"`
		RunVirtV2V           *bool  `yaml:"run_virt_v2v"`
		SnapshotQuiesce      string `yaml:"snapshot_quiesce"`
		ShutdownMode         string `yaml:"shutdown_mode"`
		ParallelDisks        int    `yaml:"parallel_disks"`
		StartVMAfterImport   bool   `yaml:"start_vm_after_import"`
	} `yaml:"migration"`
	Target struct {
		CloudStack cloudStackTargetSpec `yaml:"cloudstack"`
	} `yaml:"target"`
	Disks map[string]diskTargetSpec `yaml:"disks"`
}

type runSpecFile struct {
	VMs []runSpec `yaml:"vms"`
}

type diskTargetSpec struct {
	StorageID      string `yaml:"storageid"`
	DiskOfferingID string `yaml:"diskofferingid"`
}

type cloudStackTargetSpec struct {
	ZoneID            string `yaml:"zoneid"`
	ClusterID         string `yaml:"clusterid"`
	StorageID         string `yaml:"storageid"`
	NetworkID         string `yaml:"networkid"`
	ServiceOfferingID string `yaml:"serviceofferingid"`
	DiskOfferingID    string `yaml:"diskofferingid"`
	OSTypeID          string `yaml:"ostypeid"`
	BootType          string `yaml:"boottype"`
	BootMode          string `yaml:"bootmode"`
	RootDiskController string `yaml:"rootdiskcontroller"`
	NICAdapter        string `yaml:"nicadapter"`
	NICMappings       map[string]nicMappingSpec `yaml:"nic_mappings"`
}

type nicMappingSpec struct {
	SourceLabel     string `yaml:"source_label"`
	SourceNetwork   string `yaml:"source_network"`
	SourceMAC       string `yaml:"source_mac"`
	SourceDeviceKey int32  `yaml:"source_device_key"`
	SourceIndex     int    `yaml:"source_index"`
	NetworkID       string `yaml:"networkid"`
}

type nicAttachmentPlan struct {
	MapKey      string
	AdapterName string
	SourceIndex int
	NetworkID   string
}

type vmDisk struct {
	Key      int32
	Unit     int
	Capacity int64
	SourcePath string
}

type snapshotDiskMeta struct {
	Path     string
	ChangeID string
}

type runDiskState struct {
	Key            int32   `json:"key"`
	Unit           int     `json:"unit"`
	Capacity       int64   `json:"capacity"`
	TargetQCOW2    string  `json:"target_qcow2"`
	SourceDiskPath string  `json:"source_disk_path"`
	ChangeID       string  `json:"change_id"`
	StorageID      string  `json:"storage_id,omitempty"`
	DiskOfferingID string  `json:"disk_offering_id,omitempty"`
	VolumeID       string  `json:"volume_id,omitempty"`
	AttachedToVMID string  `json:"attached_to_vm_id,omitempty"`
	Stage          string  `json:"stage,omitempty"`
	Progress       float64 `json:"progress,omitempty"`
	QemuProgress   float64 `json:"qemu_progress,omitempty"`
	BytesRead      int64   `json:"bytes_read,omitempty"`
	BytesWritten   int64   `json:"bytes_written,omitempty"`
	BytesZero      int64   `json:"bytes_zero_skipped,omitempty"`
	CopiedBytes    int64   `json:"copied_bytes,omitempty"`
	SpeedMBps      float64 `json:"speed_mbps,omitempty"`
	EtaSeconds     int64   `json:"eta_seconds,omitempty"`
	BaseCopied     bool    `json:"base_copied,omitempty"`
	DeltaRounds    int64   `json:"delta_rounds,omitempty"`
}

type runState struct {
	VMName          string                   `json:"vm_name"`
	VMMoref         string                   `json:"vm_moref"`
	MigrationID     string                   `json:"migration_id"`
	Stage           string                   `json:"stage"`
	ActiveSnapshot  string                   `json:"active_snapshot"`
	Disks           map[string]*runDiskState `json:"disks"`
	CloudStackVMID  string                   `json:"cloudstack_vm_id,omitempty"`
	AttachedNICs    map[string]string        `json:"attached_nics,omitempty"`
	CloudStackConfigured bool                `json:"cloudstack_configured,omitempty"`
	CloudStackStarted bool                   `json:"cloudstack_started,omitempty"`
	VirtV2VDone     bool                     `json:"virt_v2v_done,omitempty"`
	Progress        float64                  `json:"progress,omitempty"`
	TransferSpeedMB float64                  `json:"transfer_speed_mbps,omitempty"`
	LastError       string                   `json:"last_error,omitempty"`
	UpdatedAt       string                   `json:"updated_at"`
}

const (
	stageInit          = "init"
	stageBaseCopy      = "base_copy"
	stageDelta         = "delta"
	stageFinalSync     = "final_sync"
	stageConverting    = "converting"
	stageImportRoot    = "import_root_disk"
	stageImportData    = "import_data_disk"
	stageDone          = "done"
)

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
	OnProgress       func(copyStats)
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
	conn, err := connectVDDKWithRetry(cfg, 6, 1*time.Second)
	if err != nil {
		return 0, err
	}
	defer conn.close()
	handle, err := openVDDKWithRetry(conn, diskPath, 8, 1*time.Second)
	if err != nil {
		return 0, err
	}
	defer handle.close()
	return handle.capacityBytes()
}

func mergeAllocatedExtents(extents []allocatedExtent, diskSizeBytes int64) []allocatedExtent {
	if len(extents) == 0 {
		return nil
	}
	sort.Slice(extents, func(i, j int) bool {
		return extents[i].Offset < extents[j].Offset
	})
	out := make([]allocatedExtent, 0, len(extents))
	for _, e := range extents {
		if e.Length <= 0 || e.Offset >= diskSizeBytes {
			continue
		}
		if e.Offset < 0 {
			e.Length += e.Offset
			e.Offset = 0
		}
		if e.Length <= 0 {
			continue
		}
		end := e.Offset + e.Length
		if end > diskSizeBytes {
			end = diskSizeBytes
		}
		e.Length = end - e.Offset
		if e.Length <= 0 {
			continue
		}
		n := len(out)
		if n == 0 {
			out = append(out, e)
			continue
		}
		last := &out[n-1]
		lastEnd := last.Offset + last.Length
		if e.Offset <= lastEnd {
			if end > lastEnd {
				last.Length = end - last.Offset
			}
			continue
		}
		out = append(out, e)
	}
	return out
}

func queryAllocatedExtents(cfg vddkConnCfg, diskPath string, diskSizeBytes int64, chunkBytes int64) ([]allocatedExtent, error) {
	conn, err := connectVDDKWithRetry(cfg, 4, 1*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.close()
	handle, err := openVDDKWithRetry(conn, diskPath, 5, 1*time.Second)
	if err != nil {
		return nil, err
	}
	defer handle.close()
	return queryAllocatedExtentsWithHandle(handle, diskSizeBytes, chunkBytes)
}

func queryAllocatedExtentsWithHandle(handle *vddkHandle, diskSizeBytes int64, chunkBytes int64) ([]allocatedExtent, error) {
	totalSectors := uint64(diskSizeBytes / sectorSize)
	if totalSectors == 0 {
		return nil, nil
	}
	chunkSectors := uint64(chunkBytes / sectorSize)
	if chunkSectors == 0 {
		chunkSectors = 1
	}
	// Query in windows to keep list sizes bounded on very large disks.
	const windowSectors = uint64((1 << 30) / sectorSize) // 1 GiB window
	if windowSectors == 0 {
		return nil, errors.New("invalid allocated-block query window")
	}

	extents := make([]allocatedExtent, 0, 1024)
	var start uint64
	for start < totalSectors {
		n := windowSectors
		if rem := totalSectors - start; rem < n {
			n = rem
		}
		got, err := handle.queryAllocatedBlocks(start, n, chunkSectors)
		if err != nil {
			return nil, err
		}
		extents = append(extents, got...)
		start += n
	}

	// Per VDDK docs: final non-chunk-aligned tail should be treated as allocated.
	if rem := totalSectors % chunkSectors; rem != 0 {
		tailStart := int64(totalSectors-rem) * sectorSize
		tailLen := int64(rem) * sectorSize
		extents = append(extents, allocatedExtent{Offset: tailStart, Length: tailLen})
	}

	return mergeAllocatedExtents(extents, diskSizeBytes), nil
}

func prepareBaseCopyMetadata(opts baseCopyOptions, allocChunkBytes int64) (int64, []allocatedExtent, error, error) {
	const probeTimeout = 75 * time.Second
	deadline := time.Now().Add(probeTimeout)
	var lastErr error
	attempt := 0

	for {
		attempt++
		conn, err := connectVDDKWithRetry(opts.VDDK, 2, 750*time.Millisecond)
		if err == nil {
			handle, openErr := openVDDKWithRetry(conn, opts.DiskPath, 2, 750*time.Millisecond)
			if openErr == nil {
				sourceDiskBytes, capErr := handle.capacityBytes()
				if capErr != nil {
					if opts.DiskSizeBytes > 0 {
						fmt.Fprintf(
							os.Stderr,
							"[base-copy] warning: failed to auto-detect source disk size (%v), using provided disk-size-bytes=%d\n",
							capErr,
							opts.DiskSizeBytes,
						)
						sourceDiskBytes = opts.DiskSizeBytes
					} else {
						lastErr = capErr
					}
				}
				if sourceDiskBytes > 0 {
					if opts.DiskSizeBytes > 0 && opts.DiskSizeBytes != sourceDiskBytes {
						fmt.Fprintf(
							os.Stderr,
							"[base-copy] disk-size-bytes mismatch requested=%d detected=%d, using detected size\n",
							opts.DiskSizeBytes,
							sourceDiskBytes,
						)
					} else if opts.DiskSizeBytes <= 0 {
						fmt.Fprintf(os.Stderr, "[base-copy] auto-detected source disk size=%d bytes\n", sourceDiskBytes)
					}

					extents, allocErr := queryAllocatedExtentsWithHandle(handle, sourceDiskBytes, allocChunkBytes)
					handle.close()
					conn.close()
					return sourceDiskBytes, extents, allocErr, nil
				}
				handle.close()
			} else {
				lastErr = openErr
			}
			conn.close()
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			break
		}
		sleepSec := attempt
		if sleepSec > 5 {
			sleepSec = 5
		}
		time.Sleep(time.Duration(sleepSec) * time.Second)
	}

	if opts.DiskSizeBytes > 0 {
		if lastErr == nil {
			lastErr = errors.New("source snapshot not yet readable")
		}
		fmt.Fprintf(
			os.Stderr,
			"[base-copy] warning: snapshot readability probe timed out (%v), continuing with provided disk-size-bytes=%d\n",
			lastErr,
			opts.DiskSizeBytes,
		)
		return opts.DiskSizeBytes, nil, fmt.Errorf("probe unavailable: %w", lastErr), nil
	}
	if lastErr == nil {
		lastErr = errors.New("unknown VDDK open error")
	}
	return 0, nil, nil, fmt.Errorf("failed to prepare source metadata after probe timeout: %w", lastErr)
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

	allocChunkBytes := int64(alignUp(opts.MinChunkMB*1024*1024, sectorSize))
	sourceDiskBytes, allocatedExtents, allocErr, err := prepareBaseCopyMetadata(opts, allocChunkBytes)
	if err != nil {
		return copyStats{}, err
	}
	opts.DiskSizeBytes = sourceDiskBytes

	if err := createSparseQCOW2(opts.TargetQCOW2, opts.DiskSizeBytes); err != nil {
		return copyStats{}, err
	}
	endpoint, err := startQcow2Endpoint(opts.TargetQCOW2)
	if err != nil {
		return copyStats{}, err
	}
	endpointClosed := false
	defer func() {
		if !endpointClosed {
			endpoint.close()
		}
	}()

	writerClient, err := dialNBDUnix(endpoint.sock)
	if err != nil {
		return copyStats{}, fmt.Errorf("nbd writer connect failed: %w", err)
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			_ = writerClient.close()
		}
	}()

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
	useAllocatedExtents := allocErr == nil
	if allocErr != nil {
		fmt.Fprintf(os.Stderr, "[base-copy] QueryAllocatedBlocks unavailable (%v), falling back to full read\n", allocErr)
	} else {
		var allocBytes int64
		for _, e := range allocatedExtents {
			allocBytes += e.Length
		}
		fmt.Fprintf(
			os.Stderr,
			"[base-copy] QueryAllocatedBlocks enabled extents=%d allocated_bytes=%d total_bytes=%d\n",
			len(allocatedExtents),
			allocBytes,
			opts.DiskSizeBytes,
		)
	}

	var bytesRead int64
	var bytesWritten int64
	var bytesZero int64
	report := func() {
		if opts.OnProgress == nil {
			return
		}
		opts.OnProgress(copyStats{
			BytesRead:        atomic.LoadInt64(&bytesRead),
			BytesWritten:     atomic.LoadInt64(&bytesWritten),
			BytesZeroSkipped: atomic.LoadInt64(&bytesZero),
			ElapsedSec:       int64(time.Since(start).Seconds()),
			Mode:             "base_copy",
		})
	}
	progressStop := make(chan struct{})
	if opts.OnProgress != nil {
		go func() {
			tk := time.NewTicker(1 * time.Second)
			defer tk.Stop()
			for {
				select {
				case <-progressStop:
					return
				case <-ctx.Done():
					return
				case <-tk.C:
					report()
				}
			}
		}()
	}
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
		enqueueRange := func(start, end int64) bool {
			offset := start
			for offset < end {
				select {
				case <-ctx.Done():
					return false
				default:
				}
				chunk := sizer.current()
				remaining := end - offset
				if int64(chunk) > remaining {
					chunk = alignDown(int(remaining), sectorSize)
					if chunk <= 0 {
						chunk = sectorSize
					}
				}
				select {
				case <-ctx.Done():
					return false
				case readQ <- block{Offset: offset, Length: chunk}:
				}
				offset += int64(chunk)
			}
			return true
		}

		if useAllocatedExtents {
			for _, e := range allocatedExtents {
				start := e.Offset
				end := e.Offset + e.Length
				if start < 0 {
					start = 0
				}
				if end > opts.DiskSizeBytes {
					end = opts.DiskSizeBytes
				}
				if end <= start {
					continue
				}
				if ok := enqueueRange(start, end); !ok {
					return
				}
			}
			return
		}

		if ok := enqueueRange(0, opts.DiskSizeBytes); !ok {
			return
		}
	}()

	var workerWG sync.WaitGroup
	var openWG sync.WaitGroup
	var activeReaders int32
	openWG.Add(opts.Readers)
	for i := 0; i < opts.Readers; i++ {
		workerWG.Add(1)
		go func(id int) {
			defer workerWG.Done()
			if id > 1 {
				time.Sleep(time.Duration(id-1) * 250 * time.Millisecond)
			}
			conn, err := connectVDDKWithRetry(opts.VDDK, 5, 1*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[base-copy] warning: reader %d connect failed, disabling this reader: %v\n", id, err)
				openWG.Done()
				return
			}
			defer conn.close()
			handle, err := openVDDKWithRetry(conn, opts.DiskPath, 6, 1*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[base-copy] warning: reader %d open failed, disabling this reader: %v\n", id, err)
				openWG.Done()
				return
			}
			defer handle.close()
			atomic.AddInt32(&activeReaders, 1)
			openWG.Done()

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
	// Wait until all readers either opened successfully or failed open.
	// This removes a race where base-copy could appear successful before
	// we knew whether any VDDK handle was actually usable.
	openWG.Wait()
	if atomic.LoadInt32(&activeReaders) == 0 {
		pushErr(fmt.Errorf("no VDDK readers could open disk %s (server=%s vm=%s snapshot=%s)", opts.DiskPath, opts.VDDK.Server, opts.VDDK.VMMoref, opts.VDDK.SnapshotMoref))
	}

	workerWG.Wait()
	close(writeQ)
	writerWG.Wait()

	select {
	case e := <-errCh:
		close(progressStop)
		report()
		return copyStats{}, e
	default:
	}

	if opts.DiskSizeBytes > 0 && atomic.LoadInt64(&bytesRead) == 0 {
		close(progressStop)
		report()
		return copyStats{}, fmt.Errorf("base copy read zero bytes for non-zero disk size=%d; refusing to continue", opts.DiskSizeBytes)
	}

	if err := writerClient.flush(); err != nil {
		close(progressStop)
		report()
		return copyStats{}, fmt.Errorf("qcow2 flush failed: %w", err)
	}
	if err := writerClient.close(); err != nil {
		close(progressStop)
		report()
		return copyStats{}, fmt.Errorf("nbd writer close failed: %w", err)
	}
	writerClosed = true
	endpoint.close()
	endpointClosed = true
	close(progressStop)
	report()

	if opts.RunVirtV2V {
		if err := runVirtV2VInPlace([]string{opts.TargetQCOW2}, opts.VirtioISO); err != nil {
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
	OnProgress       func(copyStats)
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
	for i, r := range ranges {
		if r.Start < 0 || r.Length <= 0 {
			return copyStats{}, fmt.Errorf("invalid range[%d]: start=%d length=%d", i, r.Start, r.Length)
		}
		if r.Start%sectorSize != 0 || r.Length%sectorSize != 0 {
			return copyStats{}, fmt.Errorf(
				"unaligned range[%d]: start=%d length=%d (must be %d-byte aligned)",
				i,
				r.Start,
				r.Length,
				sectorSize,
			)
		}
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
	report := func() {
		if opts.OnProgress == nil {
			return
		}
		opts.OnProgress(copyStats{
			BytesRead:    atomic.LoadInt64(&bytesRead),
			BytesWritten: atomic.LoadInt64(&bytesWritten),
			ElapsedSec:   int64(time.Since(start).Seconds()),
			Mode:         "delta_sync",
		})
	}
	progressStop := make(chan struct{})
	if opts.OnProgress != nil {
		go func() {
			tk := time.NewTicker(1 * time.Second)
			defer tk.Stop()
			for {
				select {
				case <-progressStop:
					return
				case <-ctx.Done():
					return
				case <-tk.C:
					report()
				}
			}
		}()
	}
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
			offset := int(r.Start)
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
	var openWG sync.WaitGroup
	var activeReaders int32
	openWG.Add(opts.Readers)
	for i := 0; i < opts.Readers; i++ {
		workerWG.Add(1)
		go func(id int) {
			defer workerWG.Done()
			if id > 1 {
				time.Sleep(time.Duration(id-1) * 250 * time.Millisecond)
			}
			conn, err := connectVDDKWithRetry(opts.VDDK, 5, 1*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[delta-sync] warning: reader %d connect failed, disabling this reader: %v\n", id, err)
				openWG.Done()
				return
			}
			defer conn.close()
			handle, err := openVDDKWithRetry(conn, opts.DiskPath, 6, 1*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[delta-sync] warning: reader %d open failed, disabling this reader: %v\n", id, err)
				openWG.Done()
				return
			}
			defer handle.close()
			atomic.AddInt32(&activeReaders, 1)
			openWG.Done()

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
	go func() {
		openWG.Wait()
		if atomic.LoadInt32(&activeReaders) == 0 {
			pushErr(fmt.Errorf("no delta readers could open disk %s (server=%s vm=%s snapshot=%s)", opts.DiskPath, opts.VDDK.Server, opts.VDDK.VMMoref, opts.VDDK.SnapshotMoref))
		}
	}()

	workerWG.Wait()
	close(writeQ)
	writerWG.Wait()

	select {
	case e := <-errCh:
		close(progressStop)
		report()
		return copyStats{}, e
	default:
	}

	if err := writerClient.flush(); err != nil {
		close(progressStop)
		report()
		return copyStats{}, fmt.Errorf("delta qcow2 flush failed: %w", err)
	}
	close(progressStop)
	report()

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

func loadAppConfig(path string) (*appConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg appConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config yaml: %w", err)
	}
	return &cfg, nil
}

func loadRunSpec(path string) (*runSpec, error) {
	specs, err := loadRunSpecs([]string{path})
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("spec %s did not contain any VM definitions", path)
	}
	return specs[0], nil
}

func loadRunSpecs(paths []string) ([]*runSpec, error) {
	out := make([]*runSpec, 0)
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		var many runSpecFile
		if err := yaml.Unmarshal(raw, &many); err == nil && len(many.VMs) > 0 {
			for i := range many.VMs {
				specCopy := many.VMs[i]
				out = append(out, &specCopy)
			}
			continue
		}

		var one runSpec
		if err := yaml.Unmarshal(raw, &one); err != nil {
			return nil, fmt.Errorf("parse run spec yaml %s: %w", path, err)
		}
		out = append(out, &one)
	}
	return out, nil
}

func loadRunState(path string) (*runState, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var st runState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("parse run state json: %w", err)
	}
	if st.Disks == nil {
		st.Disks = map[string]*runDiskState{}
	}
	return &st, nil
}

func defaultConfigPath() string {
	if _, err := os.Stat("config.yaml"); err == nil {
		return "config.yaml"
	}
	if _, err := os.Stat("../config.yaml"); err == nil {
		return "../config.yaml"
	}
	return "config.yaml"
}

func getServerThumbprint(host string) (string, error) {
	conn, err := tls.Dial("tcp", net.JoinHostPort(host, "443"), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return "", err
	}
	defer conn.Close()
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return "", errors.New("no peer certificates received")
	}
	sum := sha1.Sum(state.PeerCertificates[0].Raw)
	hexVal := strings.ToUpper(hex.EncodeToString(sum[:]))
	parts := make([]string, 0, len(hexVal)/2)
	for i := 0; i < len(hexVal); i += 2 {
		parts = append(parts, hexVal[i:i+2])
	}
	return strings.Join(parts, ":"), nil
}

func hostForThumbprintLookup(server string) string {
	value := strings.TrimSpace(server)
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	if idx := strings.Index(value, "/"); idx >= 0 {
		value = value[:idx]
	}
	if strings.HasPrefix(value, "[") {
		if end := strings.Index(value, "]"); end > 0 {
			return value[1:end]
		}
	}
	if strings.Count(value, ":") == 1 {
		if host, _, err := net.SplitHostPort(value); err == nil {
			return host
		}
		if idx := strings.LastIndex(value, ":"); idx > 0 {
			return value[:idx]
		}
	}
	return value
}

func ensureVDDKThumbprint(cfg *vddkConnCfg) error {
	if strings.TrimSpace(cfg.Thumbprint) != "" {
		return nil
	}
	host := hostForThumbprintLookup(cfg.Server)
	if host == "" {
		return errors.New("cannot auto-detect thumbprint: server is empty")
	}
	thumb, err := getServerThumbprint(host)
	if err != nil {
		return fmt.Errorf("auto-detect thumbprint failed for server %q: %w (provide --thumbprint to override)", cfg.Server, err)
	}
	cfg.Thumbprint = thumb
	fmt.Fprintf(os.Stderr, "[vddk] auto-detected SSL thumbprint: %s\n", thumb)
	return nil
}

func connectVCenter(ctx context.Context, cfg *appConfig) (*govmomi.Client, error) {
	u, err := neturl.Parse("https://" + cfg.VCenter.Host + "/sdk")
	if err != nil {
		return nil, err
	}
	u.User = neturl.UserPassword(cfg.VCenter.User, cfg.VCenter.Password)
	return govmomi.NewClient(ctx, u, true)
}

func isNotAuthenticatedError(err error) bool {
	if err == nil {
		return false
	}
	var sf *soap.Fault
	if errors.As(err, &sf) {
		if _, ok := sf.VimFault().(*types.NotAuthenticated); ok {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "notauthenticated") || strings.Contains(msg, "not authenticated")
}

func findVM(ctx context.Context, client *govmomi.Client, vmName string) (*object.VirtualMachine, error) {
	finder := find.NewFinder(client.Client, true)
	dc, err := finder.DefaultDatacenter(ctx)
	if err != nil {
		return nil, err
	}
	finder.SetDatacenter(dc)
	return finder.VirtualMachine(ctx, vmName)
}

func listVMDisksAndBootUnit(ctx context.Context, client *govmomi.Client, vm *object.VirtualMachine) ([]vmDisk, int, error) {
	pc := property.DefaultCollector(client.Client)
	var vmMo mo.VirtualMachine
	if err := pc.RetrieveOne(ctx, vm.Reference(), []string{"config.hardware.device", "config.bootOptions"}, &vmMo); err != nil {
		return nil, 0, err
	}
	disks := make([]vmDisk, 0)
	keyToUnit := map[int32]int{}
	for _, dev := range vmMo.Config.Hardware.Device {
		vd, ok := dev.(*types.VirtualDisk)
		if !ok {
			continue
		}
		unit := 0
		if vd.UnitNumber != nil {
			unit = int(*vd.UnitNumber)
		}
		capacity := int64(vd.CapacityInBytes)
		if capacity <= 0 && vd.CapacityInKB > 0 {
			capacity = int64(vd.CapacityInKB) * 1024
		}
		disks = append(disks, vmDisk{
			Key:        vd.Key,
			Unit:       unit,
			Capacity:   capacity,
			SourcePath: getBackingString(vd.Backing, "FileName"),
		})
		keyToUnit[vd.Key] = unit
	}
	if len(disks) == 0 {
		return nil, 0, errors.New("no virtual disks found on VM")
	}
	sort.Slice(disks, func(i, j int) bool { return disks[i].Unit < disks[j].Unit })

	bootUnit := disks[0].Unit
	if vmMo.Config.BootOptions != nil {
		for _, b := range vmMo.Config.BootOptions.BootOrder {
			bootDisk, ok := b.(*types.VirtualMachineBootOptionsBootableDiskDevice)
			if !ok {
				continue
			}
			if u, ok := keyToUnit[bootDisk.DeviceKey]; ok {
				bootUnit = u
				break
			}
		}
	}
	return disks, bootUnit, nil
}

func createSnapshot(ctx context.Context, vm *object.VirtualMachine, name string, quiesce bool) (types.ManagedObjectReference, error) {
	task, err := vm.CreateSnapshot(ctx, name, "", false, quiesce)
	if err != nil {
		return types.ManagedObjectReference{}, err
	}
	result, err := task.WaitForResult(ctx, nil)
	if err != nil {
		return types.ManagedObjectReference{}, err
	}
	switch v := result.Result.(type) {
	case types.ManagedObjectReference:
		return v, nil
	case *types.ManagedObjectReference:
		return *v, nil
	default:
		return types.ManagedObjectReference{}, fmt.Errorf("unexpected snapshot result type %T", result.Result)
	}
}

func vmToolsStatus(ctx context.Context, client *govmomi.Client, vmRef types.ManagedObjectReference) (string, error) {
	pc := property.DefaultCollector(client.Client)
	var vmMo mo.VirtualMachine
	if err := pc.RetrieveOne(ctx, vmRef, []string{"guest.toolsStatus"}, &vmMo); err != nil {
		return "", err
	}
	root := reflect.ValueOf(vmMo)
	guest := root.FieldByName("Guest")
	if !guest.IsValid() {
		return "", nil
	}
	if guest.Kind() == reflect.Ptr {
		if guest.IsNil() {
			return "", nil
		}
		guest = guest.Elem()
	}
	f := guest.FieldByName("ToolsStatus")
	if !f.IsValid() {
		return "", nil
	}
	switch f.Kind() {
	case reflect.String:
		return strings.TrimSpace(f.String()), nil
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", f.Interface())), nil
	}
}

func vmToolsUsable(status string) bool {
	return status == "toolsOk" || status == "toolsOld"
}

func createSnapshotWithMode(
	ctx context.Context,
	client *govmomi.Client,
	vm *object.VirtualMachine,
	name string,
	mode string,
	log *runLogger,
) (types.ManagedObjectReference, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "auto"
	}

	switch mode {
	case "false":
		log.Printf("Creating non-quiesced snapshot: %s", name)
		return createSnapshot(ctx, vm, name, false)
	case "true":
		log.Printf("Creating quiesced snapshot: %s", name)
		return createSnapshot(ctx, vm, name, true)
	case "auto":
		status, err := vmToolsStatus(ctx, client, vm.Reference())
		if err != nil {
			log.Printf("Warning: failed to read VMware Tools status, using non-quiesced snapshot: %v", err)
			return createSnapshot(ctx, vm, name, false)
		}
		if vmToolsUsable(status) {
			log.Printf("Trying quiesced snapshot (tools status=%s): %s", status, name)
			snap, err := createSnapshot(ctx, vm, name, true)
			if err == nil {
				return snap, nil
			}
			log.Printf("Quiesced snapshot failed (%v), falling back to non-quiesced", err)
		} else {
			log.Printf("VMware Tools not running (status=%s), using non-quiesced snapshot", status)
		}
		return createSnapshot(ctx, vm, name, false)
	default:
		return types.ManagedObjectReference{}, fmt.Errorf("invalid snapshot_quiesce mode: %s", mode)
	}
}

func vmCBTEnabled(ctx context.Context, client *govmomi.Client, vmRef types.ManagedObjectReference) (bool, error) {
	pc := property.DefaultCollector(client.Client)
	var vmMo mo.VirtualMachine
	if err := pc.RetrieveOne(ctx, vmRef, []string{"config.changeTrackingEnabled"}, &vmMo); err != nil {
		return false, err
	}
	root := reflect.ValueOf(vmMo)
	cfg := root.FieldByName("Config")
	if !cfg.IsValid() {
		return false, nil
	}
	if cfg.Kind() == reflect.Ptr {
		if cfg.IsNil() {
			return false, nil
		}
		cfg = cfg.Elem()
	}
	f := cfg.FieldByName("ChangeTrackingEnabled")
	if !f.IsValid() {
		return false, nil
	}
	switch f.Kind() {
	case reflect.Bool:
		return f.Bool(), nil
	case reflect.Ptr:
		if f.IsNil() {
			return false, nil
		}
		if f.Elem().Kind() == reflect.Bool {
			return f.Elem().Bool(), nil
		}
	}
	return false, nil
}

func setCBTInConfigSpec(spec *types.VirtualMachineConfigSpec, enabled bool) error {
	rv := reflect.ValueOf(spec).Elem()
	f := rv.FieldByName("ChangeTrackingEnabled")
	if !f.IsValid() || !f.CanSet() {
		return errors.New("ChangeTrackingEnabled field not settable in VirtualMachineConfigSpec")
	}
	switch f.Kind() {
	case reflect.Bool:
		f.SetBool(enabled)
		return nil
	case reflect.Ptr:
		if f.Type().Elem().Kind() != reflect.Bool {
			return errors.New("unexpected ChangeTrackingEnabled pointer element type")
		}
		b := enabled
		f.Set(reflect.ValueOf(&b))
		return nil
	default:
		return fmt.Errorf("unsupported ChangeTrackingEnabled kind: %s", f.Kind())
	}
}

func ensureCBTEnabled(ctx context.Context, client *govmomi.Client, vm *object.VirtualMachine, log *runLogger) error {
	enabled, err := vmCBTEnabled(ctx, client, vm.Reference())
	if err != nil {
		return err
	}
	if enabled {
		log.Printf("CBT already enabled")
		return nil
	}
	log.Printf("CBT not enabled, enabling now")
	spec := types.VirtualMachineConfigSpec{}
	if err := setCBTInConfigSpec(&spec, true); err != nil {
		return err
	}
	task, err := vm.Reconfigure(ctx, spec)
	if err != nil {
		return err
	}
	if _, err := task.WaitForResult(ctx, nil); err != nil {
		return err
	}
	log.Printf("CBT enabled successfully")
	return nil
}

func isVMPoweredOff(ctx context.Context, vm *object.VirtualMachine) (bool, error) {
	state, err := vm.PowerState(ctx)
	if err != nil {
		return false, err
	}
	return state == types.VirtualMachinePowerStatePoweredOff, nil
}

func waitForPowerOff(ctx context.Context, vm *object.VirtualMachine, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		off, err := isVMPoweredOff(ctx, vm)
		if err != nil {
			return err
		}
		if off {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for VM power off")
		}
		time.Sleep(5 * time.Second)
	}
}

func shutdownVMForFinalize(ctx context.Context, client *govmomi.Client, vm *object.VirtualMachine, mode string, log *runLogger) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "auto"
	}
	off, err := isVMPoweredOff(ctx, vm)
	if err != nil {
		return err
	}
	if off {
		log.Printf("Source VM already powered off")
		return nil
	}

	switch mode {
	case "auto":
		status, err := vmToolsStatus(ctx, client, vm.Reference())
		if err == nil && vmToolsUsable(status) {
			log.Printf("Attempting graceful guest shutdown (auto mode)")
			if _, err := methods.ShutdownGuest(ctx, client.Client, &types.ShutdownGuest{This: vm.Reference()}); err != nil {
				log.Printf("Graceful shutdown request failed: %v", err)
			}
			if err := waitForPowerOff(ctx, vm, 5*time.Minute); err == nil {
				log.Printf("Guest shutdown completed")
				return nil
			}
			log.Printf("Graceful shutdown timed out; forcing power off")
		} else {
			log.Printf("VMware Tools not running (status=%s); forcing power off", status)
		}
		task, err := vm.PowerOff(ctx)
		if err != nil {
			return err
		}
		_, err = task.WaitForResult(ctx, nil)
		return err
	case "force":
		log.Printf("Forcing source VM power off")
		task, err := vm.PowerOff(ctx)
		if err != nil {
			return err
		}
		_, err = task.WaitForResult(ctx, nil)
		return err
	case "manual":
		log.Printf("Waiting for manual shutdown of source VM")
		for {
			off, err := isVMPoweredOff(ctx, vm)
			if err != nil {
				return err
			}
			if off {
				log.Printf("Manual shutdown detected")
				return nil
			}
			time.Sleep(5 * time.Second)
		}
	default:
		return fmt.Errorf("invalid shutdown_mode: %s (expected auto|force|manual)", mode)
	}
}

func removeSnapshot(ctx context.Context, client *govmomi.Client, snapshotRef types.ManagedObjectReference) error {
	req := types.RemoveSnapshot_Task{
		This:           snapshotRef,
		RemoveChildren: false,
	}
	resp, err := methods.RemoveSnapshot_Task(ctx, client.Client, &req)
	if err != nil {
		return err
	}
	task := object.NewTask(client.Client, resp.Returnval)
	_, err = task.WaitForResult(ctx, nil)
	return err
}

func snapshotDiskMetadata(ctx context.Context, client *govmomi.Client, snapshotRef types.ManagedObjectReference) (map[int32]snapshotDiskMeta, error) {
	pc := property.DefaultCollector(client.Client)
	var snap mo.VirtualMachineSnapshot
	if err := pc.RetrieveOne(ctx, snapshotRef, []string{"config.hardware.device"}, &snap); err != nil {
		return nil, err
	}
	out := map[int32]snapshotDiskMeta{}
	for _, dev := range snap.Config.Hardware.Device {
		vd, ok := dev.(*types.VirtualDisk)
		if !ok {
			continue
		}
		fileName := getBackingString(vd.Backing, "FileName")
		changeID := getBackingString(vd.Backing, "ChangeId")
		out[vd.Key] = snapshotDiskMeta{
			Path:     fileName,
			ChangeID: changeID,
		}
	}
	return out, nil
}

func getBackingString(backing types.BaseVirtualDeviceBackingInfo, field string) string {
	if backing == nil {
		return ""
	}
	v := reflect.ValueOf(backing)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	f := v.FieldByName(field)
	if !f.IsValid() || f.Kind() != reflect.String {
		return ""
	}
	return f.String()
}

func resolveStorageID(spec *runSpec, unit int, bootUnit int) (string, error) {
	if unit == bootUnit {
		if spec.Target.CloudStack.StorageID == "" {
			return "", errors.New("target.cloudstack.storageid is required for boot disk")
		}
		return spec.Target.CloudStack.StorageID, nil
	}
	cfg, ok := spec.Disks[strconv.Itoa(unit)]
	if ok && cfg.StorageID != "" {
		return cfg.StorageID, nil
	}
	if spec.Target.CloudStack.StorageID != "" {
		return spec.Target.CloudStack.StorageID, nil
	}
	return "", fmt.Errorf("storageid is required for data disk unit %d", unit)
}

func resolveDataDiskConfig(spec *runSpec, unit int) (string, string, error) {
	cfg, ok := spec.Disks[strconv.Itoa(unit)]
	if !ok {
		cfg = diskTargetSpec{}
	}
	storageID := cfg.StorageID
	if storageID == "" {
		storageID = spec.Target.CloudStack.StorageID
	}
	if storageID == "" {
		return "", "", fmt.Errorf("storageid missing for data disk unit %d", unit)
	}
	diskOfferingID := cfg.DiskOfferingID
	if diskOfferingID == "" {
		diskOfferingID = spec.Target.CloudStack.DiskOfferingID
	}
	if diskOfferingID == "" {
		return "", "", fmt.Errorf("diskofferingid missing for data disk unit %d", unit)
	}
	return storageID, diskOfferingID, nil
}

func applyCloudStackDefaults(spec *runSpec, cfg *appConfig) {
	firstNonEmpty := func(vals ...string) string {
		for _, v := range vals {
			if strings.TrimSpace(v) != "" {
				return v
			}
		}
		return ""
	}

	tgt := &spec.Target.CloudStack
	def := cfg.CloudStackDefaults
	tgt.ZoneID = firstNonEmpty(tgt.ZoneID, def.ZoneID)
	tgt.ClusterID = firstNonEmpty(tgt.ClusterID, def.ClusterID)
	tgt.StorageID = firstNonEmpty(tgt.StorageID, def.StorageID)
	tgt.NetworkID = firstNonEmpty(tgt.NetworkID, def.NetworkID)
	tgt.ServiceOfferingID = firstNonEmpty(tgt.ServiceOfferingID, def.ServiceOfferingID)
	tgt.DiskOfferingID = firstNonEmpty(tgt.DiskOfferingID, def.DiskOfferingID)
	tgt.OSTypeID = firstNonEmpty(tgt.OSTypeID, def.OSTypeID)
	tgt.BootType = firstNonEmpty(tgt.BootType, def.BootType)
	tgt.BootMode = firstNonEmpty(tgt.BootMode, def.BootMode)
	tgt.RootDiskController = firstNonEmpty(tgt.RootDiskController, def.RootDiskController)
	tgt.NICAdapter = firstNonEmpty(tgt.NICAdapter, def.NICAdapter)

	if spec.Disks == nil {
		spec.Disks = map[string]diskTargetSpec{}
	}
	for unit, diskCfg := range spec.Disks {
		diskCfg.StorageID = firstNonEmpty(diskCfg.StorageID, tgt.StorageID, def.StorageID)
		diskCfg.DiskOfferingID = firstNonEmpty(diskCfg.DiskOfferingID, tgt.DiskOfferingID, def.DiskOfferingID)
		spec.Disks[unit] = diskCfg
	}
}

func persistSpecForRun(controlDir string, spec *runSpec) error {
	if spec == nil {
		return errors.New("nil run spec")
	}
	raw, err := yaml.Marshal(spec)
	if err != nil {
		return fmt.Errorf("marshal run spec: %w", err)
	}
	latestPath := filepath.Join(controlDir, "spec.latest.yaml")
	if err := os.WriteFile(latestPath, raw, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", latestPath, err)
	}
	stablePath := filepath.Join(controlDir, "spec.yaml")
	if _, err := os.Stat(stablePath); os.IsNotExist(err) {
		if err := os.WriteFile(stablePath, raw, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", stablePath, err)
		}
	} else if err != nil {
		return fmt.Errorf("stat %s: %w", stablePath, err)
	}
	return nil
}

func buildCloudStackNICPlan(target cloudStackTargetSpec) (string, []nicAttachmentPlan) {
	primaryNetworkID := strings.TrimSpace(target.NetworkID)
	if len(target.NICMappings) == 0 {
		return primaryNetworkID, nil
	}

	entries := make([]nicAttachmentPlan, 0, len(target.NICMappings))
	for key, mapping := range target.NICMappings {
		networkID := strings.TrimSpace(mapping.NetworkID)
		if networkID == "" {
			continue
		}
		adapterName := strings.TrimSpace(mapping.SourceLabel)
		if adapterName == "" {
			adapterName = fmt.Sprintf("NIC %s", key)
		}
		entries = append(entries, nicAttachmentPlan{
			MapKey:      strings.TrimSpace(key),
			AdapterName: adapterName,
			SourceIndex: mapping.SourceIndex,
			NetworkID:   networkID,
		})
	}
	if len(entries) == 0 {
		return primaryNetworkID, nil
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].SourceIndex != entries[j].SourceIndex {
			return entries[i].SourceIndex < entries[j].SourceIndex
		}
		if entries[i].AdapterName != entries[j].AdapterName {
			return entries[i].AdapterName < entries[j].AdapterName
		}
		return entries[i].MapKey < entries[j].MapKey
	})

	primaryIdx := -1
	for i := range entries {
		if strings.EqualFold(strings.TrimSpace(entries[i].AdapterName), "Network adapter 1") {
			primaryIdx = i
			break
		}
	}
	if primaryIdx == -1 {
		for i := range entries {
			if entries[i].SourceIndex == 0 {
				primaryIdx = i
				break
			}
		}
	}
	if primaryIdx == -1 {
		for i := range entries {
			if entries[i].MapKey == "4000" {
				primaryIdx = i
				break
			}
		}
	}

	additional := make([]nicAttachmentPlan, 0, len(entries))
	if primaryIdx >= 0 {
		primaryNetworkID = entries[primaryIdx].NetworkID
		for i := range entries {
			if i == primaryIdx {
				continue
			}
			additional = append(additional, entries[i])
		}
		return primaryNetworkID, additional
	}

	return primaryNetworkID, entries
}

func validateCloudStackTarget(target cloudStackTargetSpec) error {
	missing := make([]string, 0)
	if strings.TrimSpace(target.ZoneID) == "" {
		missing = append(missing, "zoneid")
	}
	if strings.TrimSpace(target.ClusterID) == "" {
		missing = append(missing, "clusterid")
	}
	if strings.TrimSpace(target.StorageID) == "" {
		missing = append(missing, "storageid")
	}
	primaryNetworkID, _ := buildCloudStackNICPlan(target)
	if strings.TrimSpace(primaryNetworkID) == "" {
		missing = append(missing, "networkid")
	}
	if strings.TrimSpace(target.ServiceOfferingID) == "" {
		missing = append(missing, "serviceofferingid")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing cloudstack target fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func decodeProcMountField(v string) string {
	replacer := strings.NewReplacer(
		`\\`, `\`,
		`\040`, " ",
		`\011`, "\t",
		`\012`, "\n",
	)
	return replacer.Replace(v)
}

func parseOSReleaseValue(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
	}
	return strings.TrimSpace(v)
}

func readHostOSRelease() map[string]string {
	hostOSReleaseOnce.Do(func() {
		hostOSRelease = map[string]string{}
		data, err := os.ReadFile("/etc/os-release")
		if err != nil {
			return
		}
		for _, raw := range strings.Split(string(data), "\n") {
			line := strings.TrimSpace(raw)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			idx := strings.Index(line, "=")
			if idx <= 0 {
				continue
			}
			k := strings.TrimSpace(line[:idx])
			v := parseOSReleaseValue(line[idx+1:])
			if k != "" {
				hostOSRelease[k] = v
			}
		}
	})
	return hostOSRelease
}

func hostIsUbuntu() bool {
	info := readHostOSRelease()
	id := strings.ToLower(strings.TrimSpace(info["ID"]))
	if id == "ubuntu" {
		return true
	}
	idLike := strings.ToLower(strings.TrimSpace(info["ID_LIKE"]))
	return strings.Contains(idLike, "ubuntu")
}

func nfsMountOptionsForHost() string {
	if override := strings.TrimSpace(os.Getenv("V2C_NFS_MOUNT_OPTS")); override != "" {
		return override
	}
	if hostIsUbuntu() {
		return "rw,relatime,vers=3,rsize=1048576,wsize=1048576,hard,proto=tcp,timeo=600,retrans=2"
	}
	return ""
}

func findMountEntry(mountPath string) (source string, fsType string, mounted bool, err error) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return "", "", false, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		src := decodeProcMountField(fields[0])
		dst := decodeProcMountField(fields[1])
		fst := decodeProcMountField(fields[2])
		if dst == mountPath {
			return src, fst, true, nil
		}
	}
	return "", "", false, nil
}

func isNFSFsType(fsType string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(fsType)), "nfs")
}

func ensureStorageMounted(cs *cloudStackClient, storageID string) (string, error) {
	storageID = strings.TrimSpace(storageID)
	if storageID == "" {
		return "", errors.New("storage id is empty")
	}
	mountPath := filepath.Join("/mnt", storageID)
	if err := os.MkdirAll(mountPath, 0o755); err != nil {
		return "", fmt.Errorf("failed to create storage mount path %s: %w", mountPath, err)
	}

	source, fsType, mounted, err := findMountEntry(mountPath)
	if err != nil {
		return "", err
	}
	if mounted {
		if !isNFSFsType(fsType) {
			return "", fmt.Errorf("%s is mounted as %s (%s), expected NFS", mountPath, fsType, source)
		}
		return mountPath, nil
	}

	pool, err := cs.storagePoolByID(storageID)
	if err != nil {
		return "", fmt.Errorf("failed to lookup storage pool %s: %w", storageID, err)
	}
	if !isNFSStoragePool(*pool) {
		return "", fmt.Errorf(
			"storage pool %s (%s) is not NFS (type=%q, url=%q). only NFS storage is supported",
			pool.ID, pool.Name, pool.PoolType, pool.URL,
		)
	}
	nfsSource, err := nfsSourceFromPool(*pool)
	if err != nil {
		return "", err
	}

	mountArgs := []string{"-t", "nfs"}
	if opts := nfsMountOptionsForHost(); opts != "" {
		mountArgs = append(mountArgs, "-o", opts)
	}
	mountArgs = append(mountArgs, nfsSource, mountPath)

	cmd := exec.Command("mount", mountArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf(
			"failed to mount storage pool %s (%s) on %s using %s (args=%v): %w (%s)",
			pool.ID,
			pool.Name,
			mountPath,
			nfsSource,
			mountArgs,
			err,
			strings.TrimSpace(string(out)),
		)
	}

	source, fsType, mounted, err = findMountEntry(mountPath)
	if err != nil {
		return "", err
	}
	if !mounted {
		return "", fmt.Errorf("mount command succeeded but %s is not listed in /proc/mounts", mountPath)
	}
	if !isNFSFsType(fsType) {
		return "", fmt.Errorf("%s mounted as %s (%s), expected NFS", mountPath, fsType, source)
	}
	return mountPath, nil
}

func parseFinalizeAt(s string) (time.Time, error) {
	if strings.TrimSpace(s) == "" {
		return time.Time{}, nil
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
	}
	var parseErr error
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
		parseErr = err
	}
	return time.Time{}, parseErr
}

func queryChangedRanges(
	ctx context.Context,
	client *govmomi.Client,
	vmRef types.ManagedObjectReference,
	snapshotRef types.ManagedObjectReference,
	deviceKey int32,
	changeID string,
	capacity int64,
) ([]changedRange, error) {
	ranges := make([]changedRange, 0)
	var startOffset int64
	for startOffset < capacity {
		req := types.QueryChangedDiskAreas{
			This:        vmRef,
			Snapshot:    &snapshotRef,
			DeviceKey:   deviceKey,
			StartOffset: startOffset,
			ChangeId:    changeID,
		}
		resp, err := methods.QueryChangedDiskAreas(ctx, client.Client, &req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			break
		}
		for _, area := range resp.Returnval.ChangedArea {
			ranges = append(ranges, changedRange{Start: area.Start, Length: area.Length})
		}
		next := resp.Returnval.StartOffset + resp.Returnval.Length
		if next <= startOffset {
			break
		}
		startOffset = next
	}
	return ranges, nil
}

func writeRangesTempFile(ranges []changedRange, unit int) (string, error) {
	name := filepath.Join(os.TempDir(), fmt.Sprintf("v2c_ranges_u%d_%d.json", unit, time.Now().UnixNano()))
	data, err := json.Marshal(ranges)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(name, data, 0o600); err != nil {
		return "", err
	}
	return name, nil
}

func saveRunState(path string, st *runState) error {
	st.UpdatedAt = time.Now().Format(time.RFC3339)
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

type cloudStackClient struct {
	Endpoint  string
	APIKey    string
	SecretKey string
	HTTP      *http.Client
}

func newCloudStackClient(cfg *appConfig) (*cloudStackClient, error) {
	if strings.TrimSpace(cfg.CloudStack.Endpoint) == "" ||
		strings.TrimSpace(cfg.CloudStack.APIKey) == "" ||
		strings.TrimSpace(cfg.CloudStack.SecretKey) == "" {
		return nil, errors.New("cloudstack endpoint/api_key/secret_key are required in config.yaml")
	}
	return &cloudStackClient{
		Endpoint:  normalizeCloudStackEndpoint(cfg.CloudStack.Endpoint),
		APIKey:    cfg.CloudStack.APIKey,
		SecretKey: cfg.CloudStack.SecretKey,
		HTTP: &http.Client{
			Timeout: 45 * time.Second,
		},
	}, nil
}

func (c *cloudStackClient) request(command string, params map[string]string) (map[string]any, error) {
	if params == nil {
		params = map[string]string{}
	}
	params["command"] = command
	params["apikey"] = c.APIKey
	params["response"] = "json"

	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	queryParts := make([]string, 0, len(keys))
	for _, k := range keys {
		queryParts = append(queryParts, fmt.Sprintf("%s=%s", k, neturl.QueryEscape(params[k])))
	}
	query := strings.Join(queryParts, "&")

	mac := hmac.New(sha1.New, []byte(c.SecretKey))
	_, _ = mac.Write([]byte(strings.ToLower(query)))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	reqURL := fmt.Sprintf("%s?%s&signature=%s", c.Endpoint, query, neturl.QueryEscape(signature))
	resp, err := c.HTTP.Get(reqURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("invalid cloudstack response: %w", err)
	}
	if e, ok := out["errorresponse"]; ok {
		return nil, fmt.Errorf("cloudstack error response: %v", e)
	}
	return out, nil
}

func mapGetMap(m map[string]any, key string) (map[string]any, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, false
	}
	out, ok := v.(map[string]any)
	return out, ok
}

func mapGetString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case int:
		return strconv.Itoa(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func mapGetInt(m map[string]any, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case string:
		n, _ := strconv.Atoi(t)
		return n
	default:
		return 0
	}
}

func mapGetArray(m map[string]any, key string) []any {
	if m == nil {
		return nil
	}
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	return arr
}

type cloudStackStoragePoolInfo struct {
	ID        string
	Name      string
	PoolType  string
	IPAddress string
	Path      string
	URL       string
}

func firstNonEmptyString(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func storagePoolInfoFromMap(m map[string]any) cloudStackStoragePoolInfo {
	return cloudStackStoragePoolInfo{
		ID:        mapGetString(m, "id"),
		Name:      mapGetString(m, "name"),
		PoolType:  firstNonEmptyString(mapGetString(m, "type"), mapGetString(m, "pooltype"), mapGetString(m, "storagetype")),
		IPAddress: firstNonEmptyString(mapGetString(m, "ipaddress"), mapGetString(m, "sourcehost"), mapGetString(m, "host")),
		Path:      firstNonEmptyString(mapGetString(m, "path"), mapGetString(m, "sourcepath"), mapGetString(m, "mountpoint")),
		URL:       mapGetString(m, "url"),
	}
}

func isNFSStoragePool(pool cloudStackStoragePoolInfo) bool {
	t := strings.ToLower(strings.TrimSpace(pool.PoolType))
	if strings.Contains(t, "networkfilesystem") || strings.Contains(t, "nfs") {
		return true
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(pool.URL)), "nfs://")
}

func nfsSourceFromPool(pool cloudStackStoragePoolInfo) (string, error) {
	host := strings.TrimSpace(pool.IPAddress)
	path := strings.TrimSpace(pool.Path)
	if host != "" && path != "" {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		return fmt.Sprintf("%s:%s", host, path), nil
	}

	urlValue := strings.TrimSpace(pool.URL)
	if strings.HasPrefix(strings.ToLower(urlValue), "nfs://") {
		u, err := neturl.Parse(urlValue)
		if err != nil {
			return "", fmt.Errorf("invalid NFS url %q: %w", urlValue, err)
		}
		host = strings.TrimSpace(u.Host)
		if h, p, err := net.SplitHostPort(host); err == nil {
			if strings.TrimSpace(p) != "" {
				host = h
			}
		}
		path = strings.TrimSpace(u.Path)
		if host != "" && path != "" {
			return fmt.Sprintf("%s:%s", host, path), nil
		}
	}
	return "", fmt.Errorf("storage pool %s (%s) missing NFS source details (ipaddress/path or nfs url)", pool.ID, pool.Name)
}

func (c *cloudStackClient) storagePoolByID(storageID string) (*cloudStackStoragePoolInfo, error) {
	storageID = strings.TrimSpace(storageID)
	if storageID == "" {
		return nil, errors.New("storage id is empty")
	}
	res, err := c.request("listStoragePools", map[string]string{"id": storageID})
	if err != nil {
		return nil, err
	}
	root, ok := mapGetMap(res, "liststoragepoolsresponse")
	if !ok {
		return nil, fmt.Errorf("unexpected listStoragePools response: %v", res)
	}

	candidates := make([]cloudStackStoragePoolInfo, 0)
	for _, raw := range mapGetArray(root, "storagepool") {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		candidates = append(candidates, storagePoolInfoFromMap(m))
	}
	if len(candidates) == 0 {
		if single, ok := mapGetMap(root, "storagepool"); ok {
			candidates = append(candidates, storagePoolInfoFromMap(single))
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("storage pool not found: %s", storageID)
	}

	for _, p := range candidates {
		if strings.TrimSpace(p.ID) == storageID {
			pool := p
			return &pool, nil
		}
	}

	pool := candidates[0]
	return &pool, nil
}

func (c *cloudStackClient) waitJob(jobID string, kind string) (map[string]any, error) {
	if strings.TrimSpace(jobID) == "" {
		return nil, errors.New("cloudstack job id is empty")
	}
	for {
		res, err := c.request("queryAsyncJobResult", map[string]string{
			"jobid": jobID,
		})
		if err != nil {
			return nil, err
		}
		root, ok := mapGetMap(res, "queryasyncjobresultresponse")
		if !ok {
			return nil, fmt.Errorf("unexpected queryAsyncJobResult response: %v", res)
		}
		status := mapGetInt(root, "jobstatus")
		if status == 1 {
			jobResult, ok := mapGetMap(root, "jobresult")
			if !ok {
				return root, nil
			}
			return jobResult, nil
		}
		if status == 2 {
			return nil, fmt.Errorf("%s job %s failed: %v", kind, jobID, root)
		}
		time.Sleep(5 * time.Second)
	}
}

func sanitizeHostName(vmName string) string {
	name := strings.TrimSpace(vmName)
	if name == "" {
		return "vm"
	}

	var b strings.Builder
	b.Grow(len(name))
	lastHyphen := false
	for i := 0; i < len(name); i++ {
		ch := name[i]
		isLetter := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
		isDigit := ch >= '0' && ch <= '9'
		if isLetter || isDigit {
			b.WriteByte(ch)
			lastHyphen = false
			continue
		}
		if ch == '-' && !lastHyphen {
			b.WriteByte(ch)
			lastHyphen = true
			continue
		}
		if !lastHyphen {
			b.WriteByte('-')
			lastHyphen = true
		}
	}

	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "vm"
	}
	first := out[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')) {
		out = "vm-" + out
	}
	if len(out) > 63 {
		out = out[:63]
	}
	out = strings.TrimRight(out, "-")
	if out == "" {
		out = "vm"
	}
	first = out[0]
	if !((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z')) {
		if len(out) >= 60 {
			out = "vm-" + out[:60]
		} else {
			out = "vm-" + out
		}
		out = strings.TrimRight(out, "-")
	}
	return out
}

func appendCloudStackDetails(params map[string]string, detailSet map[string]string) {
	if len(detailSet) == 0 {
		return
	}
	// CloudStack expects details in this shape:
	// details[0].rootDiskController=virtio&details[0].nicAdapter=virtio
	keys := make([]string, 0, len(detailSet))
	for key := range detailSet {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := strings.TrimSpace(detailSet[key])
		if key == "" || value == "" {
			continue
		}
		params[fmt.Sprintf("details[0].%s", key)] = value
	}
}

func buildCloudStackVMDetails(target cloudStackTargetSpec) map[string]string {
	details := map[string]string{}
	if v := strings.TrimSpace(target.RootDiskController); v != "" {
		details["rootDiskController"] = v
	}
	if v := strings.TrimSpace(target.NICAdapter); v != "" {
		details["nicAdapter"] = v
	}
	if strings.EqualFold(strings.TrimSpace(target.BootType), "UEFI") {
		mode := strings.ToUpper(strings.TrimSpace(target.BootMode))
		if mode == "" {
			mode = "LEGACY"
		}
		details["UEFI"] = mode
	}
	return details
}

func waitCloudStackJobIfPresent(cs *cloudStackClient, root map[string]any, kind string) error {
	jobID := mapGetString(root, "jobid")
	if strings.TrimSpace(jobID) == "" {
		return nil
	}
	_, err := cs.waitJob(jobID, kind)
	return err
}

func updateCloudStackVMSettings(cs *cloudStackClient, vmID string, target cloudStackTargetSpec) error {
	params := map[string]string{
		"id": strings.TrimSpace(vmID),
	}
	if v := strings.TrimSpace(target.OSTypeID); v != "" {
		params["ostypeid"] = v
	}
	appendCloudStackDetails(params, buildCloudStackVMDetails(target))
	if len(params) == 1 {
		return nil
	}
	resp, err := cs.request("updateVirtualMachine", params)
	if err != nil {
		return err
	}
	root, ok := mapGetMap(resp, "updatevirtualmachineresponse")
	if !ok {
		return nil
	}
	return waitCloudStackJobIfPresent(cs, root, "updateVirtualMachine")
}

func startCloudStackVM(cs *cloudStackClient, vmID string) error {
	resp, err := cs.request("startVirtualMachine", map[string]string{
		"id": strings.TrimSpace(vmID),
	})
	if err != nil {
		return err
	}
	root, ok := mapGetMap(resp, "startvirtualmachineresponse")
	if !ok {
		return nil
	}
	return waitCloudStackJobIfPresent(cs, root, "startVirtualMachine")
}

func isCloudStackVMAlreadyRunningError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already running") ||
		strings.Contains(msg, "state of the vm") && strings.Contains(msg, "running")
}

func importVMToCloudStack(cs *cloudStackClient, vmName string, targetCloud cloudStackTargetSpec, bootDiskPath string, importNetworkID string) (string, error) {
	networkID := strings.TrimSpace(importNetworkID)
	if networkID == "" {
		networkID = strings.TrimSpace(targetCloud.NetworkID)
	}
	params := map[string]string{
		"name":              sanitizeHostName(vmName),
		"displayname":       vmName,
		"clusterid":         targetCloud.ClusterID,
		"zoneid":            targetCloud.ZoneID,
		"importsource":      "shared",
		"hypervisor":        "kvm",
		"storageid":         targetCloud.StorageID,
		"diskpath":          filepath.Base(bootDiskPath),
		"networkid":         networkID,
		"serviceofferingid": targetCloud.ServiceOfferingID,
	}
	resp, err := cs.request("importVm", params)
	if err != nil {
		return "", err
	}
	root, ok := mapGetMap(resp, "importvmresponse")
	if !ok {
		return "", fmt.Errorf("unexpected importVm response: %v", resp)
	}
	jobID := mapGetString(root, "jobid")
	jobRes, err := cs.waitJob(jobID, "importVm")
	if err != nil {
		return "", err
	}
	vmNode, ok := mapGetMap(jobRes, "virtualmachine")
	if !ok {
		return "", fmt.Errorf("importVm job result missing virtualmachine: %v", jobRes)
	}
	vmID := mapGetString(vmNode, "id")
	if vmID == "" {
		return "", fmt.Errorf("importVm job result missing vm id: %v", jobRes)
	}
	return vmID, nil
}

func attachNICToVM(cs *cloudStackClient, vmID, networkID string) error {
	resp, err := cs.request("addNicToVirtualMachine", map[string]string{
		"virtualmachineid": vmID,
		"networkid":        networkID,
	})
	if err != nil {
		return err
	}
	root, ok := mapGetMap(resp, "addnictovirtualmachineresponse")
	if !ok {
		return nil
	}
	jobID := mapGetString(root, "jobid")
	if jobID == "" {
		return nil
	}
	_, err = cs.waitJob(jobID, "addNicToVirtualMachine")
	return err
}

func importDataDiskToCloudStack(cs *cloudStackClient, zoneID, storageID, diskOfferingID, diskPath string) (string, error) {
	params := map[string]string{
		"name":           filepath.Base(diskPath),
		"zoneid":         zoneID,
		"diskofferingid": diskOfferingID,
		"storageid":      storageID,
		"path":           filepath.Base(diskPath),
	}
	resp, err := cs.request("importVolume", params)
	if err != nil {
		return "", err
	}
	root, ok := mapGetMap(resp, "importvolumeresponse")
	if !ok {
		return "", fmt.Errorf("unexpected importVolume response: %v", resp)
	}
	jobID := mapGetString(root, "jobid")
	jobRes, err := cs.waitJob(jobID, "importVolume")
	if err != nil {
		return "", err
	}
	volNode, ok := mapGetMap(jobRes, "volume")
	if !ok {
		return "", fmt.Errorf("importVolume job result missing volume: %v", jobRes)
	}
	volumeID := mapGetString(volNode, "id")
	if volumeID == "" {
		return "", fmt.Errorf("importVolume job result missing volume id: %v", jobRes)
	}
	return volumeID, nil
}

func attachVolumeToVM(cs *cloudStackClient, volumeID, vmID string) error {
	resp, err := cs.request("attachVolume", map[string]string{
		"id":               volumeID,
		"virtualmachineid": vmID,
	})
	if err != nil {
		return err
	}
	root, ok := mapGetMap(resp, "attachvolumeresponse")
	if !ok {
		return nil
	}
	jobID := mapGetString(root, "jobid")
	if jobID == "" {
		return nil
	}
	_, err = cs.waitJob(jobID, "attachVolume")
	return err
}

type runOptions struct {
	SpecPaths     []string
	ConfigPath    string
	Readers       int
	RunVirtV2V    bool
	OverrideV2V   bool
	ParallelVMs   int
	ParallelDisks int
}

type finalizeOptions struct {
	SpecPaths  []string
	ConfigPath string
	ControlDir string
	VMNames    []string
	VMMorefs   []string
	Immediate  bool
}

type statusOptions struct {
	SpecPaths  []string
	ConfigPath string
	ControlDir string
	VMNames    []string
	VMMorefs   []string
	JSON       bool
}

type vmRuntimeTarget struct {
	Spec         *runSpec
	ControlDir   string
	StatePath    string
	FinalizePath string
	State        *runState
}

type multiStringFlag []string

func (m *multiStringFlag) String() string {
	return strings.Join(*m, ",")
}

func (m *multiStringFlag) Set(v string) error {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	*m = append(*m, v)
	return nil
}

func sliceToSet(vals []string) map[string]struct{} {
	if len(vals) == 0 {
		return nil
	}
	out := map[string]struct{}{}
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		out[v] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func controlRootFromConfig(cfg *appConfig, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.TrimSpace(override)
	}
	if cfg != nil && strings.TrimSpace(cfg.Migration.ControlDir) != "" {
		return strings.TrimSpace(cfg.Migration.ControlDir)
	}
	return "/var/lib/vm-migrator"
}

func filterRunSpecsByVMNames(specs []*runSpec, vmNames map[string]struct{}) ([]*runSpec, error) {
	if len(specs) == 0 {
		return nil, errors.New("no VM specs provided")
	}
	if len(vmNames) == 0 {
		return specs, nil
	}

	filtered := make([]*runSpec, 0, len(specs))
	found := map[string]struct{}{}
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		name := strings.TrimSpace(spec.VM.Name)
		if name == "" {
			continue
		}
		if _, ok := vmNames[name]; ok {
			filtered = append(filtered, spec)
			found[name] = struct{}{}
		}
	}
	if len(filtered) == 0 {
		names := make([]string, 0, len(vmNames))
		for n := range vmNames {
			names = append(names, n)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("no VMs from --vm filters were found in spec(s): %s", strings.Join(names, ", "))
	}

	missing := make([]string, 0)
	for n := range vmNames {
		if _, ok := found[n]; !ok {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("some --vm filters were not present in spec(s): %s", strings.Join(missing, ", "))
	}
	return filtered, nil
}

func candidateVMRuntimeDirs(controlRoot, vmName string) []string {
	base := filepath.Clean(controlRoot)
	safe := safeVMName(vmName)
	seen := map[string]struct{}{}
	out := make([]string, 0)

	add := func(path string) {
		if strings.TrimSpace(path) == "" {
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

func loadRuntimeStateForDir(dir string) (*runState, string, error) {
	paths := []string{
		filepath.Join(dir, "state.json"),
		filepath.Join(dir, "state.engine.json"),
	}
	for _, p := range paths {
		st, err := loadRunState(p)
		if err != nil {
			return nil, "", err
		}
		if st != nil {
			return st, p, nil
		}
	}
	return nil, "", nil
}

func resolveRuntimeTarget(controlRoot string, spec *runSpec, vmMorefFilter map[string]struct{}, requireState bool) (*vmRuntimeTarget, error) {
	if spec == nil {
		return nil, errors.New("nil spec")
	}
	vmName := strings.TrimSpace(spec.VM.Name)
	if vmName == "" {
		return nil, errors.New("spec.vm.name is required")
	}

	candidates := candidateVMRuntimeDirs(controlRoot, vmName)
	matches := make([]*vmRuntimeTarget, 0)
	for _, dir := range candidates {
		st, statePath, err := loadRuntimeStateForDir(dir)
		if err != nil {
			return nil, fmt.Errorf("load state for %s: %w", dir, err)
		}
		if st == nil {
			continue
		}
		if strings.TrimSpace(st.VMName) != "" && strings.TrimSpace(st.VMName) != vmName {
			continue
		}
		if len(vmMorefFilter) > 0 {
			if _, ok := vmMorefFilter[strings.TrimSpace(st.VMMoref)]; !ok {
				continue
			}
		}
		matches = append(matches, &vmRuntimeTarget{
			Spec:         spec,
			ControlDir:   dir,
			StatePath:    statePath,
			FinalizePath: filepath.Join(dir, "FINALIZE"),
			State:        st,
		})
	}

	if len(matches) == 0 {
		if requireState {
			if len(vmMorefFilter) > 0 {
				return nil, nil
			}
			return nil, fmt.Errorf("no runtime state found for vm=%s under %s", vmName, controlRoot)
		}
		return &vmRuntimeTarget{
			Spec:         spec,
			FinalizePath: filepath.Join(controlRoot, safeVMName(vmName), "FINALIZE"),
		}, nil
	}

	if len(matches) > 1 {
		found := make([]string, 0, len(matches))
		for _, m := range matches {
			moref := ""
			if m.State != nil {
				moref = strings.TrimSpace(m.State.VMMoref)
			}
			if moref == "" {
				found = append(found, m.ControlDir)
			} else {
				found = append(found, fmt.Sprintf("%s (%s)", moref, m.ControlDir))
			}
		}
		sort.Strings(found)
		return nil, fmt.Errorf(
			"multiple runtime states found for vm=%s; pass --vm-id to disambiguate. matches: %s",
			vmName,
			strings.Join(found, ", "),
		)
	}
	return matches[0], nil
}

func effectiveRunVirtV2V(cfg *appConfig, spec *runSpec) bool {
	runVirtV2V := false
	if cfg != nil {
		runVirtV2V = cfg.Virt.RunVirtV2V
	}
	if spec != nil && spec.Migration.RunVirtV2V != nil {
		runVirtV2V = *spec.Migration.RunVirtV2V
	}
	return runVirtV2V
}

func nextStageForStatus(currentStage string, runVirtV2V bool, finalizeRequested bool) string {
	switch strings.TrimSpace(currentStage) {
	case "":
		return stageInit
	case "not_started":
		return stageInit
	case stageInit:
		return stageBaseCopy
	case stageBaseCopy:
		return stageDelta
	case stageDelta:
		if finalizeRequested {
			return "Finalize"
		}
		return stageDelta
	case stageFinalSync:
		if runVirtV2V {
			return stageConverting
		}
		return stageImportRoot
	case stageConverting:
		return stageImportRoot
	case stageImportRoot:
		return stageImportData
	case stageImportData:
		return stageDone
	case stageDone:
		return "none"
	default:
		return "unknown"
	}
}

type runLogger struct {
	mu sync.Mutex
	f  *os.File
}

func newRunLogger(controlDir string) (*runLogger, error) {
	logPath := filepath.Join(controlDir, "migration.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &runLogger{f: f}, nil
}

func (l *runLogger) Close() {
	if l == nil || l.f == nil {
		return
	}
	_ = l.f.Close()
}

func (l *runLogger) Printf(format string, args ...any) {
	if l == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("[%s] %s\n", time.Now().UTC().Format("2006-01-02 15:04:05"), msg)
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprint(os.Stderr, line)
	if l.f != nil {
		_, _ = l.f.WriteString(line)
	}
}

func recomputeStateProgress(st *runState) {
	total := 0.0
	speed := 0.0
	count := 0
	for _, d := range st.Disks {
		if d == nil {
			continue
		}
		total += d.Progress
		speed += d.SpeedMBps
		count++
	}
	if count == 0 {
		st.Progress = 0
		st.TransferSpeedMB = 0
		return
	}
	st.Progress = math.Round((total/float64(count))*100) / 100
	st.TransferSpeedMB = math.Round(speed*100) / 100
}

func changedBytes(ranges []changedRange) int64 {
	var out int64
	for _, r := range ranges {
		if r.Length > 0 {
			out += r.Length
		}
	}
	return out
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func makeProgressUpdater(
	stateMu *sync.Mutex,
	statePath string,
	st *runState,
	log *runLogger,
	unit int,
	totalBytes int64,
	stage string,
) func(copyStats) {
	unitKey := strconv.Itoa(unit)
	var lastSave time.Time
	var lastLog time.Time
	var lastPct float64 = -1
	return func(cs copyStats) {
		stateMu.Lock()
		defer stateMu.Unlock()

		ds := st.Disks[unitKey]
		if ds == nil {
			return
		}
		ds.Stage = stage
		ds.BytesRead = cs.BytesRead
		ds.BytesWritten = cs.BytesWritten
		ds.BytesZero = cs.BytesZeroSkipped
		if totalBytes > 0 {
			p := (float64(cs.BytesRead) * 100) / float64(totalBytes)
			if p < 0 {
				p = 0
			}
			if p > 100 {
				p = 100
			}
			ds.Progress = math.Round(p*100) / 100
			ds.QemuProgress = ds.Progress
		}
		if cs.ElapsedSec > 0 {
			ds.SpeedMBps = math.Round((float64(cs.BytesRead)/1024.0/1024.0/float64(cs.ElapsedSec))*100) / 100
		} else {
			ds.SpeedMBps = 0
		}
		ds.CopiedBytes = ds.BytesWritten
		remaining := totalBytes - cs.BytesRead
		if remaining > 0 && ds.SpeedMBps > 0 {
			ds.EtaSeconds = int64(float64(remaining) / (ds.SpeedMBps * 1024.0 * 1024.0))
		} else {
			ds.EtaSeconds = 0
		}
		recomputeStateProgress(st)

		if log != nil {
			shouldLog := lastLog.IsZero() || time.Since(lastLog) >= 10*time.Second
			if ds.Progress >= 100 || (ds.Progress-lastPct) >= 5 {
				shouldLog = true
			}
			if shouldLog {
				log.Printf(
					"[disk%d] %s progress=%.2f%% read=%d written=%d zero_skipped=%d speed=%.2fMB/s eta=%ds",
					unit,
					stage,
					ds.Progress,
					ds.BytesRead,
					ds.BytesWritten,
					ds.BytesZero,
					ds.SpeedMBps,
					ds.EtaSeconds,
				)
				lastLog = time.Now()
				lastPct = ds.Progress
			}
		}

		if lastSave.IsZero() || time.Since(lastSave) >= 2*time.Second || ds.Progress >= 100 {
			_ = saveRunState(statePath, st)
			lastSave = time.Now()
		}
	}
}

func runVMWorkflow(ctx context.Context, cfg *appConfig, spec *runSpec, opts runOptions) error {
	applyCloudStackDefaults(spec, cfg)
	if spec.VM.Name == "" {
		return errors.New("spec.vm.name is required")
	}
	if err := validateCloudStackTarget(spec.Target.CloudStack); err != nil {
		return err
	}

	csClient, err := newCloudStackClient(cfg)
	if err != nil {
		return err
	}

	client, err := connectVCenter(ctx, cfg)
	if err != nil {
		return err
	}

	vmObj, err := findVM(ctx, client, spec.VM.Name)
	if err != nil {
		return err
	}
	vmMoref := vmObj.Reference().Value
	migrationID := fmt.Sprintf("%s_%s", spec.VM.Name, vmMoref)
	controlRoot := strings.TrimSpace(cfg.Migration.ControlDir)
	if controlRoot == "" {
		controlRoot = "/var/lib/vm-migrator"
	}
	controlDir := filepath.Join(controlRoot, migrationID)
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		return err
	}
	log, err := newRunLogger(controlDir)
	if err != nil {
		return err
	}
	defer log.Close()
	if err := persistSpecForRun(controlDir, spec); err != nil {
		return err
	}

	statePath := filepath.Join(controlDir, "state.json")
	legacyStatePath := filepath.Join(controlDir, "state.engine.json")
	finalizeFile := filepath.Join(controlDir, "FINALIZE")
	finalizeNowFile := filepath.Join(controlDir, "FINALIZE_NOW")
	log.Printf("Starting workflow vm=%s moref=%s", spec.VM.Name, vmMoref)

	vmRef := vmObj.Reference()
	var vcMu sync.RWMutex
	var reconnectMu sync.Mutex

	reconnectVCenter := func(reason string) error {
		reconnectMu.Lock()
		defer reconnectMu.Unlock()

		log.Printf("vCenter reconnect requested (%s)", reason)
		newClient, err := connectVCenter(ctx, cfg)
		if err != nil {
			return err
		}
		newVM := object.NewVirtualMachine(newClient.Client, vmRef)

		vcMu.Lock()
		oldClient := client
		client = newClient
		vmObj = newVM
		vcMu.Unlock()

		if oldClient != nil {
			logoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = oldClient.Logout(logoutCtx)
			cancel()
		}
		log.Printf("vCenter reconnect completed")
		return nil
	}

	withVCenterRetry := func(op string, fn func(*govmomi.Client, *object.VirtualMachine) error) error {
		for attempt := 0; attempt < 2; attempt++ {
			vcMu.RLock()
			c := client
			v := vmObj
			vcMu.RUnlock()

			err := fn(c, v)
			if err == nil {
				return nil
			}
			if !isNotAuthenticatedError(err) || attempt == 1 {
				return err
			}
			log.Printf("%s hit expired vCenter session, reconnecting and retrying", op)
			if recErr := reconnectVCenter(op); recErr != nil {
				return fmt.Errorf("%s reconnect failed: %w (original error: %v)", op, recErr, err)
			}
		}
		return nil
	}

	keepaliveStop := make(chan struct{})
	defer close(keepaliveStop)
	go func() {
		tk := time.NewTicker(60 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-keepaliveStop:
				return
			case <-ctx.Done():
				return
			case <-tk.C:
				vcMu.RLock()
				v := vmObj
				vcMu.RUnlock()
				pingCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				_, err := v.PowerState(pingCtx)
				cancel()
				if err == nil {
					continue
				}
				if isNotAuthenticatedError(err) {
					log.Printf("vCenter keepalive detected expired session")
					if recErr := reconnectVCenter("keepalive"); recErr != nil {
						log.Printf("vCenter keepalive reconnect failed: %v", recErr)
					}
					continue
				}
				log.Printf("vCenter keepalive warning: %v", err)
			}
		}
	}()

	var disks []vmDisk
	bootUnit := 0
	if err := withVCenterRetry("list VM disks", func(c *govmomi.Client, v *object.VirtualMachine) error {
		var callErr error
		disks, bootUnit, callErr = listVMDisksAndBootUnit(ctx, c, v)
		return callErr
	}); err != nil {
		return err
	}

	readers := spec.Migration.Readers
	if readers <= 0 {
		readers = 4
	}
	if opts.Readers > 0 {
		readers = opts.Readers
	}

	parallelDisks := spec.Migration.ParallelDisks
	if parallelDisks <= 0 {
		parallelDisks = cfg.Migration.ParallelDisks
	}
	if parallelDisks <= 0 {
		parallelDisks = 4
	}
	if opts.ParallelDisks > 0 {
		parallelDisks = opts.ParallelDisks
	}
	if parallelDisks > len(disks) {
		parallelDisks = len(disks)
	}
	if parallelDisks <= 0 {
		parallelDisks = 1
	}

	runVirtV2V := cfg.Virt.RunVirtV2V
	if spec.Migration.RunVirtV2V != nil {
		runVirtV2V = *spec.Migration.RunVirtV2V
	}
	if opts.OverrideV2V {
		runVirtV2V = opts.RunVirtV2V
	}
	shutdownMode := strings.ToLower(strings.TrimSpace(cfg.Migration.ShutdownMode))
	if strings.TrimSpace(spec.Migration.ShutdownMode) != "" {
		shutdownMode = strings.ToLower(strings.TrimSpace(spec.Migration.ShutdownMode))
	}
	if shutdownMode == "" {
		shutdownMode = "auto"
	}
	virtioISO := strings.TrimSpace(cfg.Virt.VirtioISO)
	if virtioISO == "" {
		virtioISO = "/usr/share/virtio-win/virtio-win.iso"
	}
	quiesceMode := strings.ToLower(strings.TrimSpace(cfg.Migration.SnapshotQuiesce))
	if strings.TrimSpace(spec.Migration.SnapshotQuiesce) != "" {
		quiesceMode = strings.ToLower(strings.TrimSpace(spec.Migration.SnapshotQuiesce))
	}
	if quiesceMode == "" {
		quiesceMode = "auto"
	}
	importNetworkID, additionalNICs := buildCloudStackNICPlan(spec.Target.CloudStack)
	log.Printf(
		"CloudStack NIC plan primary_network=%s additional_nics=%d",
		importNetworkID,
		len(additionalNICs),
	)
	for _, nic := range additionalNICs {
		log.Printf("CloudStack additional NIC map=%s adapter=%s index=%d network=%s", nic.MapKey, nic.AdapterName, nic.SourceIndex, nic.NetworkID)
	}
	log.Printf(
		"Runtime settings readers=%d parallel_disks=%d run_virt_v2v=%t shutdown_mode=%s snapshot_quiesce=%s virtio_iso=%s",
		readers,
		parallelDisks,
		runVirtV2V,
		shutdownMode,
		quiesceMode,
		virtioISO,
	)

	thumb, err := getServerThumbprint(cfg.VCenter.Host)
	if err != nil {
		return err
	}

	st, err := loadRunState(statePath)
	if err != nil {
		return err
	}
	if st == nil {
		st, err = loadRunState(legacyStatePath)
		if err != nil {
			return err
		}
	}
	if st == nil {
		st = &runState{
			VMName:      spec.VM.Name,
			VMMoref:     vmMoref,
			MigrationID: migrationID,
			Stage:       stageInit,
			Disks:       map[string]*runDiskState{},
			AttachedNICs: map[string]string{},
		}
	}
	if st.VMName != "" && st.VMName != spec.VM.Name {
		return fmt.Errorf("state vm mismatch: state=%s spec=%s", st.VMName, spec.VM.Name)
	}
	if st.Disks == nil {
		st.Disks = map[string]*runDiskState{}
	}
	if st.AttachedNICs == nil {
		st.AttachedNICs = map[string]string{}
	}
	st.VMName = spec.VM.Name
	st.VMMoref = vmMoref
	st.MigrationID = migrationID
	if st.Stage == "" {
		st.Stage = stageInit
	}
	log.Printf("Resuming from stage: %s", st.Stage)

	stateMu := &sync.Mutex{}
	saveState := func() error {
		recomputeStateProgress(st)
		return saveRunState(statePath, st)
	}
	saveStateLocked := func() error {
		stateMu.Lock()
		defer stateMu.Unlock()
		return saveState()
	}
	setStage := func(next string) error {
		stateMu.Lock()
		st.Stage = next
		stateMu.Unlock()
		log.Printf("Stage: %s", next)
		return saveStateLocked()
	}
	ensureAdditionalNICsAttached := func(vmID string) error {
		if len(additionalNICs) == 0 {
			return nil
		}
		for _, nic := range additionalNICs {
			if strings.TrimSpace(nic.NetworkID) == "" {
				continue
			}
			stateMu.Lock()
			already := st.AttachedNICs[nic.MapKey]
			stateMu.Unlock()
			if already == nic.NetworkID {
				continue
			}
			log.Printf("Attaching additional NIC map=%s adapter=%s index=%d network=%s to vm_id=%s", nic.MapKey, nic.AdapterName, nic.SourceIndex, nic.NetworkID, vmID)
			if err := attachNICToVM(csClient, vmID, nic.NetworkID); err != nil {
				return fmt.Errorf("attach additional NIC failed map=%s network=%s: %w", nic.MapKey, nic.NetworkID, err)
			}
			stateMu.Lock()
			st.AttachedNICs[nic.MapKey] = nic.NetworkID
			stateMu.Unlock()
			if err := saveStateLocked(); err != nil {
				return err
			}
			log.Printf("Attached additional NIC map=%s to vm_id=%s", nic.MapKey, vmID)
		}
		return nil
	}

	for _, d := range disks {
		unitKey := strconv.Itoa(d.Unit)
		ds := st.Disks[unitKey]
		if ds == nil {
			ds = &runDiskState{
				Key:      d.Key,
				Unit:     d.Unit,
				Capacity: d.Capacity,
				Stage:    stageInit,
			}
			st.Disks[unitKey] = ds
		}
		ds.Key = d.Key
		ds.Unit = d.Unit
		ds.Capacity = d.Capacity
		if strings.TrimSpace(ds.SourceDiskPath) == "" && strings.TrimSpace(d.SourcePath) != "" {
			ds.SourceDiskPath = strings.TrimSpace(d.SourcePath)
		}

		storageID, err := resolveStorageID(spec, d.Unit, bootUnit)
		if err != nil {
			return err
		}
		ds.StorageID = storageID
		mountPath, err := ensureStorageMounted(csClient, storageID)
		if err != nil {
			return err
		}
		ds.TargetQCOW2 = filepath.Join(mountPath, fmt.Sprintf("%s_disk%d.qcow2", migrationID, d.Unit))
	}
	if err := saveStateLocked(); err != nil {
		return err
	}

	if st.Stage == stageDone {
		return nil
	}

	if st.Stage == stageInit || st.Stage == stageBaseCopy || st.Stage == stageDelta || st.Stage == stageFinalSync {
		if err := withVCenterRetry("ensure CBT enabled", func(c *govmomi.Client, v *object.VirtualMachine) error {
			return ensureCBTEnabled(ctx, c, v, log)
		}); err != nil {
			return fmt.Errorf("ensure CBT enabled failed: %w", err)
		}
	}

	if st.Stage == stageInit {
		var baseSnap types.ManagedObjectReference
		if err := withVCenterRetry("create base snapshot", func(c *govmomi.Client, v *object.VirtualMachine) error {
			var callErr error
			baseSnap, callErr = createSnapshotWithMode(ctx, c, v, "Migrate_Base_"+spec.VM.Name, quiesceMode, log)
			return callErr
		}); err != nil {
			return err
		}
		log.Printf("Created base snapshot: %s", baseSnap.Value)
		stateMu.Lock()
		st.ActiveSnapshot = baseSnap.Value
		stateMu.Unlock()
		if err := setStage(stageBaseCopy); err != nil {
			return err
		}
	}

	if st.Stage == stageBaseCopy {
		if strings.TrimSpace(st.ActiveSnapshot) == "" {
			var baseSnap types.ManagedObjectReference
			if err := withVCenterRetry("create base snapshot", func(c *govmomi.Client, v *object.VirtualMachine) error {
				var callErr error
				baseSnap, callErr = createSnapshotWithMode(ctx, c, v, "Migrate_Base_"+spec.VM.Name, quiesceMode, log)
				return callErr
			}); err != nil {
				return err
			}
			stateMu.Lock()
			st.ActiveSnapshot = baseSnap.Value
			stateMu.Unlock()
			if err := saveStateLocked(); err != nil {
				return err
			}
		}
		baseMeta := map[int32]snapshotDiskMeta{}
		if err := withVCenterRetry("read base snapshot metadata", func(c *govmomi.Client, _ *object.VirtualMachine) error {
			var callErr error
			baseMeta, callErr = snapshotDiskMetadata(
				ctx,
				c,
				types.ManagedObjectReference{Type: "VirtualMachineSnapshot", Value: st.ActiveSnapshot},
			)
			return callErr
		}); err != nil {
			return err
		}

		baseCtx, baseCancel := context.WithCancel(ctx)
		defer baseCancel()
		sem := make(chan struct{}, parallelDisks)
		var wg sync.WaitGroup
		var firstErr error
		var errMu sync.Mutex

		for _, d := range disks {
			unitKey := strconv.Itoa(d.Unit)
			ds := st.Disks[unitKey]
			if ds != nil && ds.BaseCopied && fileExists(ds.TargetQCOW2) {
				continue
			}
			meta, ok := baseMeta[d.Key]
			if !ok {
				return fmt.Errorf("snapshot metadata not found for disk key=%d", d.Key)
			}

			wg.Add(1)
			go func(d vmDisk, meta snapshotDiskMeta) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-baseCtx.Done():
					return
				}
				defer func() { <-sem }()

				sourcePath := strings.TrimSpace(d.SourcePath)
				if sourcePath == "" {
					sourcePath = strings.TrimSpace(meta.Path)
				}
				if sourcePath == "" {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("source disk path missing for unit=%d key=%d", d.Unit, d.Key)
						baseCancel()
					}
					errMu.Unlock()
					return
				}

				progress := makeProgressUpdater(stateMu, statePath, st, log, d.Unit, d.Capacity, stageBaseCopy)
				log.Printf("[disk%d] base copy started source=%s snapshot_path=%s target=%s", d.Unit, sourcePath, meta.Path, st.Disks[strconv.Itoa(d.Unit)].TargetQCOW2)
				optsBase := baseCopyOptions{
					VDDK: vddkConnCfg{
						LibDir:        cfg.Migration.VDDKPath,
						Server:        cfg.VCenter.Host,
						User:          cfg.VCenter.User,
						Password:      cfg.VCenter.Password,
						Thumbprint:    thumb,
						VMMoref:       vmMoref,
						SnapshotMoref: st.ActiveSnapshot,
					},
					DiskPath:      sourcePath,
					TargetQCOW2:   st.Disks[strconv.Itoa(d.Unit)].TargetQCOW2,
					DiskSizeBytes: d.Capacity,
					Readers:       readers,
					RunVirtV2V:    false,
					OnProgress:    progress,
				}
				stats, err := runBaseCopy(baseCtx, optsBase)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("base copy failed for unit=%d: %w", d.Unit, err)
						baseCancel()
					}
					errMu.Unlock()
					return
				}

				stateMu.Lock()
				ds := st.Disks[strconv.Itoa(d.Unit)]
				ds.SourceDiskPath = sourcePath
				ds.ChangeID = meta.ChangeID
				ds.Stage = stageBaseCopy
				ds.BaseCopied = true
				ds.Progress = 100
				ds.BytesRead = stats.BytesRead
				ds.BytesWritten = stats.BytesWritten
				ds.BytesZero = stats.BytesZeroSkipped
				ds.EtaSeconds = 0
				recomputeStateProgress(st)
				_ = saveRunState(statePath, st)
				stateMu.Unlock()
				log.Printf("[disk%d] base copy completed read=%d written=%d zero_skipped=%d", d.Unit, stats.BytesRead, stats.BytesWritten, stats.BytesZeroSkipped)
			}(d, meta)
		}
		wg.Wait()
		if firstErr != nil {
			stateMu.Lock()
			st.LastError = firstErr.Error()
			_ = saveRunState(statePath, st)
			stateMu.Unlock()
			return firstErr
		}
		stateMu.Lock()
		st.LastError = ""
		stateMu.Unlock()
		if err := setStage(stageDelta); err != nil {
			return err
		}
	}

	deltaInterval := spec.Migration.DeltaInterval
	if deltaInterval <= 0 {
		deltaInterval = 300
	}
	finalizeDeltaInterval := spec.Migration.FinalizeDeltaInterval
	if finalizeDeltaInterval <= 0 {
		finalizeDeltaInterval = 30
	}
	finalizeWindow := spec.Migration.FinalizeWindow
	if finalizeWindow <= 0 {
		finalizeWindow = 600
	}
	finalizeAt, err := parseFinalizeAt(spec.Migration.FinalizeAt)
	if err != nil {
		return fmt.Errorf("invalid finalize_at: %w", err)
	}

	runDeltaRound := func(stageName string) error {
		var newSnap types.ManagedObjectReference
		if err := withVCenterRetry("create delta snapshot", func(c *govmomi.Client, v *object.VirtualMachine) error {
			var callErr error
			newSnap, callErr = createSnapshotWithMode(
				ctx,
				c,
				v,
				fmt.Sprintf("Migrate_Delta_%s_%d", spec.VM.Name, time.Now().Unix()),
				quiesceMode,
				log,
			)
			return callErr
		}); err != nil {
			return err
		}
		log.Printf("Created delta snapshot (%s): %s", stageName, newSnap.Value)
		newMeta := map[int32]snapshotDiskMeta{}
		if err := withVCenterRetry("read delta snapshot metadata", func(c *govmomi.Client, _ *object.VirtualMachine) error {
			var callErr error
			newMeta, callErr = snapshotDiskMetadata(ctx, c, newSnap)
			return callErr
		}); err != nil {
			return err
		}

		deltaCtx, deltaCancel := context.WithCancel(ctx)
		defer deltaCancel()
		sem := make(chan struct{}, parallelDisks)
		var wg sync.WaitGroup
		var firstErr error
		var errMu sync.Mutex

		for _, d := range disks {
			meta, ok := newMeta[d.Key]
			if !ok {
				return fmt.Errorf("missing delta snapshot metadata for key=%d", d.Key)
			}
			wg.Add(1)
			go func(d vmDisk, meta snapshotDiskMeta) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-deltaCtx.Done():
					return
				}
				defer func() { <-sem }()

				unitKey := strconv.Itoa(d.Unit)
				stateMu.Lock()
				ds := st.Disks[unitKey]
				prevChangeID := ""
				targetQCOW2 := ""
				// Delta data must be read from the current snapshot's backing path.
				// Using an older persisted path can read stale blocks even when CBT
				// correctly reports changed ranges.
				sourcePath := strings.TrimSpace(meta.Path)
				if ds != nil {
					prevChangeID = ds.ChangeID
					targetQCOW2 = ds.TargetQCOW2
					if sourcePath == "" && strings.TrimSpace(ds.SourceDiskPath) != "" {
						sourcePath = strings.TrimSpace(ds.SourceDiskPath)
					}
				}
				stateMu.Unlock()
				if ds == nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("missing disk state for unit=%d", d.Unit)
						deltaCancel()
					}
					errMu.Unlock()
					return
				}
				if strings.TrimSpace(prevChangeID) == "" {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("missing previous changeID for unit=%d", d.Unit)
						deltaCancel()
					}
					errMu.Unlock()
					return
				}
				if sourcePath == "" {
					sourcePath = strings.TrimSpace(d.SourcePath)
				}
				if sourcePath == "" {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("source disk path missing for delta unit=%d key=%d", d.Unit, d.Key)
						deltaCancel()
					}
					errMu.Unlock()
					return
				}
				var ranges []changedRange
				err := withVCenterRetry(
					fmt.Sprintf("query changed ranges unit=%d", d.Unit),
					func(c *govmomi.Client, v *object.VirtualMachine) error {
						var callErr error
						ranges, callErr = queryChangedRanges(ctx, c, v.Reference(), newSnap, d.Key, prevChangeID, d.Capacity)
						return callErr
					},
				)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						deltaCancel()
					}
					errMu.Unlock()
					return
				}
				log.Printf(
					"[disk%d] %s cbt ranges=%d changed_bytes=%d source=%s",
					d.Unit,
					stageName,
					len(ranges),
					changedBytes(ranges),
					sourcePath,
				)
				if len(ranges) > 0 {
					rangesPath, err := writeRangesTempFile(ranges, d.Unit)
					if err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = err
							deltaCancel()
						}
						errMu.Unlock()
						return
					}
					totalChanged := changedBytes(ranges)
					if totalChanged <= 0 {
						totalChanged = d.Capacity
					}
					progress := makeProgressUpdater(stateMu, statePath, st, log, d.Unit, totalChanged, stageName)
					_, err = runDeltaSync(deltaCtx, deltaSyncOptions{
						VDDK: vddkConnCfg{
							LibDir:        cfg.Migration.VDDKPath,
							Server:        cfg.VCenter.Host,
							User:          cfg.VCenter.User,
							Password:      cfg.VCenter.Password,
							Thumbprint:    thumb,
							VMMoref:       vmMoref,
							SnapshotMoref: newSnap.Value,
						},
						DiskPath:    sourcePath,
						TargetQCOW2: targetQCOW2,
						RangesFile:  rangesPath,
						Readers:     readers,
						OnProgress:  progress,
					})
					_ = os.Remove(rangesPath)
					if err != nil {
						errMu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("delta sync failed for unit=%d: %w", d.Unit, err)
							deltaCancel()
						}
						errMu.Unlock()
						return
					}
				}

				stateMu.Lock()
				ds = st.Disks[unitKey]
				ds.ChangeID = meta.ChangeID
				ds.SourceDiskPath = sourcePath
				ds.Stage = stageName
				ds.Progress = 100
				ds.EtaSeconds = 0
				ds.DeltaRounds++
				recomputeStateProgress(st)
				_ = saveRunState(statePath, st)
				stateMu.Unlock()
			}(d, meta)
		}

		wg.Wait()
		if firstErr != nil {
			stateMu.Lock()
			st.LastError = firstErr.Error()
			_ = saveRunState(statePath, st)
			stateMu.Unlock()
			return firstErr
		}

		var prevSnapshot string
		stateMu.Lock()
		prevSnapshot = st.ActiveSnapshot
		st.ActiveSnapshot = newSnap.Value
		st.LastError = ""
		_ = saveRunState(statePath, st)
		stateMu.Unlock()

		if prevSnapshot != "" && prevSnapshot != newSnap.Value {
			if err := withVCenterRetry("remove previous snapshot", func(c *govmomi.Client, _ *object.VirtualMachine) error {
				return removeSnapshot(ctx, c, types.ManagedObjectReference{Type: "VirtualMachineSnapshot", Value: prevSnapshot})
			}); err != nil {
				fmt.Fprintf(os.Stderr, "[run] warning: failed to remove snapshot %s: %v\n", prevSnapshot, err)
				log.Printf("Warning: failed to remove snapshot %s: %v", prevSnapshot, err)
			} else {
				log.Printf("Removed previous snapshot: %s", prevSnapshot)
			}
		}
		return nil
	}

	if st.Stage == stageDelta || st.Stage == stageFinalSync {
		firstDeltaDelayDone := false
		stateMu.Lock()
		if st.Stage != stageDelta {
			firstDeltaDelayDone = true
		} else {
			for _, ds := range st.Disks {
				if ds != nil && ds.DeltaRounds > 0 {
					firstDeltaDelayDone = true
					break
				}
			}
		}
		stateMu.Unlock()

		computeDeltaSleep := func() int {
			sleepSec := deltaInterval
			if !finalizeAt.IsZero() {
				remaining := int(time.Until(finalizeAt).Seconds())
				if remaining <= 0 {
					return 0
				}
				if remaining <= finalizeWindow {
					sleepSec = finalizeDeltaInterval
				}
				if remaining < sleepSec {
					sleepSec = remaining
				}
			}
			if sleepSec < 0 {
				return 0
			}
			return sleepSec
		}
		waitDeltaSleep := func(sleepSec int) bool {
			if sleepSec <= 0 {
				return false
			}
			remaining := time.Duration(sleepSec) * time.Second
			for remaining > 0 {
				if fileExists(finalizeNowFile) {
					return true
				}
				wait := time.Second
				if remaining < wait {
					wait = remaining
				}
				time.Sleep(wait)
				remaining -= wait
			}
			return false
		}

		for {
			finalizeNow := st.Stage == stageFinalSync
			if !finalizeNow && !finalizeAt.IsZero() && time.Now().After(finalizeAt) {
				finalizeNow = true
			}
			if !finalizeNow {
				if fileExists(finalizeFile) || fileExists(finalizeNowFile) {
					finalizeNow = true
				}
			}

			if !finalizeNow && !firstDeltaDelayDone && st.Stage == stageDelta {
				sleepSec := computeDeltaSleep()
				firstDeltaDelayDone = true
				if sleepSec > 0 {
					log.Printf("Waiting %ds before first delta sync round", sleepSec)
					if waitDeltaSleep(sleepSec) {
						log.Printf("FINALIZE NOW detected, interrupting delta wait")
					}
					continue
				}
			}

			if finalizeNow {
				log.Printf("FINALIZE detected, ensuring source VM is powered off (mode=%s)", shutdownMode)
				if err := withVCenterRetry("shutdown source VM for finalize", func(c *govmomi.Client, v *object.VirtualMachine) error {
					return shutdownVMForFinalize(ctx, c, v, shutdownMode, log)
				}); err != nil {
					return fmt.Errorf("failed to shutdown source VM before final sync: %w", err)
				}
				if err := setStage(stageFinalSync); err != nil {
					return err
				}
				log.Printf("Running final delta sync on powered-off source VM")
				if err := runDeltaRound(stageFinalSync); err != nil {
					return err
				}
				break
			}

			if err := setStage(stageDelta); err != nil {
				return err
			}
			log.Printf("Running delta sync round")
			if err := runDeltaRound(stageDelta); err != nil {
				return err
			}

			sleepSec := computeDeltaSleep()
			if sleepSec <= 0 {
				continue
			}
			if waitDeltaSleep(sleepSec) {
				log.Printf("FINALIZE NOW detected, interrupting delta wait")
			}
		}
		nextStage := stageImportRoot
		if runVirtV2V {
			nextStage = stageConverting
		}
		if err := setStage(nextStage); err != nil {
			return err
		}
	}

	if st.Stage == stageImportRoot && runVirtV2V && !st.VirtV2VDone && strings.TrimSpace(st.CloudStackVMID) == "" {
		log.Printf("Root import stage reached without virt-v2v completion; moving to converting stage")
		if err := setStage(stageConverting); err != nil {
			return err
		}
	}

	if st.Stage == stageConverting {
		if runVirtV2V {
			v2vDiskPaths, err := buildVirtV2VDiskPaths(disks, st.Disks, bootUnit)
			if err != nil {
				return err
			}
			log.Printf("Running virt-v2v-in-place with %d disk(s); boot disk first: %s", len(v2vDiskPaths), v2vDiskPaths[0])
			if err := runVirtV2VInPlace(v2vDiskPaths, virtioISO); err != nil {
				return fmt.Errorf("virt-v2v-in-place failed: %w", err)
			}
			log.Printf("virt-v2v-in-place completed for %d disk(s)", len(v2vDiskPaths))
			stateMu.Lock()
			st.VirtV2VDone = true
			stateMu.Unlock()
		} else {
			log.Printf("Skipping converting stage because run_virt_v2v is false")
			stateMu.Lock()
			st.VirtV2VDone = false
			stateMu.Unlock()
		}
		if err := setStage(stageImportRoot); err != nil {
			return err
		}
	}

	if st.Stage == stageImportRoot {
		if st.CloudStackVMID == "" {
			bootDiskState := st.Disks[strconv.Itoa(bootUnit)]
			if bootDiskState == nil {
				return fmt.Errorf("boot disk state not found for unit=%d", bootUnit)
			}
			vmID, err := importVMToCloudStack(csClient, spec.VM.Name, spec.Target.CloudStack, bootDiskState.TargetQCOW2, importNetworkID)
			if err != nil {
				return fmt.Errorf("cloudstack root import failed: %w", err)
			}
			log.Printf("Imported root disk as VM in CloudStack vm_id=%s", vmID)
			stateMu.Lock()
			st.CloudStackVMID = vmID
			st.LastError = ""
			stateMu.Unlock()
			if err := ensureAdditionalNICsAttached(vmID); err != nil {
				return err
			}
			if err := setStage(stageImportData); err != nil {
				return err
			}
		} else {
			if err := ensureAdditionalNICsAttached(st.CloudStackVMID); err != nil {
				return err
			}
			if err := setStage(stageImportData); err != nil {
				return err
			}
		}
	}

	if st.Stage == stageImportData {
		if strings.TrimSpace(st.CloudStackVMID) == "" {
			return errors.New("state is import_data_disk but cloudstack_vm_id is empty")
		}
		if err := ensureAdditionalNICsAttached(st.CloudStackVMID); err != nil {
			return err
		}
		units := make([]int, 0, len(disks))
		for _, d := range disks {
			if d.Unit != bootUnit {
				units = append(units, d.Unit)
			}
		}
		sort.Ints(units)
		for _, unit := range units {
			ds := st.Disks[strconv.Itoa(unit)]
			if ds == nil {
				return fmt.Errorf("data disk state missing for unit=%d", unit)
			}
			if ds.VolumeID != "" && ds.AttachedToVMID == st.CloudStackVMID {
				continue
			}
			storageID, diskOfferingID, err := resolveDataDiskConfig(spec, unit)
			if err != nil {
				return err
			}
			volumeID, err := importDataDiskToCloudStack(
				csClient,
				spec.Target.CloudStack.ZoneID,
				storageID,
				diskOfferingID,
				ds.TargetQCOW2,
			)
			if err != nil {
				return fmt.Errorf("cloudstack data disk import failed for unit=%d: %w", unit, err)
			}
			log.Printf("[disk%d] imported data volume volume_id=%s", unit, volumeID)
			if err := attachVolumeToVM(csClient, volumeID, st.CloudStackVMID); err != nil {
				return fmt.Errorf("attach volume failed for unit=%d: %w", unit, err)
			}
			log.Printf("[disk%d] attached data volume %s to vm_id=%s", unit, volumeID, st.CloudStackVMID)
			stateMu.Lock()
			ds.StorageID = storageID
			ds.DiskOfferingID = diskOfferingID
			ds.VolumeID = volumeID
			ds.AttachedToVMID = st.CloudStackVMID
			ds.Stage = stageImportData
			ds.Progress = 100
			stateMu.Unlock()
			if err := saveStateLocked(); err != nil {
				return err
			}
		}
		if !st.CloudStackConfigured {
			if err := updateCloudStackVMSettings(csClient, st.CloudStackVMID, spec.Target.CloudStack); err != nil {
				return fmt.Errorf("apply imported VM settings failed: %w", err)
			}
			if settings := buildCloudStackVMDetails(spec.Target.CloudStack); len(settings) > 0 || strings.TrimSpace(spec.Target.CloudStack.OSTypeID) != "" {
				log.Printf(
					"Applied CloudStack VM settings vm_id=%s ostypeid=%s uefi=%s rootdiskcontroller=%s nicadapter=%s",
					st.CloudStackVMID,
					strings.TrimSpace(spec.Target.CloudStack.OSTypeID),
					settings["UEFI"],
					strings.TrimSpace(spec.Target.CloudStack.RootDiskController),
					strings.TrimSpace(spec.Target.CloudStack.NICAdapter),
				)
			}
			stateMu.Lock()
			st.CloudStackConfigured = true
			stateMu.Unlock()
			if err := saveStateLocked(); err != nil {
				return err
			}
		}
		if spec.Migration.StartVMAfterImport && !st.CloudStackStarted {
			log.Printf("Starting imported CloudStack VM vm_id=%s", st.CloudStackVMID)
			if err := startCloudStackVM(csClient, st.CloudStackVMID); err != nil && !isCloudStackVMAlreadyRunningError(err) {
				return fmt.Errorf("start imported VM failed: %w", err)
			}
			log.Printf("Started imported CloudStack VM vm_id=%s", st.CloudStackVMID)
			stateMu.Lock()
			st.CloudStackStarted = true
			stateMu.Unlock()
			if err := saveStateLocked(); err != nil {
				return err
			}
		}
		if err := setStage(stageDone); err != nil {
			return err
		}
		log.Printf("Workflow completed successfully")
		return nil
	}

	return nil
}

func runWorkflow(ctx context.Context, opts runOptions) error {
	if opts.ConfigPath == "" {
		opts.ConfigPath = defaultConfigPath()
	}
	cfg, err := loadAppConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	if cfg.VCenter.Password == "" {
		cfg.VCenter.Password = os.Getenv("VC_PASSWORD")
	}
	if cfg.VCenter.Password == "" {
		return errors.New("missing vcenter password in config and VC_PASSWORD env")
	}
	if strings.TrimSpace(cfg.Migration.VDDKPath) == "" {
		return errors.New("config migration.vddk_path is required")
	}

	specs, err := loadRunSpecs(opts.SpecPaths)
	if err != nil {
		return err
	}
	if len(specs) == 0 {
		return errors.New("no VM specs provided")
	}

	parallelVMs := cfg.Migration.ParallelVMs
	if parallelVMs <= 0 {
		parallelVMs = 3
	}
	if opts.ParallelVMs > 0 {
		parallelVMs = opts.ParallelVMs
	}
	if parallelVMs > len(specs) {
		parallelVMs = len(specs)
	}
	if parallelVMs <= 0 {
		parallelVMs = 1
	}

	sem := make(chan struct{}, parallelVMs)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	errs := make([]error, 0)

	for _, spec := range specs {
		spec := spec
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			fmt.Fprintf(os.Stderr, "[run] starting VM workflow: %s\n", spec.VM.Name)
			if err := runVMWorkflow(ctx, cfg, spec, opts); err != nil {
				errMu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", spec.VM.Name, err))
				errMu.Unlock()
				return
			}
			fmt.Fprintf(os.Stderr, "[run] completed VM workflow: %s\n", spec.VM.Name)
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		if err := cmdRun(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
			os.Exit(1)
		}
	case "status":
		if err := cmdStatus(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "status failed: %v\n", err)
			os.Exit(1)
		}
	case "finalize":
		if err := cmdFinalize(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "finalize failed: %v\n", err)
			os.Exit(1)
		}
	case "serve":
		if err := cmdServe(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "serve failed: %v\n", err)
			os.Exit(1)
		}
	case "base-copy":
		if !expertCommandsEnabled() {
			fmt.Fprintf(os.Stderr, "base-copy is an internal expert command. Use 'run' for normal migrations.\n")
			fmt.Fprintf(os.Stderr, "To enable expert commands, set V2C_ENABLE_EXPERT_COMMANDS=1.\n")
			os.Exit(2)
		}
		if err := cmdBaseCopy(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "base-copy failed: %v\n", err)
			os.Exit(1)
		}
	case "delta-sync":
		if !expertCommandsEnabled() {
			fmt.Fprintf(os.Stderr, "delta-sync is an internal expert command. Use 'run' for normal migrations.\n")
			fmt.Fprintf(os.Stderr, "To enable expert commands, set V2C_ENABLE_EXPERT_COMMANDS=1.\n")
			os.Exit(2)
		}
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
	fmt.Fprintf(os.Stderr, "  v2c-engine run        --spec /path/spec.yaml [--spec /path/spec2.yaml] [--config /path/config.yaml]\n")
	fmt.Fprintf(os.Stderr, "  v2c-engine status     --spec /path/spec.yaml [--vm VM_NAME] [--vm-id VM_MOREF] [--config /path/config.yaml]\n")
	fmt.Fprintf(os.Stderr, "  v2c-engine finalize   --spec /path/spec.yaml [--vm VM_NAME] [--vm-id VM_MOREF] [--now] [--config /path/config.yaml]\n")
	fmt.Fprintf(os.Stderr, "  v2c-engine serve      [--config /path/config.yaml] [--listen :8000]\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Note: base-copy and delta-sync are internal expert commands.\n")
	fmt.Fprintf(os.Stderr, "Set V2C_ENABLE_EXPERT_COMMANDS=1 to use them directly.\n")
}

func expertCommandsEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("V2C_ENABLE_EXPERT_COMMANDS"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func cmdRun(args []string) error {
	var opts runOptions
	var specs multiStringFlag
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.Var(&specs, "spec", "Python-style VM migration spec YAML (repeatable)")
	fs.StringVar(&opts.ConfigPath, "config", defaultConfigPath(), "Global config.yaml path")
	fs.IntVar(&opts.Readers, "readers", 0, "Override readers from spec migration.readers")
	fs.BoolVar(&opts.RunVirtV2V, "run-virt-v2v", false, "Override spec migration.run_virt_v2v")
	fs.BoolVar(&opts.OverrideV2V, "override-run-virt-v2v", false, "If true, force use of --run-virt-v2v")
	fs.IntVar(&opts.ParallelVMs, "parallel-vms", 0, "Max VM workflows to run in parallel")
	fs.IntVar(&opts.ParallelDisks, "parallel-disks", 0, "Max per-VM disk copy/sync workers")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts.SpecPaths = specs
	if extra := fs.Args(); len(extra) > 0 {
		opts.SpecPaths = append(opts.SpecPaths, extra...)
	}
	if len(opts.SpecPaths) == 0 {
		return errors.New("run requires at least one --spec <file>")
	}
	return runWorkflow(context.Background(), opts)
}

func cmdFinalize(args []string) error {
	var opts finalizeOptions
	var specs multiStringFlag
	var vmNames multiStringFlag
	var vmMorefs multiStringFlag

	fs := flag.NewFlagSet("finalize", flag.ContinueOnError)
	fs.Var(&specs, "spec", "VM migration spec YAML (repeatable)")
	fs.StringVar(&opts.ConfigPath, "config", defaultConfigPath(), "Global config.yaml path")
	fs.StringVar(&opts.ControlDir, "control-dir", "", "Control directory root (override config migration.control_dir)")
	fs.Var(&vmNames, "vm", "VM name to finalize (repeatable)")
	fs.Var(&vmMorefs, "vm-id", "VM MoRef to disambiguate target (repeatable)")
	fs.BoolVar(&opts.Immediate, "now", false, "Trigger finalize immediately (do not wait for next delta interval)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts.SpecPaths = specs
	opts.VMNames = vmNames
	opts.VMMorefs = vmMorefs
	if extra := fs.Args(); len(extra) > 0 {
		opts.SpecPaths = append(opts.SpecPaths, extra...)
	}
	if len(opts.SpecPaths) == 0 {
		return errors.New("finalize requires at least one --spec <file>")
	}

	cfg, err := loadAppConfig(opts.ConfigPath)
	if err != nil {
		if strings.TrimSpace(opts.ControlDir) == "" {
			return err
		}
		cfg = &appConfig{}
	}
	controlRoot := controlRootFromConfig(cfg, opts.ControlDir)

	specList, err := loadRunSpecs(opts.SpecPaths)
	if err != nil {
		return err
	}
	specList, err = filterRunSpecsByVMNames(specList, sliceToSet(opts.VMNames))
	if err != nil {
		return err
	}

	vmMorefFilter := sliceToSet(opts.VMMorefs)
	processed := map[string]struct{}{}
	count := 0
	errs := make([]error, 0)

	for _, spec := range specList {
		target, err := resolveRuntimeTarget(controlRoot, spec, vmMorefFilter, true)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if target == nil {
			continue
		}
		if strings.TrimSpace(target.ControlDir) == "" {
			errs = append(errs, fmt.Errorf("runtime target not found for vm=%s", spec.VM.Name))
			continue
		}
		if _, seen := processed[target.ControlDir]; seen {
			continue
		}
		processed[target.ControlDir] = struct{}{}

		stage := ""
		if target.State != nil {
			stage = strings.TrimSpace(target.State.Stage)
		}
		if stage == stageDone {
			fmt.Fprintf(os.Stdout, "[finalize] vm=%s moref=%s stage=%s already completed\n", spec.VM.Name, target.State.VMMoref, stageDone)
			count++
			continue
		}

		touchMarker := func(path string) error {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
			_ = f.Close()
			now := time.Now()
			return os.Chtimes(path, now, now)
		}
		already := fileExists(target.FinalizePath)
		if err := touchMarker(target.FinalizePath); err != nil {
			errs = append(errs, fmt.Errorf("create finalize marker for vm=%s: %w", spec.VM.Name, err))
			continue
		}
		if opts.Immediate {
			finalizeNowPath := filepath.Join(target.ControlDir, "FINALIZE_NOW")
			alreadyNow := fileExists(finalizeNowPath)
			if err := touchMarker(finalizeNowPath); err != nil {
				errs = append(errs, fmt.Errorf("create finalize-now marker for vm=%s: %w", spec.VM.Name, err))
				continue
			}
			if alreadyNow {
				fmt.Fprintf(os.Stdout, "[finalize] vm=%s moref=%s finalize-now already requested (%s)\n", spec.VM.Name, target.State.VMMoref, finalizeNowPath)
			} else {
				fmt.Fprintf(os.Stdout, "[finalize] vm=%s moref=%s finalize-now requested (%s)\n", spec.VM.Name, target.State.VMMoref, finalizeNowPath)
			}
		} else if already {
			fmt.Fprintf(os.Stdout, "[finalize] vm=%s moref=%s finalize already requested (%s)\n", spec.VM.Name, target.State.VMMoref, target.FinalizePath)
		} else {
			fmt.Fprintf(os.Stdout, "[finalize] vm=%s moref=%s finalize requested (%s)\n", spec.VM.Name, target.State.VMMoref, target.FinalizePath)
		}
		count++
	}

	if count == 0 && len(errs) == 0 {
		return errors.New("no matching VM runtime found for finalize request")
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func cmdStatus(args []string) error {
	var opts statusOptions
	var specs multiStringFlag
	var vmNames multiStringFlag
	var vmMorefs multiStringFlag

	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.Var(&specs, "spec", "VM migration spec YAML (repeatable)")
	fs.StringVar(&opts.ConfigPath, "config", defaultConfigPath(), "Global config.yaml path")
	fs.StringVar(&opts.ControlDir, "control-dir", "", "Control directory root (override config migration.control_dir)")
	fs.Var(&vmNames, "vm", "VM name filter (repeatable)")
	fs.Var(&vmMorefs, "vm-id", "VM MoRef filter (repeatable)")
	fs.BoolVar(&opts.JSON, "json", false, "Print status as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts.SpecPaths = specs
	opts.VMNames = vmNames
	opts.VMMorefs = vmMorefs
	if extra := fs.Args(); len(extra) > 0 {
		opts.SpecPaths = append(opts.SpecPaths, extra...)
	}
	if len(opts.SpecPaths) == 0 {
		return errors.New("status requires at least one --spec <file>")
	}

	cfg, err := loadAppConfig(opts.ConfigPath)
	if err != nil {
		if strings.TrimSpace(opts.ControlDir) == "" {
			return err
		}
		cfg = &appConfig{}
	}
	controlRoot := controlRootFromConfig(cfg, opts.ControlDir)

	specList, err := loadRunSpecs(opts.SpecPaths)
	if err != nil {
		return err
	}
	specList, err = filterRunSpecsByVMNames(specList, sliceToSet(opts.VMNames))
	if err != nil {
		return err
	}

	vmMorefFilter := sliceToSet(opts.VMMorefs)
	rows := make([]map[string]any, 0)
	seen := map[string]struct{}{}

	for _, spec := range specList {
		target, err := resolveRuntimeTarget(controlRoot, spec, vmMorefFilter, false)
		if err != nil {
			return err
		}
		if target != nil && target.State != nil && strings.TrimSpace(target.ControlDir) != "" {
			if _, ok := seen[target.ControlDir]; ok {
				continue
			}
			seen[target.ControlDir] = struct{}{}
		}

		vmName := spec.VM.Name
		vmMoref := ""
		currentStage := "not_started"
		progress := float64(0)
		finalizeRequested := false
		finalizeNowRequested := false
		runVirt := effectiveRunVirtV2V(cfg, spec)

		if target != nil && target.State != nil {
			vmMoref = strings.TrimSpace(target.State.VMMoref)
			currentStage = strings.TrimSpace(target.State.Stage)
			if currentStage == "" {
				currentStage = stageInit
			}
			progress = target.State.Progress
			if strings.TrimSpace(target.FinalizePath) != "" {
				finalizeRequested = fileExists(target.FinalizePath)
				finalizeNowPath := filepath.Join(target.ControlDir, "FINALIZE_NOW")
				finalizeNowRequested = fileExists(finalizeNowPath)
				if finalizeNowRequested {
					finalizeRequested = true
				}
			}
		}

		nextStage := nextStageForStatus(currentStage, runVirt, finalizeRequested)
		row := map[string]any{
			"vm_name":            vmName,
			"vm_moref":           emptyToNil(vmMoref),
			"current_stage":      currentStage,
			"next_stage":         nextStage,
			"finalize_requested": finalizeRequested,
			"finalize_now_requested": finalizeNowRequested,
			"progress":           progress,
		}
		if target != nil {
			row["control_dir"] = emptyToNil(strings.TrimSpace(target.ControlDir))
			row["state_file"] = emptyToNil(strings.TrimSpace(target.StatePath))
		}
		rows = append(rows, row)
	}

	if len(vmMorefFilter) > 0 {
		filtered := make([]map[string]any, 0, len(rows))
		for _, r := range rows {
			moref, _ := r["vm_moref"].(string)
			if moref == "" {
				continue
			}
			if _, ok := vmMorefFilter[moref]; ok {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	if len(rows) == 0 {
		return errors.New("no matching VM status found")
	}

	sort.Slice(rows, func(i, j int) bool {
		li := fmt.Sprintf("%v", rows[i]["vm_name"])
		lj := fmt.Sprintf("%v", rows[j]["vm_name"])
		if li == lj {
			mi := fmt.Sprintf("%v", rows[i]["vm_moref"])
			mj := fmt.Sprintf("%v", rows[j]["vm_moref"])
			return mi < mj
		}
		return li < lj
	})

	if opts.JSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rows)
	}

	for _, row := range rows {
		fmt.Fprintf(
			os.Stdout,
			"[status] vm=%s moref=%v current=%s next=%s finalize_requested=%t finalize_now_requested=%t progress=%.2f%%\n",
			row["vm_name"],
			row["vm_moref"],
			row["current_stage"],
			row["next_stage"],
			row["finalize_requested"],
			row["finalize_now_requested"],
			row["progress"],
		)
	}
	return nil
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
	fs.StringVar(&o.VDDK.Thumbprint, "thumbprint", o.VDDK.Thumbprint, "SSL thumbprint (optional; auto-detected if empty)")
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
	if err := ensureVDDKThumbprint(&o.VDDK); err != nil {
		return err
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
	fs.StringVar(&o.VDDK.Thumbprint, "thumbprint", o.VDDK.Thumbprint, "SSL thumbprint (optional; auto-detected if empty)")
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
	if err := ensureVDDKThumbprint(&o.VDDK); err != nil {
		return err
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
	if o.VDDK.LibDir == "" || o.VDDK.Server == "" || o.VDDK.User == "" ||
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
	if o.VDDK.LibDir == "" || o.VDDK.Server == "" || o.VDDK.User == "" ||
		o.VDDK.VMMoref == "" || o.VDDK.SnapshotMoref == "" || o.DiskPath == "" || o.TargetQCOW2 == "" || o.RangesFile == "" {
		return errors.New("missing required delta-sync flags")
	}
	if o.VDDK.Password == "" {
		return errors.New("password is required (flag or VC_PASSWORD env)")
	}
	return nil
}
