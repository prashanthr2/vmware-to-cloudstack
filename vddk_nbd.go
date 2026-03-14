package main

import (
    "context"
    "flag"
    "log"
    "net"
    "net/url"
    "os"
    "sync"
    "time"

    "github.com/vmware/govmomi"
    "github.com/vmware/govmomi/vim25/types"
    "github.com/vmware/govmomi/object"
    "github.com/vmware/govmomi/vddk"
    "github.com/digitalocean/go-nbd/server"
)

type VDDKReader struct {
    handles []*vddk.Handle
    mu      sync.Mutex
    size    int64
    index   int
}

func (r *VDDKReader) ReadAt(p []byte, off int64) (n int, err error) {
    r.mu.Lock()
    h := r.handles[r.index%len(r.handles)]
    r.index++
    r.mu.Unlock()
    return h.ReadAt(p, off)
}

func (r *VDDKReader) Size() (int64, error) {
    return r.size, nil
}

func main() {
    vmMoref := flag.String("vm", "", "VM moref")
    diskPath := flag.String("disk", "", "disk path")
    snapshotMoref := flag.String("snapshot", "", "snapshot moref")
    socket := flag.String("socket", "", "unix socket")
    vcenter := flag.String("vcenter", "", "vcenter")
    user := flag.String("user", "", "user")
    pass := flag.String("pass", "", "pass")
    thumb := flag.String("thumb", "", "thumbprint")
    numHandles := flag.Int("handles", 4, "number of VDDK handles")
    flag.Parse()

    ctx := context.Background()
    u, err := govmomi.NewClient(ctx, &url.URL{Scheme: "https", Host: *vcenter}, true)
    if err != nil {
        log.Fatal(err)
    }
    err = u.Login(ctx, url.UserPassword(*user, *pass))
    if err != nil {
        log.Fatal(err)
    }

    vm := object.NewVirtualMachine(u.Client, types.ManagedObjectReference{Type: "VirtualMachine", Value: *vmMoref})

    // Open VDDK handles
    var handles []*vddk.Handle
    for i := 0; i < *numHandles; i++ {
        config := vddk.Config{
            Libdir:     "/opt/vmware-vddk/vmware-vix-disklib-distrib/",
            Server:     *vcenter,
            User:       *user,
            Password:   *pass,
            Thumbprint: *thumb,
        }
        h, err := vddk.Open(vm, *diskPath, *snapshotMoref, config)
        if err != nil {
            log.Fatal(err)
        }
        handles = append(handles, h)
    }

    size := handles[0].Size()
    reader := &VDDKReader{handles: handles, size: size}

    // Start NBD server
    s, err := server.New(reader, &server.Options{ReadOnly: true})
    if err != nil {
        log.Fatal(err)
    }

    l, err := net.Listen("unix", *socket)
    if err != nil {
        log.Fatal(err)
    }
    defer os.Remove(*socket)

    log.Println("NBD server started")
    s.Serve(l)
}