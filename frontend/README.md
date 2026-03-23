# Frontend

React + Vite dashboard for the VMware to CloudStack migration backend.

## API backend (Go)

```bash
cd ../go_rewrite
./v2c-engine serve --config ./config.yaml --listen :8000
```

## Run

```bash
cd frontend
cp .env.example .env
rm -rf node_modules package-lock.json
npm install
npm run dev
```

`VITE_API_BASE` is optional. If unset, the UI uses `http(s)://<current-host>:8000`.

## Highlights

- Auto-detect VM disks from vCenter when VM selection changes
- Disk table with label, size, type, datastore, storage target, and disk offering
- Validation: migration start disabled until all data disks have disk offerings
- Environment manager (vCenter + CloudStack) with add/edit/delete persisted in local storage
- Real-time migration dashboard with overall progress, per-disk progress, speed, and ETA
- Finalize button + live log tailing
