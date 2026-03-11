# Frontend

React + Vite dashboard for the VMware to CloudStack migration backend.

## Run

```bash
cd frontend
cp .env.example .env
rm -rf node_modules package-lock.json
npm install
npm run dev
```

By default the UI targets `http://127.0.0.1:8000`.

## Notes

- This setup is compatible with Node.js 16+.

## Features

- Create migration specs with strategy fields (`finalize_at`, delta intervals, shutdown mode)
- Start migration right after spec generation
- View running/recent jobs
- Trigger finalize marker creation
- View latest stdout/stderr logs per job
