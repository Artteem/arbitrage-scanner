# Arbitrage Scanner

Monorepo for a crypto arbitrage scanner (backend + frontend).  
Tech stack: **Python 3.11 / FastAPI** (backend), **TypeScript / Next.js** (frontend).

## Structure
- `backend/` — FastAPI service, business logic and integrations.
- `frontend/` — Next.js app (dashboard for spreads, see below).
- `docs/` — Architecture, decisions, and runbooks.
- `scripts/` — Utility scripts.
- `.github/workflows/` — CI configs.

## Quickstart
See `docs/02-setup.md` for environment setup and `docs/03-backend-dev.md` to run the API locally.

## Frontend

The `frontend` directory contains a Next.js 14 dashboard that consumes the deployed FastAPI
service. To run it locally:

```bash
cd frontend
npm install
npm run dev
```

By default the UI connects to `http://localhost:8000`. Override the API URL by setting the
`NEXT_PUBLIC_API_BASE_URL` environment variable before starting the dev server.
