# Retail Automation

Local Docker Compose environment containing PostgreSQL, n8n, and a small
FastAPI inventory service.

## Prerequisites

- Docker Desktop with Docker Compose

## Setup

1. Create the local environment file:

   ```powershell
   Copy-Item .env.example .env
   ```

2. Replace the placeholder values in `.env`.

3. Start the services:

   ```powershell
   docker compose up -d
   ```

4. Check their status:

   ```powershell
   docker compose ps
   ```

## Local services

- n8n: <http://localhost:5678>
- Inventory API: <http://localhost:8000>
- PostgreSQL: `localhost:5432`

Example inventory request:

```powershell
curl.exe http://localhost:8000/inventory/MANGO-12PK
```

Docker volumes hold the PostgreSQL and n8n data. Do not remove the volumes
unless you intentionally want to delete local data.
