# case-delfos
This project implements a complete ETL pipeline containerized with Docker.
It extracts data from a **Source Database** via a **FastAPI** service, transforms it using **Dagster** assets, and loads it into a **Target Database**.

## Architecture
* **source_db**: PostgreSQL (Pre-loaded with sample data).
* **target_db**: PostgreSQL (Destination for transformed data).
* **fastapi**: Middleware API that queries `source_db`.
* **dagster**: Orchestrator that pulls from `fastapi` and writes to `target_db`.


## Project Directory Structure

```text
my-etl-project/
├── api/                     # The FastAPI Service
│   ├── api.py
│   ├── Dockerfile
│   └── requirements.txt
├── etl/                     # The Dagster Service
│   ├── src/
│   │   ├── extract.py
│   │   ├── load.py
│   │   ├── logging_config.py
│   │   └── transform.py
│   ├── dagster.yaml         # (Optional config to setup permanent psql DB)
│   ├── definitions.py
│   ├── Dockerfile
│   ├── main.py              # Python script version of ETL
│   ├── requirements.txt
│   └── wait-for-services.sh # Ensures right container building order
├── scripts/                 # Database init scripts
│   └── create-source.sql
├── .env                     # Secrets (DB user, pass, etc.)
└── docker-compose.yml

```

## Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine + Compose plugin)


## Quick Start

### 1. Configure Environment
Create a `.env` file in the project root:

```ini
# Target Database (Postgres) Configuration
TARGET_DB=delfos_target

# Both Databases Credentials
DB_USER=postgres
DB_PASSWORD=postgres
DB_PORT=5432

```

> **NOTICE:** The inserted database credentials **must belong to a superuser**

### 2. Build and Run

Run the stack using Docker Compose:

```bash
docker compose up --build -d

```

*Wait ~15 seconds for the databases to initialize and the API to become healthy.*

## Usage - Run the ETL pipeline

### Option A: The Dagster UI (Orchestrator)

Open the Dagster UI: **[http://localhost:3000](https://www.google.com/search?q=http://localhost:3000)**

1. Click **Overview** or **Assets** to see your asset graph.
2. Click **Materialize All** (top right) to run the full pipeline.
3. Once finished, the green status indicates data has been loaded into `target_db`.

### Option B: The Manual Script (Ad-hoc)

You can run the ETL logic manually via a standalone script included in the container. This bypasses the Dagster scheduler and runs immediately.

```bash
docker compose exec dagster python main.py 'DD-MM-YYYY'

```

*This executes `etl/main.py` inside the running environment, using the same logic and connections as the Dagster job.*


## Verification

### Check API Health

Open your browser to: **[http://localhost:8000/docs](https://www.google.com/search?q=http://localhost:8000/docs)**

* You should see the Swagger UI.
* Try the `/health` endpoint to see the API status (on/offline).
* Try the `/data` endpoint to ensure it can read from the Source DB.


## Debugging & Management

**View Logs**
If something isn't working, check the logs for a specific service:

```bash
docker compose logs -f dagster
# or
docker compose logs -f fastapi

```

**Check Database Content**
To verify data inside the containers without installing local tools:

```bash
# Check Source DB
docker compose exec -it source_db psql -U myuser -d source_db -c "\dt"

# Check Target DB (verify ETL results)
docker compose exec -it target_db psql -U myuser -d postgres -c "SELECT * FROM power_data LIMIT 5;"

```

**Full Reset (Fixes most errors)**
If your database schema changes or startup scripts fail, perform a clean reset:

```bash
# Stops containers and DELETES database volumes
docker compose down -v 

# Rebuilds and starts fresh
docker compose up --build -d

```

## Troubleshooting

**Permission Denied:** `./wait-for-services.sh**`
If you see a permission error on startup:

* **Mac/Linux:** Run `chmod +x etl/wait-for-services.sh` locally and rebuild.
* **Windows:** Ensure your git client didn't convert line endings to CRLF.
