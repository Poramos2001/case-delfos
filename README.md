# case-delfos
precisa ter um super user e botar o login e senha dele no config.json

# TODO
- containerizo com docker

```bash
# Source API Configuration
SOURCE_API_URL=https://api.energy-source.com/v1/
SOURCE_API_KEY=your_secret_api_token_123

# Target Database (Postgres) Configuration
PG_USER=postgres_user
PG_PASSWORD=super_secure_password
PG_DB=delfos-target
PG_HOST=localhost
PG_PORT=5432
```

```bash
dagster dev -f definitions.py
```
```bash
uvicorn api:app --port 8000
```
---
To containerize a project with Dagster, FastAPI, and a shared environment, you need an architecture that separates your concerns while allowing them to communicate.

The standard production-ready pattern for Dagster in Docker involves splitting the "System" (UI and Daemon) from your "User Code" (ETL logic).

Here is a step-by-step guide to setting up your architecture.

### 1. Project Directory Structure

First, organize your project so Docker contexts are clean. We will create separate folders for your API and your Dagster code.

```text
my-etl-project/
├── api/                     # The FastAPI Service
│   ├── api.py
│   ├── Dockerfile
│   └── requirements.txt
├── etl/                     # The Dagster Service
│   ├── src/
│   │   ├── extract.py
│   │   ├── transform.py
│   │   └── load.py
│   ├── definitions.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dagster.yaml         # (Optional config)
├── scripts/                 # Database init scripts
│   └── create-source.sql
├── .env                     # Secrets (DB user, pass, etc.)
└── docker-compose.yml       # The orchestrator

```

---

### 5. Managing the `.env` File

Ensure your `.env` file is in the root directory. Docker Compose handles the injection automatically via `env_file`.

**Example `.env` content:**

```ini
# Credentials for your source database (used by FastAPI or ETL)
SOURCE_DB_USER=admin
SOURCE_DB_PASS=secret123
SOURCE_DB_HOST=192.168.1.50 # Or external IP

```

### How to Run

1. **Build and Start:**
```bash
docker-compose up --build

```


2. **Access Dagster UI:**
Open your browser to `http://localhost:3000`. You should see your code location listed and ready to run jobs.
3. **Access FastAPI:**
Open `http://localhost:8000/docs` to see your API Swagger documentation.

### Why this architecture?

* **Decoupling:** If your ETL code crashes (e.g., OOM error), it crashes the `user_code` container, but the Dagster Webserver and Daemon stay alive.
* **Networking:** The `user_code` container can talk to `fastapi_app` simply by using the hostname `http://fastapi_app:8000` inside your Python code.
* **Persistence:** The `dagster_postgresql` container ensures that if you restart Docker, you don't lose your job history or backfills.

**Would you like me to explain how to make the Dagster ETL code communicate with the FastAPI container over the docker network?**