# Logistics Intelligence Pipeline — Demo Guide

Step-by-step guide to run the full pipeline end-to-end using Docker. No Python installation required on the host machine — everything runs inside containers.

---

## 1. Prerequisites

| Requirement | Details |
|-------------|---------|
| **Docker Desktop** | Installed and **running** (green icon in taskbar). [Download here](https://www.docker.com/products/docker-desktop/) |
| **Git** | To clone the repository |
| **EIA API Key** | Free — register at [eia.gov/opendata](https://www.eia.gov/opendata/) |
| **OpenWeather API Key** | Free tier — register at [openweathermap.org/api](https://openweathermap.org/api) |

> **Tip:** Both API keys are free and take ~2 minutes to set up. The EIA key is available instantly; OpenWeather keys may take up to 10 minutes to activate.

---

## 2. Setup

### 2.1 Clone the repository

```bash
git clone https://github.com/SergioPL14/logistics-intelligence-pipeline.git
cd logistics-intelligence-pipeline
```

### 2.2 Configure environment variables

```bash
cp .env.example .env
```

Open `.env` in any text editor and replace the placeholder API keys:

```env
EIA_API_KEY=your_real_eia_key_here
OPENWEATHER_API_KEY=your_real_openweather_key_here
```

Leave all other values at their defaults — they're pre-configured for the Docker stack:

| Variable | Default | Purpose |
|----------|---------|---------|
| `POSTGRES_USER` | `logistics` | Data warehouse username |
| `POSTGRES_PASSWORD` | `logistics_pwd` | Data warehouse password |
| `POSTGRES_DB` | `logistics_dwh` | Data warehouse database name |
| `_AIRFLOW_WWW_USER_USERNAME` | `admin` | Airflow UI login |
| `_AIRFLOW_WWW_USER_PASSWORD` | `admin` | Airflow UI password |
| `ADMINER_PORT` | `8081` | Adminer web UI port |

### 2.3 Build and start the stack

```bash
docker compose up -d --build
```

This builds the custom Airflow image (installs Python dependencies via `uv`) and starts 6 containers:

| Container | Role | Port |
|-----------|------|------|
| `logistics_postgres` | Data warehouse (bronze/silver/gold schemas) | 5432 |
| `airflow_metadata_db` | Airflow's internal metadata database | — |
| `airflow_init` | One-shot: migrates Airflow DB + creates admin user | — |
| `airflow_webserver` | Airflow web UI | **8080** |
| `airflow_scheduler` | Executes DAG tasks | — |
| `logistics_adminer` | Database browser UI | **8081** |

### 2.4 Wait for services to be healthy

The first build takes 2–3 minutes. Check that everything is up:

```bash
docker compose ps
```

Expected output — all services show `healthy` (except `airflow_init` which shows `exited (0)`):

```
NAME                    STATUS
logistics_postgres      running (healthy)
airflow_metadata_db     running (healthy)
airflow_init            exited (0)
airflow_webserver       running (healthy)
airflow_scheduler       running (healthy)
logistics_adminer       running (healthy)
```

If any service shows `starting` or `unhealthy`, wait another 30 seconds and check again.

---

## 3. Running the Pipeline

### 3.1 Open the Airflow UI

Navigate to **http://localhost:8080** and log in:

- **Username:** `admin`
- **Password:** `admin`

### 3.2 Find the DAG

You'll see the `logistics_pipeline` DAG in the list. It has a toggle switch on the left and action buttons on the right.

### 3.3 Enable and trigger

1. **Toggle the DAG ON** — click the switch on the left so it turns blue
2. **Trigger the DAG** — click the **Play** button (triangle icon) on the right, then select **"Trigger DAG"**

### 3.4 Watch execution

Click on the DAG name (`logistics_pipeline`) to open the detail view. Switch to the **Graph** tab for the best visualization.

The 8 tasks execute in this order:

```
extract_orders ──┬──> transform_silver_orders ──┐
                 │                               │
                 └──> extract_weather ──> transform_silver_weather ──┐
                                                                    │
extract_diesel ─────> transform_silver_diesel ──────────────────────┼──> build_gold ──> load_gold
```

**What happens at each stage:**

| Task | What it does | Duration |
|------|-------------|----------|
| `extract_orders` | Reads Olist CSV files, writes `data/bronze/orders.parquet` | ~5s |
| `extract_diesel` | Calls EIA API for diesel prices, writes `data/bronze/diesel_prices.parquet` | ~3s |
| `extract_weather` | Reads bronze orders to find top-20 cities, calls OpenWeather API, writes `data/bronze/weather_snapshots.parquet` | ~10s |
| `transform_silver_orders` | Cleans orders (drops nulls, casts types), writes `data/silver/orders.parquet` | ~2s |
| `transform_silver_diesel` | Casts diesel periods to datetime, sorts, writes `data/silver/diesel_prices.parquet` | ~1s |
| `transform_silver_weather` | Deduplicates to latest observation per city, writes `data/silver/weather_latest.parquet` | ~1s |
| `build_gold` | Joins all silver data + computes risk scores, writes `data/gold/delivery_risk.parquet` | ~3s |
| `load_gold` | Loads gold parquet into `gold.delivery_risk` table in PostgreSQL | ~2s |

**Success indicator:** All tasks turn **dark green** in the Graph view. Total runtime is approximately 30–60 seconds.

---

## 4. Verifying Results

### 4.1 Open Adminer

Navigate to **http://localhost:8081** and log in:

| Field | Value |
|-------|-------|
| **System** | PostgreSQL |
| **Server** | `postgres` |
| **Username** | `logistics` |
| **Password** | `logistics_pwd` |
| **Database** | `logistics_dwh` |

### 4.2 Browse the schemas

After logging in, you'll see three schemas in the left sidebar. Click on each to explore:

#### Bronze (raw data)

| Table | Expected rows | What to check |
|-------|--------------|---------------|
| `bronze.orders` | ~100,000 | Raw order data with all original columns |
| `bronze.diesel` | ~1,674 | Weekly diesel prices going back to 1994 |
| `bronze.weather` | ~20 | One weather snapshot per delivery city |

#### Silver (cleaned)

| Table | Expected rows | What to check |
|-------|--------------|---------------|
| `silver.orders` | ~95,000–100,000 | Fewer rows than bronze (nulls dropped), clean types |
| `silver.diesel` | ~1,674 | Same count, but `period` column is a proper timestamp |
| `silver.weather` | ~20 | Deduplicated — one row per city |

#### Gold (business-ready)

| Table | Expected rows | What to check |
|-------|--------------|---------------|
| `gold.delivery_risk` | ~95,000 | Every order has a `score` (0–1) and `risk_band` (low/medium/high) |

### 4.3 Inspect risk scores

Click on `gold.delivery_risk` → **Select data** to browse the scored orders. Key columns:

| Column | Description |
|--------|------------|
| `order_id` | Unique order identifier |
| `score` | Composite risk score (0.0–1.0, rounded to 4 decimals) |
| `risk_band` | Categorical label: `low`, `medium`, or `high` |
| `weight_score` | Sub-score from package weight |
| `diesel_score` | Sub-score from diesel price |
| `condition_score` | Sub-score from weather condition |
| `wind_score` | Sub-score from wind speed |
| `precip_score` | Sub-score from precipitation |

---

## 5. Exploring the Risk Scores

Click **SQL command** in Adminer's left sidebar to run queries directly.

### Distribution by risk band

```sql
SELECT risk_band, COUNT(*) AS orders, ROUND(AVG(score), 4) AS avg_score
FROM gold.delivery_risk
GROUP BY risk_band
ORDER BY avg_score;
```

### Top 10 highest-risk orders

```sql
SELECT order_id, score, risk_band, weight_score, diesel_score, condition_score
FROM gold.delivery_risk
ORDER BY score DESC
LIMIT 10;
```

### Risk breakdown by weather condition

```sql
SELECT weather_condition, COUNT(*) AS orders,
       ROUND(AVG(score), 4) AS avg_score,
       ROUND(AVG(condition_score), 4) AS avg_condition_score
FROM gold.delivery_risk
GROUP BY weather_condition
ORDER BY avg_score DESC;
```

### Risk distribution by customer state

```sql
SELECT customer_state, COUNT(*) AS orders,
       ROUND(AVG(score), 4) AS avg_score,
       SUM(CASE WHEN risk_band = 'high' THEN 1 ELSE 0 END) AS high_risk_count
FROM gold.delivery_risk
GROUP BY customer_state
ORDER BY avg_score DESC
LIMIT 10;
```

### Score component correlation

```sql
SELECT ROUND(AVG(weight_score), 4) AS avg_weight,
       ROUND(AVG(diesel_score), 4) AS avg_diesel,
       ROUND(AVG(condition_score), 4) AS avg_condition,
       ROUND(AVG(wind_score), 4) AS avg_wind,
       ROUND(AVG(precip_score), 4) AS avg_precip
FROM gold.delivery_risk;
```

> **Note:** With the Olist demo dataset, most scores cluster in the "low" band because historical diesel prices fall below the $3.00 baseline threshold. In a live production scenario with current diesel prices and real-time weather variability, the score distribution would be more varied.

---

## 6. Monitoring & Logs

### Task logs in Airflow

1. Go to **http://localhost:8080** → click on `logistics_pipeline`
2. In the **Grid** or **Graph** view, click on any task instance (colored square/box)
3. Click **Logs** to see the full execution output

Common things to check in logs:
- **Extract tasks:** API response codes, number of records fetched
- **Transform tasks:** Row counts before/after cleaning
- **Build gold:** Number of rows after joins, score statistics
- **Load gold:** Number of rows written to PostgreSQL

### Container health

```bash
# Check all container statuses
docker compose ps

# View live logs for a specific service
docker compose logs -f airflow-scheduler

# View logs for all services
docker compose logs -f
```

### Re-running the pipeline

To run the pipeline again (e.g., after some time has passed and weather has changed):

1. Go to the Airflow UI → `logistics_pipeline`
2. Click the **Play** button → **"Trigger DAG"**

Each run produces fresh weather data and a new risk assessment. The `load_gold` task replaces the previous gold table contents with the latest scores.

---

## 7. Teardown

### Stop containers (keep data)

```bash
docker compose down
```

This stops all containers but preserves the PostgreSQL data volumes. Next time you run `docker compose up -d`, your data is still there.

### Stop containers and delete all data

```bash
docker compose down -v
```

The `-v` flag removes the named volumes (`postgres_data`, `airflow_db_data`), deleting all database contents. Use this for a clean restart.

---

## 8. Troubleshooting

### Docker Desktop not running

```
error during connect: ... open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified.
```

**Fix:** Open Docker Desktop from the Start menu and wait until it shows "running" (green icon). Then retry.

### Port already in use

```
Bind for 0.0.0.0:8080 failed: port is already allocated
```

**Fix:** Either stop the process using that port, or change the port mapping in `docker-compose.yml`. For example, to move Airflow to port 9090, change `"8080:8080"` to `"9090:8080"`.

### Permission denied on `logs/` directory

```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/...'
```

**Fix:**

```bash
mkdir -p logs
chmod 777 logs
```

### Extract task fails with API error

Check the task logs in the Airflow UI. Common causes:
- **Invalid API key:** Verify your keys in `.env` are correct and activated
- **OpenWeather key not yet active:** New keys can take up to 10 minutes to activate
- **Rate limiting:** The free OpenWeather tier allows 60 calls/minute — the pipeline uses ~20

### Extract weather fails with `FileNotFoundError`

```
FileNotFoundError: Missing Olist file: /opt/airflow/resources/...
```

**Fix:** Ensure the `resources/` directory with the Olist CSV files exists in the project root. The `docker-compose.yml` mounts it into the container automatically via `./resources:/opt/airflow/resources`.

### All tasks green but no data in Adminer

Make sure you're connecting to the right database:
- **Server:** `postgres` (not `localhost` — Adminer runs inside Docker)
- **Database:** `logistics_dwh`
- Check the `gold` schema, not the default `public` schema

### Need a completely fresh start

```bash
docker compose down -v
docker compose up -d --build
```

This removes all data volumes and rebuilds the images from scratch.
