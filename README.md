# 🚛 Logistics Intelligence Pipeline

Production-grade batch ETL pipeline that computes **delivery risk scores** by combining live order data with **real-time diesel prices** and **current weather conditions** — designed to run periodically (e.g. hourly) and deliver actionable logistics intelligence.

Built with Python, Apache Airflow, PostgreSQL, and the medallion architecture pattern.

> **📌 Note:** This project uses the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) as a stand-in for a live order stream. In a production deployment, the orders extractor would connect to the company's OMS (Order Management System) or event stream. The rest of the pipeline — diesel prices, weather data, transforms, scoring, and loading — runs against live APIs and is production-ready as-is.

---

## 🎯 Business Problem & Solution

### The Problem

E-commerce logistics operators face a persistent challenge: **predicting which deliveries are at risk of delays or increased costs before they happen**. The factors that drive delivery risk are scattered across multiple, disconnected data sources:

- **Order characteristics** — heavier packages cost more to ship and are more prone to handling delays
- **Fuel prices** — diesel price fluctuations directly impact carrier costs and can trigger route changes or capacity constraints
- **Weather conditions** — storms, heavy rain, and strong winds cause road closures, slower transit, and higher accident rates

Without a unified view, logistics teams rely on gut feeling or react to problems after they've already impacted customers. There's no systematic way to flag high-risk shipments proactively, allocate resources preemptively, or quantify how external factors compound to affect delivery reliability.

### The Solution

This pipeline is designed to **run on a recurring schedule** (hourly, bi-hourly, or any cadence that fits the business), producing a fresh risk assessment each cycle. On every run, it:

1. **Extracts fresh data** — Pulls the latest orders, queries the EIA API for current diesel prices, and fetches live weather conditions from OpenWeather for the most active delivery cities
2. **Refines and conforms** — Cleans, deduplicates, and standardizes each dataset in a silver layer, making them join-ready
3. **Scores every order** — Joins all three datasets and computes a composite 0–1 risk score that quantifies delivery risk based on package weight, current diesel prices, and weather conditions at the delivery location
4. **Classifies risk** — Categorizes each order into low / medium / high risk bands for easy operational triage
5. **Loads to the warehouse** — Persists scored results to PostgreSQL, where downstream dashboards, alerting systems, or APIs can consume them

The result is a **near-real-time risk signal** that refreshes with every pipeline run — enabling logistics teams to see which deliveries need attention right now and why, not after something has already gone wrong.

---

## 📐 Architecture

### Medallion Architecture

The pipeline follows the **bronze → silver → gold** medallion pattern, where each layer progressively refines the data:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     LIVE DATA SOURCES                                   │
│                                                                         │
│   📦 Order System        ⛽ EIA REST API        🌤️ OpenWeather API      │
│   (OMS / event stream;   (weekly US retail      (current weather for    │
│    Olist CSVs for demo)   diesel prices)         top-N delivery cities) │
└────────┬─────────────────────┬──────────────────────┬───────────────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE — Raw landing (parquet + csv)                                │
│                                                                         │
│   orders.parquet         diesel_prices.parquet   weather_snapshots.parquet│
└────────┬─────────────────────┬──────────────────────┬───────────────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER — Cleaned & conformed                                        │
│                                                                         │
│   orders.parquet         diesel_prices.parquet   weather_latest.parquet  │
│   (drop null weights,    (cast period to         (latest observation    │
│    cast timestamps)       datetime, sort asc)     per delivery city)    │
└────────┬─────────────────────┬──────────────────────┬───────────────────┘
         │                     │                      │
         └─────────────────────┼──────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD — Business-ready                                               │
│                                                                         │
│   delivery_risk.parquet                                                  │
│   (order_id, score, risk_band, diesel_price, weather_condition, ...)    │
│                                                                         │
│   Joins: orders ← merge_asof → diesel (backward on timestamp)          │
│          result ← inner join → weather (on zip_code)                    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🐘 POSTGRESQL — Data Warehouse                                         │
│                                                                         │
│   Schemas: bronze.* │ silver.* │ gold.delivery_risk                     │
│                                                                         │
│   ➡️ Consumed by: dashboards, alerting, APIs, analytics                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### Airflow DAG

The pipeline is orchestrated as a single Airflow DAG with 8 tasks, designed to run on a configurable schedule (e.g. `@hourly`, `0 */2 * * *` for bi-hourly, or manual trigger):

```
extract_orders ──► transform_silver_orders ──┐
                                             │
extract_diesel ──► transform_silver_diesel ──┼──► build_gold ──► load_to_postgres
                                             │
extract_weather ─► transform_silver_weather ─┘
```

- **Extract tasks** run in parallel — pulling fresh data from all three sources concurrently
- **Silver transforms** run in parallel after their respective extract completes
- **Gold build** waits for all silver transforms, then joins and scores
- **Load** writes the scored results to PostgreSQL for downstream consumption

Each run produces a complete, timestamped snapshot of delivery risk across all active orders.

### 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| 🐍 Language | Python 3.12+ |
| 🔄 Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| 🐘 Warehouse | PostgreSQL 16 |
| 📊 Processing | pandas + pyarrow |
| ✅ Validation | Pydantic v2 |
| 🗄️ ORM | SQLAlchemy 2.0 (Core, not ORM) |
| 📦 Package Manager | uv |
| 🧪 Testing | pytest (100% coverage) |
| 🔍 Linting | ruff |
| 🐳 Infrastructure | Docker Compose |

---

## 📁 Project Structure

```
logistics-intelligence-pipeline/
├── dags/
│   └── logistics_pipeline.py      # Airflow DAG definition
├── src/
│   ├── extract/                   # 🔽 Data source connectors
│   │   ├── extract_orders.py      #    Order system reader (Olist CSVs for demo)
│   │   ├── extract_diesel.py      #    EIA v2 API client
│   │   └── extract_weather.py     #    OpenWeather API client
│   ├── transform/                 # 🔄 Data transformations
│   │   ├── silver_orders.py       #    Drop nulls, cast timestamps
│   │   ├── silver_diesel.py       #    Cast period, sort for merge_asof
│   │   ├── silver_weather.py      #    Latest snapshot per zip
│   │   ├── risk_score.py          #    Pure risk scoring function
│   │   └── gold_delivery_risk.py  #    Join + score → gold output
│   ├── load/                      # 🔼 Warehouse writers
│   │   ├── db.py                  #    Engine factory (env vars)
│   │   ├── tables.py              #    SQLAlchemy Table definitions
│   │   └── loader.py              #    DataFrame → SQL loader
│   └── models/                    # 📋 Pydantic contracts
│       ├── order.py               #    Order model
│       ├── diesel_price.py        #    DieselPrice model
│       ├── weather_snapshot.py    #    WeatherSnapshot model
│       └── delivery_risk_score.py #    DeliveryRiskScore model
├── data/
│   ├── bronze/                    # Raw landing zone
│   ├── silver/                    # Cleaned data
│   └── gold/                      # Business-ready output
├── tests/                         # Pytest suite (100% coverage)
├── docker/
│   ├── Dockerfile.airflow         # Airflow image with uv
│   └── init-db.sql                # Creates bronze/silver/gold schemas
├── docker-compose.yml             # Full stack definition
├── pyproject.toml                 # Project config + dependencies
└── .env.example                   # Template for environment variables
```

---

## 🚀 Getting Started

### Prerequisites

- **Python 3.12+**
- **uv** — install with `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **Docker Desktop** (optional, for Airflow + Postgres stack)

### Installation

```bash
git clone https://github.com/SergioPL14/logistics-intelligence-pipeline.git
cd logistics-intelligence-pipeline
uv sync
```

### Environment Variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your API keys:

| Variable | Description | Where to get it |
|----------|------------|-----------------|
| `EIA_API_KEY` | US Energy Information Administration API key | [eia.gov/opendata](https://www.eia.gov/opendata/) |
| `OPENWEATHER_API_KEY` | OpenWeather API key | [openweathermap.org/api](https://openweathermap.org/api) |
| `POSTGRES_USER` | PostgreSQL username | Your choice |
| `POSTGRES_PASSWORD` | PostgreSQL password | Your choice |
| `POSTGRES_DB` | PostgreSQL database name | Default: `logistics_dwh` |

---

## ▶️ Running the Pipeline

### Option A: Local CLI (no Docker needed)

Run each stage manually to see the pipeline in action:

```bash
# 1. Extract — pull fresh data into bronze
uv run python -m src.extract.extract_orders
uv run python -m src.extract.extract_diesel
uv run python -m src.extract.extract_weather

# 2. Transform — bronze → silver → gold
uv run python -c "
from src.transform.silver_orders import transform_orders
from src.transform.silver_diesel import transform_diesel
from src.transform.silver_weather import transform_weather
from src.transform.gold_delivery_risk import build_gold

orders = transform_orders()
diesel = transform_diesel()
weather = transform_weather()
gold = build_gold(orders, diesel, weather)
print(f'Gold: {len(gold)} rows')
print(gold[['order_id','score','risk_band']].head())
"
```

Output files are written to `data/bronze/`, `data/silver/`, and `data/gold/`.

### Option B: Docker + Airflow (production-like)

This is the intended production setup — Airflow handles scheduling, retries, and monitoring:

```bash
docker compose up -d --build
```

| Service | URL | Credentials |
|---------|-----|-------------|
| 🌐 Airflow UI | http://localhost:8080 | admin / admin |
| 🗃️ Adminer | http://localhost:8081 | see `.env` |
| 🐘 PostgreSQL | localhost:5432 | see `.env` |

Trigger the `logistics_pipeline` DAG from the Airflow UI, or set a schedule (e.g. `@hourly`) in `dags/logistics_pipeline.py` for continuous operation.

---

## 📊 Risk Score

### Overview

Each order receives a **delivery risk score** between 0.0 (no risk) and 1.0 (maximum risk), based on five factors that capture logistics cost and environmental conditions. Scores are rounded to 4 decimal places and recomputed on every pipeline run with the latest available data.

### Formula

The score is a **weighted sum of normalized sub-scores**, each clipped to [0, 1]:

```
score = 0.30 × weight_score
      + 0.25 × diesel_score
      + 0.25 × condition_score
      + 0.10 × wind_score
      + 0.10 × precip_score
```

### Sub-scores

| Factor | Formula | Range / Cap |
|--------|---------|-------------|
| 📦 **Weight** | `order_weight_g / 30,000` | 0–30 kg |
| ⛽ **Diesel** | `(price_usd_per_gallon - 3.0) / 3.0` | $3.00–$6.00 |
| 🌧️ **Condition** | Categorical lookup (see below) | 0.0–1.0 |
| 💨 **Wind** | `wind_speed_ms / 20.0` | 0–20 m/s |
| 🌊 **Precipitation** | `precipitation_mm / 10.0` | 0–10 mm |

**Weather condition mapping:**

| Condition | Risk |
|-----------|------|
| Clear, Clouds | 0.0 |
| Rain, Drizzle, Mist, Fog, Haze | 0.5 |
| Snow, Thunderstorm | 1.0 |
| Unknown/other | 0.3 |

### Risk Bands

| Band | Score Range |
|------|------------|
| 🟢 Low | 0.00 – 0.32 |
| 🟡 Medium | 0.33 – 0.65 |
| 🔴 High | 0.66 – 1.00 |

### Design Rationale

- **Linear weighted sum** — interpretable, testable per-component, tunable without retraining
- **Logistics economics** (weight + diesel) account for ~55% of the score
- **Environmental factors** (condition + wind + precipitation) account for ~45%
- All thresholds are module-level constants — easy to tune without code changes
- Scores refresh on every pipeline run, reflecting the latest diesel prices and weather conditions

---

## 📂 Data Sources

### 📦 Orders

In production, this connects to the company's Order Management System (OMS), event stream, or transactional database to pull active/recent orders.

For this portfolio project, the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100K orders, 2016–2018) serves as a realistic stand-in.

- **Key fields:** order_id, customer_zip_code (5-digit prefix), customer_city, customer_state, order_weight_g

### ⛽ EIA Diesel Prices

Live weekly U.S. retail diesel prices from the Energy Information Administration (EIA) v2 API. Updated every Monday — the pipeline always fetches the latest available price.

- **Series:** `EMD_EPD2D_PTE_NUS_DPG` (No. 2 Diesel, U.S. retail)
- **Update frequency:** Weekly
- **Historical depth:** ~1,674 weekly observations

### 🌤️ OpenWeather

Live weather conditions for the top-N most frequent delivery cities, fetched from the OpenWeather free-tier API on every pipeline run.

- **Geocoding:** `/geo/1.0/direct` (city + state + country code)
- **Weather:** `/data/2.5/weather` (temperature, wind, precipitation, condition)
- **Coverage:** Top 20 delivery locations by default

---

## ⚠️ Known Limitations

### 🌤️ Weather Temporal Alignment

The OpenWeather extractor uses the **current weather endpoint** (free tier). Each pipeline run captures a live weather snapshot, which is the correct behavior for a production pipeline running hourly/bi-hourly — the weather data reflects conditions at scoring time.

However, when running against the historical Olist dataset for demo purposes, the weather data does not correspond to the actual weather at the time of each order. In production with live orders, this limitation disappears.

**Future upgrade path:** For historical analysis, backfill weather data from NOAA (US) or INMET (Brazil).

### 🌍 Geocoding Approximation

Olist stores a 5-digit prefix of the 8-digit Brazilian CEP (postal code). Since OpenWeather's zip-based geocoding doesn't support partial CEPs, the pipeline uses **city + state geocoding** instead. Multiple zip prefixes within the same city share a single weather observation. In production, full postal codes or exact coordinates from the OMS would improve granularity.

### 📊 Demo Score Distribution

With the Olist demo dataset, most scores cluster in the "low" band because historical diesel prices (~$2.40–$2.60) fall below the $3.00 base threshold. In a live production scenario with current diesel prices (~$3.50–$4.50+) and real-time weather variability, the score distribution would be more varied and operationally meaningful.

---

## 🔮 Future Improvements

### 📈 Data & Scoring

| Improvement | Description | Impact |
|-------------|------------|--------|
| **Learn risk weights from delivery history** | Train weights via logistic regression or gradient boosting using actual delivery delays vs. estimates | Scores reflect real delay patterns instead of expert assumptions |
| **Traffic & road condition data** | Integrate Google Maps or TomTom traffic APIs for real-time route risk | Captures congestion, road closures, and construction at scoring time |
| **Carrier performance scoring** | Add carrier on-time delivery rates as a sub-score | Factors in carrier reliability, not just external conditions |
| **Seasonal patterns** | Add time-of-year features (holidays, rainy season, Black Friday) | Captures demand surges and seasonal weather trends |
| **Regional diesel prices** | Use state-level or city-level fuel prices instead of national average | More accurate cost estimation per delivery route |

### 🏗️ Infrastructure & Operations

| Improvement | Description | Impact |
|-------------|------------|--------|
| **Incremental loads** | Switch from full-replace to append/upsert with change detection | Faster runs, lower API usage, essential for hourly cadence at scale |
| **Data quality checks** | Add Great Expectations or dbt tests between pipeline stages | Catch schema drift, null spikes, or anomalous values before they reach gold |
| **Alerting** | Slack/email notifications on DAG failure or data quality issues | Faster incident response for a pipeline that runs every hour |
| **CI/CD** | GitHub Actions for lint + test + Docker build on every PR | Catch regressions before merge |
| **Partitioned storage** | Partition gold tables by run timestamp for time-series analysis | Track how risk evolves over time, efficient querying |
| **Horizontal scaling** | Replace pandas with Spark or DuckDB for larger order volumes | Handle millions of orders per run without memory constraints |

### 📊 Analytics & Consumption

| Improvement | Description | Impact |
|-------------|------------|--------|
| **Live dashboard** | Streamlit or Grafana dashboard refreshing with each pipeline run | Visual operational tool — logistics teams see risk in real time |
| **Real-time scoring API** | Expose `compute_risk` as a FastAPI endpoint for on-demand scoring | Score individual orders at checkout time, not just in batch |
| **Anomaly detection** | Flag orders whose risk score deviates significantly from recent averages | Early warning for unusual risk spikes (e.g. sudden weather event) |
| **Risk trend analysis** | Track how each order's risk changes across pipeline runs | Detect worsening conditions for in-transit deliveries |
| **Automated re-routing** | Trigger carrier notifications or route changes when risk exceeds threshold | Close the loop — pipeline drives action, not just insight |

---

## 🧪 Testing

```bash
# Run all tests
uv run pytest

# With coverage report
uv run pytest --cov=src --cov-report=term-missing

# Run specific test modules
uv run pytest tests/transform/
uv run pytest tests/load/
uv run pytest tests/models/
```

Current coverage: **100%** across all `src/` modules (109 tests).

---

## 🛠️ Development

```bash
# Install dependencies
uv sync

# Add a dependency
uv add <package>
uv add --dev <package>

# Lint
uv run ruff check .

# Format
uv run ruff format .

# Run a script
uv run python -m src.extract.extract_orders
```

---

## 📄 License

This project is for educational and portfolio purposes. The Olist dataset is publicly available under [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/).
