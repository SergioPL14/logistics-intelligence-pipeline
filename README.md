# Logistics Intelligence Pipeline

Production-grade data pipeline that joins e-commerce orders with **EIA diesel prices** and **OpenWeather** conditions to compute **delivery risk scores**.

## Architecture
- **Orchestration:** Apache Airflow (LocalExecutor)
- **Warehouse:** PostgreSQL with `bronze` / `silver` / `gold` medallion schemas
- **DB UI:** Adminer
- **Modeling:** Pydantic v2 schemas
- **ETL:** `src/extract` → `src/transform` → `src/load`

## Layout
```
dags/                  Airflow DAGs
src/
  extract/             API + source connectors (EIA, OpenWeather, orders)
  transform/           Cleaning, joins, risk-score logic
  load/                Warehouse writers
  models/              Pydantic schemas (contracts)
  utils/               Shared helpers (config, logging, http)
data/
  bronze/              Raw landing
  silver/              Cleaned, conformed
  gold/                Business-ready marts
tests/                 Pytest suite
docker/                Dockerfile + DB init scripts
```

## Quick start
```bash
cp .env.example .env       # fill in EIA_API_KEY + OPENWEATHER_API_KEY
docker compose up -d --build
```

| Service           | URL                     | Credentials       |
|-------------------|-------------------------|-------------------|
| Airflow UI        | http://localhost:8080   | admin / admin     |
| Adminer           | http://localhost:8081   | see `.env`        |
| Postgres (DWH)    | localhost:5432          | see `.env`        |
