# mx-economic-pipeline

![CI](https://github.com/JoelBuenrostro/mx-economic-pipeline/actions/workflows/ci.yml/badge.svg)

ETL pipeline that extracts Mexican economic indicators from Banxico's public API, transforms and validates the data, stores it in a local SQLite database, and exposes it through a REST API built with FastAPI.

## Overview

```
Banxico API
    │
    ▼
[Extract]  ── extractor.py     Fetch time series, structured error handling & logging
    │
    ▼
[Transform] ── transformer.py  Parse dates, validate ranges, deduplicate (pandas)
    │
    ▼
[Load]     ── loader.py        Upsert into SQLite via SQLAlchemy (no duplicates)
    │
    ▼
[Expose]   ── FastAPI          REST endpoints to query stored data
```

## Data sources

| Series | ID | Description | Frequency |
|---|---|---|---|
| `usd_mxn` | SF43718 | USD/MXN exchange rate | Daily |
| `inpc` | SP1 | Consumer price index (inflation) | Monthly |
| `tiie_28` | SF61745 | TIIE 28-day benchmark rate | Daily |

Source: [Banco de México — SIE API](https://www.banxico.org.mx/SieAPIRest/service/v1/)

## Tech stack

- **Python 3.11+**
- **FastAPI** — REST API layer
- **pandas** — data transformation and validation
- **SQLAlchemy** — ORM and database access
- **SQLite** — local data storage
- **pydantic** — request/response schema validation
- **pytest** — 36 unit and integration tests
- **GitHub Actions** — CI on every push
- **Docker** — containerized deployment

## Project structure

```
mx-economic-pipeline/
├── pipeline/
│   ├── __init__.py
│   ├── extractor.py      # Banxico API client with error handling
│   ├── transformer.py    # Data validation and transformation (pandas)
│   ├── loader.py         # SQLite upsert logic (SQLAlchemy)
│   └── models.py         # SQLAlchemy ORM models
├── api/
│   ├── __init__.py
│   ├── main.py           # FastAPI application
│   ├── routes.py         # Route definitions
│   ├── schemas.py        # Pydantic schemas
│   └── database.py       # DB session dependency
├── tests/
│   ├── test_transformer.py
│   └── test_api.py
├── .github/workflows/
│   └── ci.yml            # Run pytest on every push
├── conftest.py
├── run_pipeline.py       # CLI entrypoint
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

## Quickstart

### Local

```bash
# 1. Clone and install
git clone https://github.com/joelbuenrostro/mx-economic-pipeline.git
cd mx-economic-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Set up your Banxico token
cp .env.example .env
# Edit .env and add your BANXICO_TOKEN
# Get a free token at: https://www.banxico.org.mx/SieAPIRest/service/v1/token

# 3. Run the pipeline
python run_pipeline.py                                          # last 30 days
python run_pipeline.py --fecha-inicio 2025-01-01 --fecha-fin 2026-04-23  # custom range

# 4. Start the API
uvicorn api.main:app --reload

# 5. Run tests
pytest tests/ -v
```

### Docker

```bash
# Build and start the API
docker compose up api

# Run the pipeline once
docker compose run --rm pipeline
```

## API endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check — status and total record count |
| GET | `/series` | List all available series |
| GET | `/series/{name}/datos` | Query data with optional `fecha_inicio` / `fecha_fin` filters |

Interactive docs available at `http://localhost:8000/docs` once the API is running.

## Notes

- **INPC is monthly** — use a date range of at least one full month to retrieve data (e.g. `--fecha-inicio 2025-01-01`). Querying a short range like the last 30 days may return no records.
- **Upsert strategy** — re-running the pipeline on the same date range never produces duplicate rows. Existing records are updated only if the value changed.
- **Partial failure tolerance** — if one series fails to fetch, the pipeline continues with the remaining ones and logs the error.
- **Structured logging** — every extract step logs HTTP status, record counts, and skipped values.

## Key design decisions

- **Validation in the transform layer** — values marked as `N/E` (not available) are explicitly skipped and counted, not coerced to zero.
- **Range-based validation** — each series has defined min/max bounds; out-of-range values are discarded and logged before reaching the database.
- **Dependency injection in tests** — the API test suite overrides the database dependency with a temporary SQLite file, ensuring full isolation without mocking the ORM.

## Author

Joel Buenrostro — [LinkedIn](https://www.linkedin.com/in/joelbuenrostro/) · [GitHub](https://github.com/joelbuenrostro)