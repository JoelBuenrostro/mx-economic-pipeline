# mx-economic-pipeline

ETL pipeline that extracts Mexican economic indicators from Banxico's public API, transforms and validates the data, stores it in a local database, and exposes it through a REST API built with FastAPI.

## Overview

```
Banxico API
    │
    ▼
[Extract]  ── extractor.py   Fetch time series, structured error handling & logging
    │
    ▼
[Transform] ── transformer.py  Parse dates, validate ranges, clean nulls (pandas)
    │
    ▼
[Load]     ── loader.py      Upsert into SQLite via SQLAlchemy (no duplicates)
    │
    ▼
[Expose]   ── FastAPI        REST endpoints to query stored data
```

## Data sources

| Series | ID | Description |
|---|---|---|
| `usd_mxn` | SF43718 | USD/MXN exchange rate |
| `inpc` | SP1 | Consumer price index (inflation) |
| `tiie_28` | SF61745 | TIIE 28-day benchmark rate |

Source: [Banco de México — SIE API](https://www.banxico.org.mx/SieAPIRest/service/v1/)

## Tech stack

- **Python 3.11+**
- **FastAPI** — REST API layer
- **pandas** — data transformation and validation
- **SQLAlchemy** — ORM and database access
- **SQLite** — local data storage
- **pydantic** — request/response schema validation
- **pytest** — unit tests
- **GitHub Actions** — CI on every push
- **Docker** — containerized deployment

## Project structure

```
mx-economic-pipeline/
├── pipeline/
│   ├── extractor.py      # Banxico API client with error handling
│   ├── transformer.py    # Data validation and transformation (pandas)
│   ├── loader.py         # SQLite upsert logic (SQLAlchemy)
│   └── models.py         # SQLAlchemy ORM models
├── api/
│   ├── main.py           # FastAPI application
│   ├── routes.py         # Route definitions
│   └── schemas.py        # Pydantic schemas
├── tests/
│   ├── test_transformer.py
│   └── test_api.py
├── .github/workflows/
│   └── ci.yml            # Run pytest on every push
├── run_pipeline.py       # CLI entrypoint
├── requirements.txt
└── Dockerfile
```

## Quickstart

```bash
# Clone and install
git clone https://github.com/joelbuenrostro/mx-economic-pipeline.git
cd mx-economic-pipeline
pip install -r requirements.txt

# Run the pipeline (fetches last 30 days)
python run_pipeline.py

# Start the API
uvicorn api.main:app --reload

# Run tests
pytest
```

## API endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/series` | List all available series |
| GET | `/series/{name}/datos` | Query data with optional `fecha_inicio` / `fecha_fin` params |
| GET | `/health` | Health check |

## Key design decisions

- **Structured logging** on every extract step — failed requests are logged with HTTP status and response body, never silently swallowed.
- **Data validation** in the transform layer — values marked as `N/E` (not available) are explicitly skipped and counted, not coerced to zero.
- **Upsert strategy** in the load layer — re-running the pipeline on the same date range never produces duplicate rows.
- **Partial failure tolerance** — if one series fails to fetch, the pipeline continues with the remaining ones and reports the error.

## Author

Joel Buenrostro — [LinkedIn](https://www.linkedin.com/in/joelbuenrostro/) · [GitHub](https://github.com/joelbuenrostro)
