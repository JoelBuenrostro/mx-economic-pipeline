"""
run_pipeline.py
CLI entrypoint — runs the full ETL pipeline for a given date range.

Usage:
    python run_pipeline.py
    python run_pipeline.py --fecha-inicio 2025-01-01 --fecha-fin 2025-01-31
"""

import argparse
import logging
import os
from datetime import date, timedelta

from pipeline.extractor import fetch_all_series
from pipeline.transformer import transform_all
from pipeline.loader import load_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Run the mx-economic-pipeline ETL.")
    parser.add_argument(
        "--fecha-inicio",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=30),
        help="Start date in YYYY-MM-DD format (default: 30 days ago)",
    )
    parser.add_argument(
        "--fecha-fin",
        type=date.fromisoformat,
        default=date.today(),
        help="End date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--db-url",
        default="sqlite:///data/economic_data.db",
        help="SQLAlchemy database URL (default: sqlite:///data/economic_data.db)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    os.makedirs("data", exist_ok=True)

    logger.info(
        "Iniciando pipeline — rango: %s → %s", args.fecha_inicio, args.fecha_fin
    )

    logger.info("[ 1/3 ] Extract...")
    raw = fetch_all_series(args.fecha_inicio, args.fecha_fin)

    if not raw:
        logger.error("No se obtuvieron datos. Abortando.")
        return

    logger.info("[ 2/3 ] Transform...")
    clean = transform_all(raw)

    if not clean:
        logger.error("Ninguna serie sobrevivió la transformación. Abortando.")
        return

    logger.info("[ 3/3 ] Load...")
    results = load_all(clean, db_url=args.db_url)

    logger.info("Pipeline completado.")
    for serie, stats in results.items():
        logger.info(
            "  %s → %d insertados, %d actualizados",
            serie, stats["inserted"], stats["updated"],
        )


if __name__ == "__main__":
    main()