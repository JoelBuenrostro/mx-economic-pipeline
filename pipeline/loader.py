"""
loader.py
Loads transformed DataFrames into SQLite using SQLAlchemy.
Handles upserts — re-running the pipeline never produces duplicate rows.
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from pipeline.models import Base, EconomicRecord

logger = logging.getLogger(__name__)

DEFAULT_DB_URL = "sqlite:///data/economic_data.db"


def get_engine(db_url: str = DEFAULT_DB_URL):
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    return engine


def load(df: pd.DataFrame, serie_key: str, db_url: str = DEFAULT_DB_URL) -> dict:
    """
    Upserts a transformed DataFrame into the database.

    For each record:
    - If (serie, fecha) already exists → update valor.
    - If not → insert new row.

    Args:
        df:        Clean DataFrame from transformer — columns: serie, fecha, valor.
        serie_key: Used only for logging context.
        db_url:    SQLAlchemy database URL.

    Returns:
        Dict with keys: inserted (int), updated (int), total (int).
    """
    if df.empty:
        logger.warning("Serie '%s': DataFrame vacío, nada que cargar.", serie_key)
        return {"inserted": 0, "updated": 0, "total": 0}

    engine = get_engine(db_url)
    inserted = 0
    updated = 0

    logger.info(
        "Cargando %d registros para serie '%s'...", len(df), serie_key
    )

    with Session(engine) as session:
        for _, row in df.iterrows():
            existing = session.scalar(
                select(EconomicRecord).where(
                    EconomicRecord.serie == row["serie"],
                    EconomicRecord.fecha == row["fecha"],
                )
            )
            if existing:
                if existing.valor != row["valor"]:
                    existing.valor = row["valor"]
                    updated += 1
            else:
                session.add(EconomicRecord(
                    serie=row["serie"],
                    fecha=row["fecha"],
                    valor=row["valor"],
                ))
                inserted += 1

        session.commit()

    result = {"inserted": inserted, "updated": updated, "total": inserted + updated}
    logger.info(
        "Serie '%s': %d insertados, %d actualizados.",
        serie_key, inserted, updated,
    )
    return result


def load_all(
    clean: dict[str, pd.DataFrame],
    db_url: str = DEFAULT_DB_URL,
) -> dict[str, dict]:
    """
    Loads all transformed series into the database.
    Series that fail are logged and excluded from the result.
    """
    results = {}
    for key, df in clean.items():
        try:
            results[key] = load(df, key, db_url)
        except Exception as exc:
            logger.error(
                "Serie '%s' falló en carga y será omitida: %s", key, exc
            )
    return results