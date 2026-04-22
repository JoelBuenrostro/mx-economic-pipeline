"""
transformer.py
Validates and transforms raw records returned by extractor.py.
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

VALIDATION_RULES = {
    "usd_mxn": {"min": 5.0,   "max": 100.0},
    "inpc":    {"min": 1.0,   "max": 500.0},
    "tiie_28": {"min": 0.001, "max": 100.0},
}

DATE_FORMATS = ["%d/%m/%Y", "%Y-%m-%d"]


def _parse_date(date_str: str) -> Optional[datetime]:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def transform(records: list[dict], serie_key: str) -> pd.DataFrame:
    """
    Validates and transforms raw records from the extractor.

    Steps:
        1. Parse and validate dates.
        2. Validate numeric values are within expected range.
        3. Drop duplicates by date, keeping the last occurrence.
        4. Sort by date ascending.

    Args:
        records:   Raw records from extractor — list of {serie, fecha, valor}.
        serie_key: Series identifier used to look up validation rules.

    Returns:
        Clean pandas DataFrame with columns: serie, fecha (datetime), valor (float).

    Raises:
        ValueError: If serie_key has no validation rules defined.
        ValueError: If the resulting DataFrame is empty after cleaning.
    """
    if serie_key not in VALIDATION_RULES:
        raise ValueError(
            f"No hay reglas de validación para '{serie_key}'. "
            f"Series conocidas: {list(VALIDATION_RULES.keys())}"
        )

    if not records:
        raise ValueError(f"No se recibieron registros para la serie '{serie_key}'.")

    rules = VALIDATION_RULES[serie_key]
    logger.info(
        "Transformando %d registros para serie '%s' — rango válido: [%s, %s]",
        len(records), serie_key, rules["min"], rules["max"],
    )

    df = pd.DataFrame(records)

    initial_count = len(df)

    parsed_dates = df["fecha"].apply(_parse_date)
    invalid_dates_mask = parsed_dates.isna()
    if invalid_dates_mask.any():
        logger.warning(
            "Serie '%s': %d registros con fecha no parseable — serán descartados: %s",
            serie_key,
            invalid_dates_mask.sum(),
            df.loc[invalid_dates_mask, "fecha"].tolist(),
        )
    df = df[~invalid_dates_mask].copy()
    df["fecha"] = parsed_dates[~invalid_dates_mask]

    out_of_range_mask = (df["valor"] < rules["min"]) | (df["valor"] > rules["max"])
    if out_of_range_mask.any():
        logger.warning(
            "Serie '%s': %d registros fuera de rango [%s, %s] — serán descartados:\n%s",
            serie_key,
            out_of_range_mask.sum(),
            rules["min"],
            rules["max"],
            df.loc[out_of_range_mask, ["fecha", "valor"]].to_string(index=False),
        )
    df = df[~out_of_range_mask].copy()

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["fecha"], keep="last")
    if len(df) < before_dedup:
        logger.warning(
            "Serie '%s': %d duplicados por fecha eliminados.",
            serie_key, before_dedup - len(df),
        )

    df = df.sort_values("fecha").reset_index(drop=True)

    discarded = initial_count - len(df)
    logger.info(
        "Serie '%s': transformación completa — %d registros válidos, %d descartados.",
        serie_key, len(df), discarded,
    )

    if df.empty:
        raise ValueError(
            f"Serie '{serie_key}': ningún registro sobrevivió la validación. "
            "Revisa el rango de fechas o los datos fuente."
        )

    return df[["serie", "fecha", "valor"]]


def transform_all(raw: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    """
    Transforms all series returned by fetch_all_series.

    Series that fail transformation are logged and excluded.
    Returns a dict of serie_key -> clean DataFrame.
    """
    results = {}
    for key, records in raw.items():
        try:
            results[key] = transform(records, key)
        except Exception as exc:
            logger.error(
                "Serie '%s' falló en transformación y será omitida: %s", key, exc
            )
    return results