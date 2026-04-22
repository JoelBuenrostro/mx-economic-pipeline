"""
extractor.py
Fetches economic time series data from Banxico's public API.
"""

import logging
import os
import httpx
from datetime import date
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BANXICO_TOKEN = os.getenv("BANXICO_TOKEN")
BANXICO_BASE_URL = "https://www.banxico.org.mx/SieAPIRest/service/v1"

SERIES = {
    "usd_mxn": "SF43718",   # Tipo de cambio USD/MXN
    "inpc":    "SP1",        # Inflación (INPC)
    "tiie_28": "SF61745",    # TIIE a 28 días
}


def fetch_series(
    serie_key: str,
    fecha_inicio: date,
    fecha_fin: date,
    token: Optional[str] = None,
) -> list[dict]:
    """
    Fetches a single economic series from Banxico for a given date range.

    Args:
        serie_key:    Key from SERIES dict (e.g. "usd_mxn").
        fecha_inicio: Start date (inclusive).
        fecha_fin:    End date (inclusive).
        token:        Optional Banxico API token. If not provided, uses BANXICO_TOKEN env var.

    Returns:
        List of dicts with keys: serie, fecha, valor.

    Raises:
        ValueError: If serie_key is not recognized.
        httpx.HTTPStatusError: On non-2xx responses.
    """
    if serie_key not in SERIES:
        raise ValueError(
            f"Serie '{serie_key}' no reconocida. Opciones: {list(SERIES.keys())}"
        )

    resolved_token = token or BANXICO_TOKEN
    if not resolved_token:
        raise ValueError(
            "Se requiere un token de Banxico. "
            "Define BANXICO_TOKEN en tu archivo .env o pásalo como argumento."
        )

    serie_id = SERIES[serie_key]
    fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
    fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")
    url = f"{BANXICO_BASE_URL}/series/{serie_id}/datos/{fecha_inicio_str}/{fecha_fin_str}"

    headers = {
        "Accept": "application/json",
        "Bmx-Token": resolved_token,
    }

    logger.info(
        "Fetching serie '%s' (%s) from %s to %s",
        serie_key, serie_id, fecha_inicio_str, fecha_fin_str,
    )

    try:
        response = httpx.get(url, headers=headers, timeout=15.0)
        response.raise_for_status()
    except httpx.TimeoutException:
        logger.error("Timeout al conectar con Banxico API para serie '%s'", serie_key)
        raise
    except httpx.HTTPStatusError as exc:
        logger.error(
            "Error HTTP %s al obtener serie '%s': %s",
            exc.response.status_code, serie_key, exc.response.text[:200],
        )
        raise

    data = response.json()

    try:
        raw_datos = data["bmx"]["series"][0]["datos"]
    except (KeyError, IndexError) as exc:
        logger.error(
            "Estructura inesperada en respuesta de Banxico para serie '%s': %s",
            serie_key, str(exc),
        )
        raise ValueError(f"Respuesta inesperada de Banxico: {exc}") from exc

    records = []
    skipped = 0
    for item in raw_datos:
        valor_str = item.get("dato", "").strip()
        if valor_str in ("N/E", "", "N/D"):
            skipped += 1
            continue
        try:
            records.append({
                "serie": serie_key,
                "fecha": item["fecha"],
                "valor": float(valor_str.replace(",", ".")),
            })
        except (ValueError, KeyError) as exc:
            logger.warning(
                "Registro omitido para serie '%s' — valor no parseable '%s': %s",
                serie_key, valor_str, exc,
            )
            skipped += 1

    logger.info(
        "Serie '%s': %d registros obtenidos, %d omitidos",
        serie_key, len(records), skipped,
    )
    return records


def fetch_all_series(
    fecha_inicio: date,
    fecha_fin: date,
    token: Optional[str] = None,
) -> dict[str, list[dict]]:
    """
    Fetches all configured series for a given date range.

    Returns:
        Dict mapping serie_key -> list of records.
        Series that fail are logged and excluded (partial results returned).
    """
    results = {}
    for key in SERIES:
        try:
            results[key] = fetch_series(key, fecha_inicio, fecha_fin, token)
        except Exception as exc:
            logger.error(
                "Serie '%s' falló y será omitida del resultado: %s", key, exc
            )
    return results