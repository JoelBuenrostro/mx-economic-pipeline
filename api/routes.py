"""
routes.py
API route definitions.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.schemas import EconomicRecordOut, HealthResponse, PaginatedRecords, SerieInfo
from pipeline.models import EconomicRecord
from pipeline.extractor import SERIES
from api.database import get_db

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health_check(db: Session = Depends(get_db)):
    total = db.scalar(select(func.count()).select_from(EconomicRecord))
    return HealthResponse(
        status="ok",
        series_disponibles=list(SERIES.keys()),
        total_registros=total or 0,
    )


@router.get("/series", response_model=list[SerieInfo])
def list_series():
    descripciones = {
        "usd_mxn": "Tipo de cambio USD/MXN (Fix)",
        "inpc": "Índice Nacional de Precios al Consumidor (INPC)",
        "tiie_28": "Tasa de Interés Interbancaria de Equilibrio a 28 días (TIIE)",
    }
    return [
        SerieInfo(key=key, serie_id=serie_id, descripcion=descripciones.get(key, ""))
        for key, serie_id in SERIES.items()
    ]


@router.get("/series/{serie_key}/datos", response_model=PaginatedRecords)
def get_datos(
    serie_key: str,
    fecha_inicio: Optional[datetime] = Query(default=None),
    fecha_fin: Optional[datetime] = Query(default=None),
    db: Session = Depends(get_db),
):
    if serie_key not in SERIES:
        raise HTTPException(
            status_code=404,
            detail=f"Serie '{serie_key}' no encontrada. Series válidas: {list(SERIES.keys())}",
        )

    stmt = select(EconomicRecord).where(EconomicRecord.serie == serie_key)

    if fecha_inicio:
        stmt = stmt.where(EconomicRecord.fecha >= fecha_inicio)
    if fecha_fin:
        stmt = stmt.where(EconomicRecord.fecha <= fecha_fin)

    stmt = stmt.order_by(EconomicRecord.fecha.asc())
    records = db.scalars(stmt).all()

    return PaginatedRecords(
        serie=serie_key,
        total=len(records),
        records=records,
    )