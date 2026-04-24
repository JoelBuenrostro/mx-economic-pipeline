"""
schemas.py
Pydantic schemas for request validation and response serialization.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict

class EconomicRecordOut(BaseModel):
    serie: str
    fecha: datetime
    valor: float

    model_config = ConfigDict(from_attributes=True)


class SerieInfo(BaseModel):
    key: str
    serie_id: str
    descripcion: str


class PaginatedRecords(BaseModel):
    serie: str
    total: int
    records: list[EconomicRecordOut]


class HealthResponse(BaseModel):
    status: str
    series_disponibles: list[str]
    total_registros: int