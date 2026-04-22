"""
models.py
SQLAlchemy ORM models for the mx-economic-pipeline database.
"""

from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class EconomicRecord(Base):
    __tablename__ = "economic_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    serie = Column(String(50), nullable=False)
    fecha = Column(DateTime, nullable=False)
    valor = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    __table_args__ = (
        UniqueConstraint("serie", "fecha", name="uq_serie_fecha"),
    )

    def __repr__(self):
        return f"<EconomicRecord serie={self.serie} fecha={self.fecha} valor={self.valor}>"