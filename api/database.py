"""
database.py
Database session dependency for FastAPI.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pipeline.models import Base

DB_URL = "sqlite:///data/economic_data.db"

engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
Base.metadata.create_all(engine)


def get_db():
    with Session(engine) as session:
        yield session