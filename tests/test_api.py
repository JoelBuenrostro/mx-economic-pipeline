"""
test_api.py
Integration tests for the FastAPI layer.
Uses a temporary file-based SQLite DB to avoid in-memory connection scope issues.
No Banxico token required.
"""

import os
import tempfile
import pytest

from datetime import datetime
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pipeline.models import Base, EconomicRecord
from api.main import app
from api.database import get_db


# ── Shared temp DB fixture ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def db_path(tmp_path_factory):
    """Crea un archivo SQLite temporal que dura toda la sesión de tests."""
    return str(tmp_path_factory.mktemp("db") / "test.db")


@pytest.fixture(scope="session")
def test_engine(db_path):
    """Engine conectado al archivo temporal, con tablas creadas."""
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(autouse=True)
def clean_db(test_engine):
    """Limpia todos los registros antes de cada test."""
    with Session(test_engine) as session:
        session.query(EconomicRecord).delete()
        session.commit()
    yield


@pytest.fixture
def client(test_engine):
    """TestClient con la DB de test inyectada."""
    def override_get_db():
        with Session(test_engine) as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def seeded_client(test_engine):
    """TestClient con registros de ejemplo precargados."""
    with Session(test_engine) as session:
        session.add_all([
            EconomicRecord(serie="usd_mxn", fecha=datetime(2024, 1, 1), valor=17.15),
            EconomicRecord(serie="usd_mxn", fecha=datetime(2024, 1, 2), valor=17.20),
            EconomicRecord(serie="usd_mxn", fecha=datetime(2024, 2, 1), valor=17.05),
            EconomicRecord(serie="inpc",    fecha=datetime(2024, 1, 1), valor=131.5),
            EconomicRecord(serie="tiie_28", fecha=datetime(2024, 1, 1), valor=11.25),
        ])
        session.commit()

    def override_get_db():
        with Session(test_engine) as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()


# ── /health ───────────────────────────────────────────────────────────────────

class TestHealth:

    def test_health_empty_db(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["total_registros"] == 0
        assert set(body["series_disponibles"]) == {"usd_mxn", "inpc", "tiie_28"}

    def test_health_counts_records(self, seeded_client):
        r = seeded_client.get("/health")
        assert r.json()["total_registros"] == 5


# ── /series ───────────────────────────────────────────────────────────────────

class TestListSeries:

    def test_returns_three_series(self, client):
        r = client.get("/series")
        assert r.status_code == 200
        assert len(r.json()) == 3

    def test_series_have_required_fields(self, client):
        r = client.get("/series")
        for item in r.json():
            assert "key" in item
            assert "serie_id" in item
            assert "descripcion" in item

    def test_series_keys(self, client):
        r = client.get("/series")
        keys = {item["key"] for item in r.json()}
        assert keys == {"usd_mxn", "inpc", "tiie_28"}


# ── /series/{key}/datos ───────────────────────────────────────────────────────

class TestGetDatos:

    def test_returns_all_records(self, seeded_client):
        r = seeded_client.get("/series/usd_mxn/datos")
        assert r.status_code == 200
        body = r.json()
        assert body["serie"] == "usd_mxn"
        assert body["total"] == 3
        assert len(body["records"]) == 3

    def test_unknown_serie_returns_404(self, client):
        r = client.get("/series/serie_falsa/datos")
        assert r.status_code == 404
        assert "no encontrada" in r.json()["detail"]

    def test_filter_fecha_inicio(self, seeded_client):
        r = seeded_client.get("/series/usd_mxn/datos?fecha_inicio=2024-01-15")
        body = r.json()
        assert body["total"] == 1
        assert body["records"][0]["valor"] == pytest.approx(17.05)

    def test_filter_fecha_fin(self, seeded_client):
        r = seeded_client.get("/series/usd_mxn/datos?fecha_fin=2024-01-01")
        body = r.json()
        assert body["total"] == 1
        assert body["records"][0]["valor"] == pytest.approx(17.15)

    def test_filter_date_range(self, seeded_client):
        r = seeded_client.get(
            "/series/usd_mxn/datos?fecha_inicio=2024-01-01&fecha_fin=2024-01-02"
        )
        assert r.json()["total"] == 2

    def test_records_sorted_ascending(self, seeded_client):
        r = seeded_client.get("/series/usd_mxn/datos")
        fechas = [rec["fecha"] for rec in r.json()["records"]]
        assert fechas == sorted(fechas)

    def test_empty_db_returns_zero(self, client):
        r = client.get("/series/usd_mxn/datos")
        body = r.json()
        assert body["total"] == 0
        assert body["records"] == []

    def test_response_schema(self, seeded_client):
        r = seeded_client.get("/series/inpc/datos")
        record = r.json()["records"][0]
        assert "serie" in record
        assert "fecha" in record
        assert "valor" in record

    def test_date_range_no_results(self, seeded_client):
        r = seeded_client.get(
            "/series/usd_mxn/datos?fecha_inicio=2020-01-01&fecha_fin=2020-12-31"
        )
        assert r.json()["total"] == 0