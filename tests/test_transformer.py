"""
test_transformer.py
Unit tests for pipeline/transformer.py — no Banxico token required.
"""

import pytest
import pandas as pd
from datetime import datetime

from pipeline.transformer import transform, transform_all, VALIDATION_RULES


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_records(serie: str, values: list[tuple]) -> list[dict]:
    """Helper: [(fecha_str, valor), ...] → list of raw records."""
    return [{"serie": serie, "fecha": f, "valor": v} for f, v in values]


VALID_USD_MXN = make_records("usd_mxn", [
    ("01/01/2024", 17.15),
    ("02/01/2024", 17.20),
    ("03/01/2024", 17.05),
])

VALID_INPC = make_records("inpc", [
    ("01/01/2024", 131.5),
    ("01/02/2024", 132.1),
])

VALID_TIIE = make_records("tiie_28", [
    ("01/01/2024", 11.25),
    ("01/02/2024", 11.00),
])


# ── Happy path ─────────────────────────────────────────────────────────────────

class TestTransformHappyPath:

    def test_returns_dataframe(self):
        df = transform(VALID_USD_MXN, "usd_mxn")
        assert isinstance(df, pd.DataFrame)

    def test_columns(self):
        df = transform(VALID_USD_MXN, "usd_mxn")
        assert list(df.columns) == ["serie", "fecha", "valor"]

    def test_row_count(self):
        df = transform(VALID_USD_MXN, "usd_mxn")
        assert len(df) == 3

    def test_fecha_is_datetime(self):
        df = transform(VALID_USD_MXN, "usd_mxn")
        assert pd.api.types.is_datetime64_any_dtype(df["fecha"])

    def test_sorted_ascending(self):
        records = make_records("usd_mxn", [
            ("03/01/2024", 17.05),
            ("01/01/2024", 17.15),
            ("02/01/2024", 17.20),
        ])
        df = transform(records, "usd_mxn")
        assert df["fecha"].is_monotonic_increasing

    def test_serie_column_value(self):
        df = transform(VALID_USD_MXN, "usd_mxn")
        assert (df["serie"] == "usd_mxn").all()

    def test_inpc(self):
        df = transform(VALID_INPC, "inpc")
        assert len(df) == 2

    def test_tiie(self):
        df = transform(VALID_TIIE, "tiie_28")
        assert len(df) == 2

    def test_iso_date_format(self):
        """Transformer should accept YYYY-MM-DD as well as DD/MM/YYYY."""
        records = make_records("usd_mxn", [
            ("2024-01-01", 17.15),
            ("2024-01-02", 17.20),
        ])
        df = transform(records, "usd_mxn")
        assert len(df) == 2
        assert df["fecha"].iloc[0] == datetime(2024, 1, 1)


# ── Validation: out-of-range values ───────────────────────────────────────────

class TestTransformOutOfRange:

    def test_drops_value_above_max(self):
        records = make_records("usd_mxn", [
            ("01/01/2024", 17.15),
            ("02/01/2024", 999.99),  # way above max=100
        ])
        df = transform(records, "usd_mxn")
        assert len(df) == 1
        assert df["valor"].iloc[0] == pytest.approx(17.15)

    def test_drops_value_below_min(self):
        records = make_records("usd_mxn", [
            ("01/01/2024", 17.15),
            ("02/01/2024", 0.001),   # below min=5.0
        ])
        df = transform(records, "usd_mxn")
        assert len(df) == 1

    def test_drops_negative_tiie(self):
        records = make_records("tiie_28", [
            ("01/01/2024", 11.25),
            ("02/01/2024", -1.0),
        ])
        df = transform(records, "tiie_28")
        assert len(df) == 1

    def test_all_out_of_range_raises(self):
        records = make_records("usd_mxn", [
            ("01/01/2024", 0.0),
            ("02/01/2024", 999.0),
        ])
        with pytest.raises(ValueError, match="ningún registro sobrevivió"):
            transform(records, "usd_mxn")


# ── Validation: bad dates ──────────────────────────────────────────────────────

class TestTransformBadDates:

    def test_drops_unparseable_date(self):
        records = make_records("usd_mxn", [
            ("01/01/2024", 17.15),
            ("not-a-date", 17.20),
        ])
        df = transform(records, "usd_mxn")
        assert len(df) == 1

    def test_all_bad_dates_raises(self):
        records = make_records("usd_mxn", [
            ("bad-date-1", 17.15),
            ("bad-date-2", 17.20),
        ])
        with pytest.raises(ValueError):
            transform(records, "usd_mxn")


# ── Deduplication ─────────────────────────────────────────────────────────────

class TestTransformDedup:

    def test_drops_duplicate_dates_keeps_last(self):
        records = make_records("usd_mxn", [
            ("01/01/2024", 17.00),
            ("01/01/2024", 17.99),  # duplicate — last should win
            ("02/01/2024", 17.50),
        ])
        df = transform(records, "usd_mxn")
        assert len(df) == 2
        jan1 = df[df["fecha"] == datetime(2024, 1, 1)]
        assert jan1["valor"].iloc[0] == pytest.approx(17.99)


# ── Edge cases ────────────────────────────────────────────────────────────────

class TestTransformEdgeCases:

    def test_empty_records_raises(self):
        with pytest.raises(ValueError, match="No se recibieron registros"):
            transform([], "usd_mxn")

    def test_unknown_serie_raises(self):
        with pytest.raises(ValueError, match="No hay reglas de validación"):
            transform(VALID_USD_MXN, "serie_inexistente")

    def test_single_valid_record(self):
        records = make_records("usd_mxn", [("01/01/2024", 17.15)])
        df = transform(records, "usd_mxn")
        assert len(df) == 1


# ── transform_all ─────────────────────────────────────────────────────────────

class TestTransformAll:

    def test_returns_all_valid_series(self):
        raw = {
            "usd_mxn": VALID_USD_MXN,
            "inpc": VALID_INPC,
            "tiie_28": VALID_TIIE,
        }
        result = transform_all(raw)
        assert set(result.keys()) == {"usd_mxn", "inpc", "tiie_28"}

    def test_excludes_failing_series(self):
        """A series that fails transformation should be excluded, not crash everything."""
        raw = {
            "usd_mxn": VALID_USD_MXN,
            "inpc": [],  # will raise ValueError — empty records
        }
        result = transform_all(raw)
        assert "usd_mxn" in result
        assert "inpc" not in result

    def test_empty_input_returns_empty(self):
        result = transform_all({})
        assert result == {}
