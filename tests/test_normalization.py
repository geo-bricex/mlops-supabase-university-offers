import json

import pandas as pd
import pytest

from src.etl.ingest import (
    normalize_column_name,
    normalize_columns,
    strict_json_dumps,
)


def test_normalize_column_name():
    assert normalize_column_name("NIVEL FORMACIÓN") == "NIVEL_FORMACION"
    assert normalize_column_name("CANTÓN") == "CANTON"
    assert normalize_column_name("  campo  amplio ") == "CAMPO_AMPLIO"


def test_normalize_columns_detects_duplicates():
    df = pd.DataFrame(columns=["NOMBRE IES", "NOMBRE_IES"])
    with pytest.raises(ValueError):
        normalize_columns(df)


def test_strict_json_dumps_replaces_nested_missing_values():
    payload = {
        "canton_norm": float("nan"),
        "score": pd.NA,
        "nested": [1.0, None],
    }

    encoded = strict_json_dumps(payload)
    decoded = json.loads(encoded)

    assert "NaN" not in encoded
    assert decoded["canton_norm"] is None
    assert decoded["score"] is None
