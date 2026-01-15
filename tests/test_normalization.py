import pandas as pd
import pytest

from src.etl.ingest import normalize_column_name, normalize_columns


def test_normalize_column_name():
    assert normalize_column_name("NIVEL FORMACIÓN") == "NIVEL_FORMACION"
    assert normalize_column_name("CANTÓN") == "CANTON"
    assert normalize_column_name("  campo  amplio ") == "CAMPO_AMPLIO"


def test_normalize_columns_detects_duplicates():
    df = pd.DataFrame(columns=["NOMBRE IES", "NOMBRE_IES"])
    with pytest.raises(ValueError):
        normalize_columns(df)
