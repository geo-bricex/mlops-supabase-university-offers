from src.etl.ingest import generate_natural_key, generate_row_hash


def test_natural_key_deterministic():
    row = {
        "nombre_norm": "uni a",
        "carrera_norm": "sistemas",
        "campo_amplio_norm": "tecnologias",
        "nivel_formacion_norm": "grado",
        "modalidad_norm": "presencial",
        "provincia_norm": "pichincha",
        "canton_norm": "quito",
    }
    assert generate_natural_key(row) == generate_natural_key(row.copy())


def test_row_hash_deterministic():
    row = {
        "natural_key": "a|b|c|d|e|f|g",
        "estado_norm": "activa",
    }
    assert generate_row_hash(row) == generate_row_hash(row.copy())
