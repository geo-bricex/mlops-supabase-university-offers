import pandas as pd

from src.ml.quality_risk import prepare_quality_risk_frame


def test_prepare_quality_risk_frame_builds_expected_features():
    raw = pd.DataFrame(
        [
            {
                "file_id": "file-1",
                "row_num": 7,
                "natural_key": "uni-a|prog-a|campo-a|grado|online|pichincha|quito",
                "nombre_ies": "Universidad A",
                "tipo_ies": "PUBLICA",
                "tipo_financiamiento": "PUBLICO",
                "nombre_carrera": "Ingenieria",
                "campo_amplio": "Ingenieria",
                "nivel_formacion": "GRADO",
                "modalidad": "ONLINE",
                "provincia": "Pichincha",
                "canton": "Quito",
                "estado": "VIGENTE",
                "normalized_fields": {
                    "provincia_norm": "pichincha",
                    "canton_norm": "quito",
                    "geo_method": "catalog_exact",
                    "geo_score_prov": 100,
                    "geo_score_canton": 98,
                },
                "issue_count": 2,
                "issue_types": ["duplicate_natural_key", "conflicting_estado"],
                "actual_label": True,
            }
        ]
    )

    prepared = prepare_quality_risk_frame(raw)

    assert len(prepared) == 1
    assert bool(prepared.iloc[0]["actual_label"]) is True
    assert prepared.iloc[0]["provincia_norm"] == "pichincha"
    assert prepared.iloc[0]["canton_norm"] == "quito"
    assert prepared.iloc[0]["geo_method"] == "catalog_exact"
    assert prepared.iloc[0]["geo_score_prov"] == 100.0
    assert prepared.iloc[0]["has_nombre_ies"] == 1
    assert prepared.iloc[0]["has_nombre_carrera"] == 1
    assert prepared.iloc[0]["natural_key_token_count"] == 7
