# Feature and encoding sensitivity report

Run ID: `8c464366-c5ab-433a-abb0-380bad37683a`

## Prespecified scenarios

### Leakage-controlled primary analysis

- Included features: `["tipo_ies", "tipo_financiamiento", "campo_amplio", "nivel_formacion", "modalidad"]`
- Excluded features and rationale: `{"canton_norm": "Output of the same territory-normalization process evaluated by missing_territory_norm and invalid_territory_pair.", "carrera_name_len": "A zero length is a direct proxy for missing program name.", "estado": "Used by the conflicting_estado rule; excluded to avoid learning a direct input to the label-generating consistency check.", "geo_method": "Direct diagnostic output of the territory-matching operation used by the territorial quality rules.", "geo_score_canton": "Direct confidence output of the canton normalization operation.", "geo_score_prov": "Direct confidence output of the province normalization operation.", "has_canton_norm": "Deterministically reproduces part of missing_territory_norm.", "has_nombre_carrera": "Deterministically reproduces the missing_nombre_carrera rule condition.", "has_nombre_ies": "Deterministically reproduces the missing_nombre_ies rule condition.", "has_provincia_norm": "Deterministically reproduces part of missing_territory_norm.", "ies_name_len": "A zero length is a direct proxy for missing institution name.", "natural_key_token_count": "Derived from the business key used by the duplicate rule and from fields participating in completeness checks.", "provincia_norm": "Output of the same territory-normalization process evaluated by missing_territory_norm and invalid_territory_pair."}`
- Interpretation: Predictive sensitivity to pre-audit contextual attributes after removing direct rule outputs and deterministic rule proxies.

### Full-feature sensitivity analysis

- Included features: `["tipo_ies", "tipo_financiamiento", "campo_amplio", "nivel_formacion", "modalidad", "estado", "provincia_norm", "canton_norm", "geo_method", "geo_score_prov", "geo_score_canton", "has_nombre_ies", "has_nombre_carrera", "has_provincia_norm", "has_canton_norm", "ies_name_len", "carrera_name_len", "natural_key_token_count"]`
- Excluded features: none
- Interpretation: Rule-reproduction sensitivity analysis; it is not evidence of independent discovery of unseen quality problems.

## Scenario results

| Scenario | Role | Winner | CV AP mean ± SD | Test AP | Test F1 @0.5 | Operational threshold | Operational F1 |
|---|---|---|---:|---:|---:|---:|---:|
| leakage_controlled | primary | RandomForestClassifier | 0.631359 ± 0.013577 | 0.655390 | 0.631330 | 0.36 | 0.558623 |
| full_feature | sensitivity | RandomForestClassifier | 0.915518 ± 0.005057 | 0.916634 | 0.818641 | 0.20 | 0.792539 |

## Categorical-encoding comparison on identical grouped training folds

| Algorithm | Encoding | Compatible | OOF AP | OOF F1 @0.5 | Features | Sparse | Matrix bytes | Time (s) |
|---|---|---|---:|---:|---:|---|---:|---:|
| LogisticRegression | onehot | True | 0.595496 | 0.550477 | 45 | True | 1026308 | 2.458 |
| LogisticRegression | ordinal | True | 0.271005 | 0.000000 | 5 | False | 641440 | 8.179 |
| GradientBoostingClassifier | onehot | True | 0.625721 | 0.558728 | 45 | True | 1026308 | 3.609 |
| GradientBoostingClassifier | ordinal | True | 0.623330 | 0.558752 | 5 | False | 641440 | 10.015 |
| RandomForestClassifier | onehot | True | 0.629134 | 0.616443 | 45 | True | 1026308 | 7.369 |
| RandomForestClassifier | ordinal | True | 0.624343 | 0.615705 | 5 | False | 641440 | 21.437 |

Sparse one-hot encoding is the primary representation because the predictors are nominal and integer codes from an ordinal encoder would impose unsupported order relations. The ordinal results are retained only as a training-set sensitivity benchmark using identical groups and classifier hyperparameters. Matrix memory is the realized transformed design-matrix footprint, not total process peak memory.
