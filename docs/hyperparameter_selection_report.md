# Grouped hyperparameter selection report

## Experiment identity

- Run ID: `8c464366-c5ab-433a-abb0-380bad37683a`
- UTC start: `2026-07-30T15:53:21.390522+00:00`
- Git commit executed: `ba33dc1eb0f3f34c3d73c80694ddc75e23e3a60e`
- Dirty worktree at start: `True`
- Logical dataset SHA-256: `786449b2d7499f96e290d71f7cf5c33bcf0fb75a5964a8e6c587841880d03701`
- Source SHA-256: `fe366924ce44b577c74f72282b042ca7908aedf59445db00893b9a3b2d58848f`
- Rows/groups: 20045/18179
- Training rows/groups: 16036/14544
- Sealed-test rows/groups: 4009/3635
- Train/test group overlap: 0
- Actual row-level test fraction: 0.200000
- Python/scikit-learn: 3.14.6/1.9.0

## Method

One `natural_key` group is assigned wholly to either training or test. The approximately 80/20 holdout is selected reproducibly from `StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=42)` candidates by minimizing the combined row-size and prevalence deviation. The sealed test is not used for feature-scenario choice, model selection, hyperparameter selection, categorical-encoding analysis, preprocessing, or threshold selection.

Within training, every `RandomizedSearchCV` uses five-fold `StratifiedGroupKFold`, 40 sampled configurations, `groups=natural_key` passed explicitly to `fit`, `scoring="average_precision"`, `refit=True`, and `n_jobs=-1`. Imputation, one-hot encoding, scaling where applicable, and classification remain inside `Pipeline`/`ColumnTransformer`. True OOF probabilities are generated with the same grouped folds; no training metric uses in-sample predictions.

The primary scenario is `leakage_controlled`. It excludes deterministic label proxies and outputs of the same normalization/rule operations that construct `actual_label`: `{"canton_norm": "Output of the same territory-normalization process evaluated by missing_territory_norm and invalid_territory_pair.", "carrera_name_len": "A zero length is a direct proxy for missing program name.", "estado": "Used by the conflicting_estado rule; excluded to avoid learning a direct input to the label-generating consistency check.", "geo_method": "Direct diagnostic output of the territory-matching operation used by the territorial quality rules.", "geo_score_canton": "Direct confidence output of the canton normalization operation.", "geo_score_prov": "Direct confidence output of the province normalization operation.", "has_canton_norm": "Deterministically reproduces part of missing_territory_norm.", "has_nombre_carrera": "Deterministically reproduces the missing_nombre_carrera rule condition.", "has_nombre_ies": "Deterministically reproduces the missing_nombre_ies rule condition.", "has_provincia_norm": "Deterministically reproduces part of missing_territory_norm.", "ies_name_len": "A zero length is a direct proxy for missing institution name.", "natural_key_token_count": "Derived from the business key used by the duplicate rule and from fields participating in completeness checks.", "provincia_norm": "Output of the same territory-normalization process evaluated by missing_territory_norm and invalid_territory_pair."}`. The `full_feature` scenario is a sensitivity analysis of rule reproduction and must not be interpreted as independent discovery of data-quality defects.

The primary model is selected by mean grouped-CV Average Precision, with grouped-OOF F1 at 0.5 only for an exact tie. A reference threshold of 0.5 is retained. The operational threshold is selected before test access by maximizing F2 over the predeclared OOF grid 0.05–0.95 in steps of 0.01.

## Full model comparison

| Scenario | Algorithm | CV AP mean | CV AP SD | CV F1 | Test AP | Test ROC AUC | Test F1 @0.5 | Status |
|---|---|---:|---:|---:|---:|---:|---:|---|
| leakage_controlled | LogisticRegression | 0.598698 | 0.012425 | 0.550263 | 0.630255 | 0.830285 | 0.575307 | rejected |
| leakage_controlled | GradientBoostingClassifier | 0.627654 | 0.015247 | 0.558527 | 0.656070 | 0.847090 | 0.576206 | rejected |
| leakage_controlled | RandomForestClassifier | 0.631359 | 0.013577 | 0.616500 | 0.655390 | 0.849264 | 0.631330 | selected |
| full_feature | LogisticRegression | 0.841834 | 0.013937 | 0.739279 | 0.848508 | 0.941698 | 0.745320 | rejected |
| full_feature | GradientBoostingClassifier | 0.905503 | 0.009882 | 0.810189 | 0.904379 | 0.964856 | 0.799318 | rejected |
| full_feature | RandomForestClassifier | 0.915518 | 0.005057 | 0.825815 | 0.916634 | 0.970233 | 0.818641 | selected |

## Scenario comparison

| Scenario | Role | Winner | CV AP mean ± SD | Test AP | Test F1 @0.5 | Operational threshold | Operational F1 |
|---|---|---|---:|---:|---:|---:|---:|
| leakage_controlled | primary | RandomForestClassifier | 0.631359 ± 0.013577 | 0.655390 | 0.631330 | 0.36 | 0.558623 |
| full_feature | sensitivity | RandomForestClassifier | 0.915518 ± 0.005057 | 0.916634 | 0.818641 | 0.20 | 0.792539 |

## Search spaces, final parameters, and metrics

### leakage_controlled: LogisticRegression

- Categorical encoding: `onehot`
- Search space: `[{"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__l1_ratio": [1.0], "model__max_iter": [500, 1000, 2000], "model__penalty": ["l1"], "model__solver": ["saga"]}, {"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__l1_ratio": [0.0], "model__max_iter": [500, 1000, 2000], "model__penalty": ["l2"], "model__solver": ["lbfgs", "liblinear", "saga"]}, {"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__l1_ratio": [0.1, 0.25, 0.5, 0.75, 0.9], "model__max_iter": [500, 1000, 2000], "model__penalty": ["elasticnet"], "model__solver": ["saga"]}]`
- Final parameters: `{"model__C": 0.3, "model__class_weight": null, "model__l1_ratio": 1.0, "model__max_iter": 500, "model__penalty": "l1", "model__solver": "saga"}`
- Grouped-CV Average Precision: 0.598698 ± 0.012425
- Grouped-OOF F1 at 0.5: 0.550263 ± 0.021228
- Mean fit time of the winning configuration: 1.970326 s
- Sealed test at 0.5: accuracy=0.836119, precision=0.722403, recall=0.477981, F1=0.575307, ROC AUC=0.830285, Average Precision=0.630255, confusion matrix=[[2907, 171], [486, 445]].
- Operational test metrics: Not applicable; an operational threshold is selected only for the scenario winner.
### leakage_controlled: GradientBoostingClassifier

- Categorical encoding: `onehot`
- Search space: `{"model__learning_rate": [0.01, 0.03, 0.05, 0.1, 0.2], "model__max_depth": [1, 2, 3, 4], "model__min_samples_leaf": [1, 2, 5, 10], "model__min_samples_split": [2, 5, 10, 20], "model__n_estimators": [50, 100, 150, 200, 300], "model__subsample": [0.6, 0.75, 0.9, 1.0]}`
- Final parameters: `{"model__learning_rate": 0.05, "model__max_depth": 4, "model__min_samples_leaf": 10, "model__min_samples_split": 2, "model__n_estimators": 200, "model__subsample": 0.6}`
- Grouped-CV Average Precision: 0.627654 ± 0.015247
- Grouped-OOF F1 at 0.5: 0.558527 ± 0.023346
- Mean fit time of the winning configuration: 5.813118 s
- Sealed test at 0.5: accuracy=0.835620, precision=0.717949, recall=0.481203, F1=0.576206, ROC AUC=0.847090, Average Precision=0.656070, confusion matrix=[[2902, 176], [483, 448]].
- Operational test metrics: Not applicable; an operational threshold is selected only for the scenario winner.
### leakage_controlled: RandomForestClassifier

- Categorical encoding: `onehot`
- Search space: `{"model__class_weight": [null, "balanced", "balanced_subsample"], "model__max_depth": [null, 5, 10, 20, 30], "model__max_features": ["sqrt", "log2", 0.5, null], "model__min_samples_leaf": [1, 2, 4, 8], "model__min_samples_split": [2, 5, 10, 20], "model__n_estimators": [100, 200, 300, 500]}`
- Final parameters: `{"model__class_weight": "balanced_subsample", "model__max_depth": 10, "model__max_features": 0.5, "model__min_samples_leaf": 2, "model__min_samples_split": 2, "model__n_estimators": 300}`
- Grouped-CV Average Precision: 0.631359 ± 0.013577
- Grouped-OOF F1 at 0.5: 0.616500 ± 0.012880
- Mean fit time of the winning configuration: 32.433031 s
- Sealed test at 0.5: accuracy=0.805687, precision=0.564298, recall=0.716434, F1=0.631330, ROC AUC=0.849264, Average Precision=0.655390, confusion matrix=[[2563, 515], [264, 667]].
- Operational test metrics: threshold=0.36, precision=0.419268, recall=0.836735, F1=0.558623, confusion matrix=[[1999, 1079], [152, 779]].
### full_feature: LogisticRegression

- Categorical encoding: `onehot`
- Search space: `[{"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__l1_ratio": [1.0], "model__max_iter": [500, 1000, 2000], "model__penalty": ["l1"], "model__solver": ["saga"]}, {"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__l1_ratio": [0.0], "model__max_iter": [500, 1000, 2000], "model__penalty": ["l2"], "model__solver": ["lbfgs", "liblinear", "saga"]}, {"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__l1_ratio": [0.1, 0.25, 0.5, 0.75, 0.9], "model__max_iter": [500, 1000, 2000], "model__penalty": ["elasticnet"], "model__solver": ["saga"]}]`
- Final parameters: `{"model__C": 3.0, "model__class_weight": null, "model__l1_ratio": 0.0, "model__max_iter": 1000, "model__penalty": "l2", "model__solver": "liblinear"}`
- Grouped-CV Average Precision: 0.841834 ± 0.013937
- Grouped-OOF F1 at 0.5: 0.739279 ± 0.023811
- Mean fit time of the winning configuration: 1.075261 s
- Sealed test at 0.5: accuracy=0.888002, precision=0.789663, recall=0.705693, F1=0.745320, ROC AUC=0.941698, Average Precision=0.848508, confusion matrix=[[2903, 175], [274, 657]].
- Operational test metrics: Not applicable; an operational threshold is selected only for the scenario winner.
### full_feature: GradientBoostingClassifier

- Categorical encoding: `onehot`
- Search space: `{"model__learning_rate": [0.01, 0.03, 0.05, 0.1, 0.2], "model__max_depth": [1, 2, 3, 4], "model__min_samples_leaf": [1, 2, 5, 10], "model__min_samples_split": [2, 5, 10, 20], "model__n_estimators": [50, 100, 150, 200, 300], "model__subsample": [0.6, 0.75, 0.9, 1.0]}`
- Final parameters: `{"model__learning_rate": 0.2, "model__max_depth": 4, "model__min_samples_leaf": 10, "model__min_samples_split": 20, "model__n_estimators": 300, "model__subsample": 0.6}`
- Grouped-CV Average Precision: 0.905503 ± 0.009882
- Grouped-OOF F1 at 0.5: 0.810189 ± 0.012183
- Mean fit time of the winning configuration: 21.426047 s
- Sealed test at 0.5: accuracy=0.911948, precision=0.849034, recall=0.755102, F1=0.799318, ROC AUC=0.964856, Average Precision=0.904379, confusion matrix=[[2953, 125], [228, 703]].
- Operational test metrics: Not applicable; an operational threshold is selected only for the scenario winner.
### full_feature: RandomForestClassifier

- Categorical encoding: `onehot`
- Search space: `{"model__class_weight": [null, "balanced", "balanced_subsample"], "model__max_depth": [null, 5, 10, 20, 30], "model__max_features": ["sqrt", "log2", 0.5, null], "model__min_samples_leaf": [1, 2, 4, 8], "model__min_samples_split": [2, 5, 10, 20], "model__n_estimators": [100, 200, 300, 500]}`
- Final parameters: `{"model__class_weight": null, "model__max_depth": 20, "model__max_features": 0.5, "model__min_samples_leaf": 2, "model__min_samples_split": 2, "model__n_estimators": 500}`
- Grouped-CV Average Precision: 0.915518 ± 0.005057
- Grouped-OOF F1 at 0.5: 0.825815 ± 0.010381
- Mean fit time of the winning configuration: 81.983990 s
- Sealed test at 0.5: accuracy=0.919431, precision=0.857647, recall=0.783029, F1=0.818641, ROC AUC=0.970233, Average Precision=0.916634, confusion matrix=[[2957, 121], [202, 729]].
- Operational test metrics: threshold=0.20, precision=0.687451, recall=0.935553, F1=0.792539, confusion matrix=[[2682, 396], [60, 871]].

## Primary result

The selected primary model is **RandomForestClassifier**, with grouped-CV Average Precision **0.631359 ± 0.013577**. At threshold 0.5, sealed-test Average Precision is 0.655390, ROC AUC is 0.849264, precision is 0.564298, recall is 0.716434, and F1 is 0.631330. The OOF-selected operational threshold is **0.36**, yielding sealed-test precision 0.419268, recall 0.836735, and F1 0.558623.

## Prediction provenance

Training probabilities are labeled `oof_train`; sealed holdout probabilities are labeled `sealed_test`; scores produced by the separately refitted full-data deployment artifact are labeled `production_inference` and are excluded from performance estimation. Counts: `{"oof_train": 16036, "production_inference": 20045, "sealed_test": 4009}`. Prediction evidence SHA-256: `2e1a6cd2c85b6f2d56eb9a3c46f62c84113df45dc1319821bb884989b9e1c124`.

## Differences from the supplied manuscript PDF

The PDF reports Random Forest, 4,650 positive rows (23.2%), accuracy 0.812, precision 0.644, recall 0.895, F1 0.749, ROC AUC 0.955, Average Precision 0.880, a predicted-positive rate of 32.3%, and an overfitting gap of 0.030. Those values do not originate from this grouped, leakage-controlled experiment and must not be retained as final results. The definitive values are the run-specific results above. The former row-stratified run is also superseded because duplicate `natural_key` groups could cross evaluation boundaries and training monitoring used in-sample probabilities.

## Registry and limitations

Persistence status: `success`. The registry separates three primary candidates from sensitivity-scenario evaluations and stores OOF/test/production origins. Storage status: `failed`.

The target remains an operational proxy derived from deterministic audit rules rather than independently adjudicated ground truth. Grouped validation prevents duplicate-key leakage but does not establish external or temporal generalizability. The full-feature scenario measures rule reproduction. Automatic drift-triggered retraining is not implemented; monitoring remains human-supervised.
