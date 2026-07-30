"""Generate manuscript-facing reports exclusively from persisted result data."""

import json
from pathlib import Path
from typing import Any

from src.ml.artifacts import json_default

REPOSITORY_URL = "https://github.com/geo-bricex/mlops-supabase-university-offers"


def _json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        default=json_default,
    )


def _model_table(results: dict[str, Any]) -> str:
    rows = [
        (
            "| Scenario | Algorithm | CV AP mean | CV AP SD | CV F1 | "
            "Test AP | Test ROC AUC | Test F1 @0.5 | Status |"
        ),
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for scenario in results["scenarios"]:
        for model in scenario["models"]:
            test = model["test_metrics_at_0_5"]
            rows.append(
                "| {scenario} | {algorithm} | {cv:.6f} | {sd:.6f} | "
                "{f1:.6f} | {ap:.6f} | {roc:.6f} | {test_f1:.6f} | "
                "{status} |".format(
                    scenario=scenario["name"],
                    algorithm=model["algorithm"],
                    cv=model["cv_mean"],
                    sd=model["cv_std"],
                    f1=model["cv_f1_mean"],
                    ap=test["average_precision"],
                    roc=test["roc_auc"],
                    test_f1=test["f1"],
                    status=model["model_status"],
                )
            )
    return "\n".join(rows)


def _scenario_table(results: dict[str, Any]) -> str:
    rows = [
        (
            "| Scenario | Role | Winner | CV AP mean ± SD | Test AP | "
            "Test F1 @0.5 | Operational threshold | Operational F1 |"
        ),
        "|---|---|---|---:|---:|---:|---:|---:|",
    ]
    for scenario in results["scenarios"]:
        winner = next(
            model
            for model in scenario["models"]
            if model["algorithm"] == scenario["selected_model"]
        )
        test = winner["test_metrics_at_0_5"]
        operational = winner["test_metrics_operational"]
        rows.append(
            "| {name} | {role} | {winner} | {cv:.6f} ± {sd:.6f} | "
            "{ap:.6f} | {f1:.6f} | {threshold:.2f} | {op_f1:.6f} |".format(
                name=scenario["name"],
                role=scenario["role"],
                winner=scenario["selected_model"],
                cv=winner["cv_mean"],
                sd=winner["cv_std"],
                ap=test["average_precision"],
                f1=test["f1"],
                threshold=operational["threshold"],
                op_f1=operational["f1"],
            )
        )
    return "\n".join(rows)


def write_hyperparameter_report(
    results: dict[str, Any],
    path: Path,
) -> None:
    """Write the complete grouped-selection report from real results."""
    model_sections = []
    for scenario in results["scenarios"]:
        for model in scenario["models"]:
            reference = model["test_metrics_at_0_5"]
            operational = model.get("test_metrics_operational")
            operational_text = (
                "Not applicable; an operational threshold is selected only "
                "for the scenario winner."
                if operational is None
                else (
                    f"threshold={operational['threshold']:.2f}, "
                    f"precision={operational['precision']:.6f}, "
                    f"recall={operational['recall']:.6f}, "
                    f"F1={operational['f1']:.6f}, confusion "
                    f"matrix={operational['confusion_matrix']}."
                )
            )
            model_sections.append(
                f"""### {scenario["name"]}: {model["algorithm"]}

- Categorical encoding: `{model["categorical_encoding"]}`
- Search space: `{_json(model["search_space"])}`
- Final parameters: `{_json(model["best_params"])}`
- Grouped-CV Average Precision: {model["cv_mean"]:.6f} ± {model["cv_std"]:.6f}
- Grouped-OOF F1 at 0.5: {model["cv_f1_mean"]:.6f} ± {model["cv_f1_std"]:.6f}
- Mean fit time of the winning configuration: {model["best_mean_fit_time"]:.6f} s
- Sealed test at 0.5: accuracy={reference["accuracy"]:.6f}, precision={reference["precision"]:.6f}, recall={reference["recall"]:.6f}, F1={reference["f1"]:.6f}, ROC AUC={reference["roc_auc"]:.6f}, Average Precision={reference["average_precision"]:.6f}, confusion matrix={reference["confusion_matrix"]}.
- Operational test metrics: {operational_text}
"""
            )

    primary = next(
        scenario
        for scenario in results["scenarios"]
        if scenario["name"] == results["primary_scenario"]
    )
    selected = next(
        model
        for model in primary["models"]
        if model["algorithm"] == results["selected_model"]
    )
    reference = selected["test_metrics_at_0_5"]
    operational = selected["test_metrics_operational"]
    content = f"""# Grouped hyperparameter selection report

## Experiment identity

- Run ID: `{results["run_id"]}`
- UTC start: `{results["created_at"]}`
- Git commit executed: `{results["environment"]["git_commit"]}`
- Dirty worktree at start: `{results["environment"]["git_dirty"]}`
- Logical dataset SHA-256: `{results["dataset"]["dataset_sha256"]}`
- Source SHA-256: `{results["dataset"].get("source_sha256", "not available")}`
- Rows/groups: {results["dataset"]["dataset_rows"]}/{results["dataset"]["dataset_groups"]}
- Training rows/groups: {results["dataset"]["train_rows"]}/{results["dataset"]["train_groups"]}
- Sealed-test rows/groups: {results["dataset"]["test_rows"]}/{results["dataset"]["test_groups"]}
- Train/test group overlap: {results["dataset"]["group_overlap_count"]}
- Actual row-level test fraction: {results["dataset"]["row_test_fraction"]:.6f}
- Python/scikit-learn: {results["environment"]["python_version"]}/{results["environment"]["sklearn_version"]}

## Method

One `natural_key` group is assigned wholly to either training or test. The approximately 80/20 holdout is selected reproducibly from `StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=42)` candidates by minimizing the combined row-size and prevalence deviation. The sealed test is not used for feature-scenario choice, model selection, hyperparameter selection, categorical-encoding analysis, preprocessing, or threshold selection.

Within training, every `RandomizedSearchCV` uses five-fold `StratifiedGroupKFold`, 40 sampled configurations, `groups=natural_key` passed explicitly to `fit`, `scoring="average_precision"`, `refit=True`, and `n_jobs=-1`. Imputation, one-hot encoding, scaling where applicable, and classification remain inside `Pipeline`/`ColumnTransformer`. True OOF probabilities are generated with the same grouped folds; no training metric uses in-sample predictions.

The primary scenario is `leakage_controlled`. It excludes deterministic label proxies and outputs of the same normalization/rule operations that construct `actual_label`: `{_json(primary["feature_definition"]["excluded_features"])}`. The `full_feature` scenario is a sensitivity analysis of rule reproduction and must not be interpreted as independent discovery of data-quality defects.

The primary model is selected by mean grouped-CV Average Precision, with grouped-OOF F1 at 0.5 only for an exact tie. A reference threshold of 0.5 is retained. The operational threshold is selected before test access by maximizing F2 over the predeclared OOF grid 0.05–0.95 in steps of 0.01.

## Full model comparison

{_model_table(results)}

## Scenario comparison

{_scenario_table(results)}

## Search spaces, final parameters, and metrics

{"".join(model_sections)}
## Primary result

The selected primary model is **{results["selected_model"]}**, with grouped-CV Average Precision **{selected["cv_mean"]:.6f} ± {selected["cv_std"]:.6f}**. At threshold 0.5, sealed-test Average Precision is {reference["average_precision"]:.6f}, ROC AUC is {reference["roc_auc"]:.6f}, precision is {reference["precision"]:.6f}, recall is {reference["recall"]:.6f}, and F1 is {reference["f1"]:.6f}. The OOF-selected operational threshold is **{operational["threshold"]:.2f}**, yielding sealed-test precision {operational["precision"]:.6f}, recall {operational["recall"]:.6f}, and F1 {operational["f1"]:.6f}.

## Prediction provenance

Training probabilities are labeled `oof_train`; sealed holdout probabilities are labeled `sealed_test`; scores produced by the separately refitted full-data deployment artifact are labeled `production_inference` and are excluded from performance estimation. Counts: `{_json(results["prediction_provenance"]["counts"])}`. Prediction evidence SHA-256: `{results["prediction_provenance"]["sha256"]}`.

## Differences from the supplied manuscript PDF

The PDF reports Random Forest, 4,650 positive rows (23.2%), accuracy 0.812, precision 0.644, recall 0.895, F1 0.749, ROC AUC 0.955, Average Precision 0.880, a predicted-positive rate of 32.3%, and an overfitting gap of 0.030. Those values do not originate from this grouped, leakage-controlled experiment and must not be retained as final results. The definitive values are the run-specific results above. The former row-stratified run is also superseded because duplicate `natural_key` groups could cross evaluation boundaries and training monitoring used in-sample probabilities.

## Registry and limitations

Persistence status: `{results["persistence"]["status"]}`. The registry separates three primary candidates from sensitivity-scenario evaluations and stores OOF/test/production origins. Storage status: `{results["persistence"]["storage"]["status"]}`.

The target remains an operational proxy derived from deterministic audit rules rather than independently adjudicated ground truth. Grouped validation prevents duplicate-key leakage but does not establish external or temporal generalizability. The full-feature scenario measures rule reproduction. Automatic drift-triggered retraining is not implemented; monitoring remains human-supervised.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_editorial_response(
    results: dict[str, Any],
    path: Path,
) -> None:
    """Write an English response and final manuscript-ready paragraph."""
    primary = next(
        scenario
        for scenario in results["scenarios"]
        if scenario["name"] == results["primary_scenario"]
    )
    selected = next(
        model
        for model in primary["models"]
        if model["algorithm"] == results["selected_model"]
    )
    spaces = "; ".join(
        f"{model['algorithm']}: {_json(model['search_space'])}"
        for model in primary["models"]
    )
    parameters = "; ".join(
        f"{model['algorithm']}: {_json(model['best_params'])}"
        for model in primary["models"]
    )
    paragraph = (
        "Hyperparameter selection was performed exclusively on the training "
        "data using RandomizedSearchCV with five-fold stratified group "
        "cross-validation (StratifiedGroupKFold, shuffle=True, "
        "random_state=42), with natural_key supplied as the grouping variable, "
        "40 sampled configurations per algorithm, "
        'scoring="average_precision", refit=True, and parallel execution. '
        f"The evaluated primary-scenario spaces were {spaces}. The final "
        f"parameters were {parameters}. The primary leakage-controlled "
        "scenario removed deterministic proxies and transformation outputs "
        "used directly by the label-generating audit rules, whereas the "
        "full-feature scenario was retained only as a rule-reproduction "
        "sensitivity analysis. All imputers, sparse one-hot encoders, the "
        "Logistic Regression scaler, and classifiers were encapsulated in "
        "scikit-learn Pipeline and ColumnTransformer objects and fitted within "
        "each fold. Training probabilities were generated exclusively out of "
        "fold, and the group-disjoint 20% test set remained sealed until the "
        "feature scenario, three candidate configurations, winner, and F2 "
        "operational threshold had been fixed from training data. "
        f"{results['selected_model']} was selected with grouped-CV Average "
        f"Precision {selected['cv_mean']:.6f} "
        f"(SD {selected['cv_std']:.6f}); grouped-OOF F1 was used only for an "
        "exact tie. Search results, grouped partitions, prediction provenance, "
        "dataset and software hashes, test metrics at 0.5 and at the "
        "OOF-selected threshold, artifacts, and candidate states are recorded "
        "in the Supabase/PostgreSQL registry and the public repository "
        f"({REPOSITORY_URL}) under run {results['run_id']}."
    )
    content = f"""# Editorial response: grouped hyperparameter selection

We thank the editor/reviewer for prompting a stricter examination of duplicate-record dependence and semantic leakage. The revised workflow now treats `natural_key` as a group in both the holdout and every cross-validation fold, uses true grouped OOF probabilities for training estimates and threshold selection, separates a leakage-controlled primary analysis from a full-feature rule-reproduction sensitivity analysis, and records prediction origin explicitly.

## Manuscript-ready paragraph

{paragraph}
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_feature_ablation_report(
    results: dict[str, Any],
    path: Path,
) -> None:
    """Write scenario and categorical-encoding sensitivity evidence."""
    encoding_rows = [
        (
            "| Algorithm | Encoding | Compatible | OOF AP | OOF F1 @0.5 | "
            "Features | Sparse | Matrix bytes | Time (s) |"
        ),
        "|---|---|---|---:|---:|---:|---|---:|---:|",
    ]
    for row in results["encoding_comparison"]:
        encoding_rows.append(
            "| {algorithm} | {encoding} | {compatible} | {ap} | {f1} | "
            "{features} | {sparse} | {memory} | {time} |".format(
                algorithm=row["algorithm"],
                encoding=row["encoding"],
                compatible=row["compatible"],
                ap=(
                    f"{row['oof_average_precision']:.6f}"
                    if row["oof_average_precision"] is not None
                    else "n/a"
                ),
                f1=(
                    f"{row['oof_f1_at_0_5']:.6f}"
                    if row["oof_f1_at_0_5"] is not None
                    else "n/a"
                ),
                features=row["transformed_features"] or "n/a",
                sparse=row["output_sparse"],
                memory=row["transformed_matrix_bytes"] or "n/a",
                time=f"{row['oof_duration_seconds']:.3f}",
            )
        )
    primary = results["scenario_definitions"]["leakage_controlled"]
    full = results["scenario_definitions"]["full_feature"]
    content = f"""# Feature and encoding sensitivity report

Run ID: `{results["run_id"]}`

## Prespecified scenarios

### Leakage-controlled primary analysis

- Included features: `{_json(primary["included_features"])}`
- Excluded features and rationale: `{_json(primary["excluded_features"])}`
- Interpretation: {primary["interpretation"]}

### Full-feature sensitivity analysis

- Included features: `{_json(full["included_features"])}`
- Excluded features: none
- Interpretation: {full["interpretation"]}

## Scenario results

{_scenario_table(results)}

## Categorical-encoding comparison on identical grouped training folds

{chr(10).join(encoding_rows)}

Sparse one-hot encoding is the primary representation because the predictors are nominal and integer codes from an ordinal encoder would impose unsupported order relations. The ordinal results are retained only as a training-set sensitivity benchmark using identical groups and classifier hyperparameters. Matrix memory is the realized transformed design-matrix footprint, not total process peak memory.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_rule_catalog(
    results: dict[str, Any],
    path: Path,
) -> None:
    """Write the executed rule catalog and non-exclusive event counts."""
    catalog = results["dataset"].get("rule_catalog", [])
    counts = results["dataset"].get("rule_counts", {})
    rows = [
        (
            "| ID | Issue type | Dimension | Severity | Events | "
            "Affected rows | Affected groups | Label-positive rows |"
        ),
        "|---|---|---|---|---:|---:|---:|---:|",
    ]
    details = []
    for rule in catalog:
        count = counts.get(rule["rule_id"], {})
        rows.append(
            "| {rule_id} | {issue_type} | {dimension} | {severity} | "
            "{events} | {rows} | {groups} | {labels} |".format(
                rule_id=rule["rule_id"],
                issue_type=rule["issue_type"],
                dimension=rule["dimension"],
                severity=rule["severity"],
                events=count.get("event_count", 0),
                rows=count.get("affected_row_count", 0),
                groups=count.get("affected_group_count", 0),
                labels=count.get("label_positive_row_count", 0),
            )
        )
        details.append(
            f"""### {rule["rule_id"]}: {rule["name"]}

- Required columns: `{_json(rule["required_columns"])}`
- Condition: {rule["condition"]}
- Event type/granularity: `{rule["issue_type"]}` / `{rule["event_granularity"]}`
- Severity: `{rule["severity"]}`
- Contributes to `actual_label`: `{rule["contributes_to_label"]}`
- Version: `{rule["version"]}`
- Description: {rule["description"]}
"""
        )
    content = f"""# Executed data-quality rule catalog

Run ID: `{results["run_id"]}`

This catalog is generated from the same Python metadata used by the audit engine. It lists only implemented rules. Audit-event counts are not mutually exclusive: one row or `natural_key` may violate multiple rules. ETL rows loaded/skipped, normalization diagnostics, audit events, directly affected rows, and group-propagated positive labels are distinct quantities.

## Run counts

{chr(10).join(rows)}

## Rule definitions

{"".join(details).rstrip()}
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
