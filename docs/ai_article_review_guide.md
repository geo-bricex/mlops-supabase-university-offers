# Guide for AI-assisted article revision

## Authoritative evidence

Use these sources in this order:

1. `docs/hyperparameter_selection_report.md` for the grouped-validation method, final parameters, and real metrics.
2. `docs/editorial_response_hyperparameters.md` for the manuscript-ready English paragraph.
3. `docs/feature_ablation_report.md` for leakage-control and encoding sensitivity.
4. `docs/rule_catalog.md` for the six rules actually executed and their non-exclusive counts.
5. `reports/modeling/8c464366-c5ab-433a-abb0-380bad37683a/8c464366-c5ab-433a-abb0-380bad37683a_results.json` as the machine-readable source of truth.
6. `reports/article.md` as a concise draft, not as a substitute for the result JSON.

The authoritative completed run is `8c464366-c5ab-433a-abb0-380bad37683a`. The older run `0f6f077d-9e12-4129-93bf-7048a7d15bdc` is superseded because it used row-level rather than group-disjoint validation. Run `53d93eee-bbb9-43ef-8ca3-1639ce2f7947` was interrupted by a host restart and produced no final result JSON; it must not be cited.

## Interpretation constraints

- Report the `leakage_controlled` scenario as the primary analysis.
- Treat `full_feature` only as a rule-reproduction sensitivity analysis.
- State that `natural_key` groups are disjoint across the holdout and all five CV folds.
- State that model selection uses grouped-CV Average Precision; test metrics never select features, hyperparameters, the winner, or the operational threshold.
- Distinguish grouped OOF training estimates, sealed-test estimates, and full-data production inference.
- Do not describe the audit-derived target as independent clinical, causal, or expert-adjudicated ground truth.
- Do not claim automated drift detection or automated retraining; the workflow is human-supervised.
- Do not claim MLflow or FastAPI integration; experiment traceability uses Supabase/PostgreSQL and artifacts, while API access uses Kong/PostgREST.
- Do not reuse metrics from the supplied PDF when they differ from the authoritative run.

## Questions suitable for an AI reviewer

1. Which sentences in the manuscript conflict with the authoritative grouped experiment?
2. How should the Methods section explain group-disjoint holdout, grouped CV, OOF threshold selection, and leakage prevention?
3. Which tables and figures must be replaced with the run-specific comparison, ROC, and Precision–Recall evidence?
4. Does every numerical claim match the result JSON to the stated precision?
5. Are the primary and sensitivity scenarios labeled clearly enough to prevent overclaiming?
6. Which limitations are required for a Q1 submission given the rule-derived target and lack of external or temporal validation?
7. Does the editorial response accurately describe the search spaces and winning parameters without implying test-set selection?
8. Which architecture claims are implemented by the repository, and which should be reframed as future work?

## Known validation limits

The completed Docker run persisted successfully to the model registry, but its Storage record is `failed` because one full-feature Random Forest binary exceeded the former 50 MiB limit. The repository now defaults the local Storage limit to 256 MiB for future runs; that change does not retroactively alter the recorded run. A second identical full experiment was started but interrupted, so two-run reproducibility must not be claimed until another complete run passes `scripts/verify_docker_run.py`.
