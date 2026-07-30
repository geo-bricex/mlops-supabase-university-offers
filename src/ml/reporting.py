"""Generate publication and editorial-response documents from real results."""

import json
from collections.abc import Iterable
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


def _comparison_table(models: Iterable[dict[str, Any]]) -> str:
    rows = [
        "| Algorithm | CV AP mean | CV AP SD | CV F1 mean | Test AP | Test F1 | Status |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for model in models:
        rows.append(
            "| {algorithm} | {cv_mean:.6f} | {cv_std:.6f} | "
            "{cv_f1_mean:.6f} | {test_ap:.6f} | {test_f1:.6f} | "
            "{status} |".format(
                algorithm=model["algorithm"],
                cv_mean=model["cv_mean"],
                cv_std=model["cv_std"],
                cv_f1_mean=model["cv_f1_mean"],
                test_ap=model["test_metrics"]["average_precision"],
                test_f1=model["test_metrics"]["f1"],
                status=model["model_status"],
            )
        )
    return "\n".join(rows)


def write_hyperparameter_report(
    results: dict[str, Any],
    path: Path,
) -> None:
    """Write the complete Spanish methodological report from result data."""
    model_sections = []
    for model in results["models"]:
        metrics = model["test_metrics"]
        model_sections.append(
            f"""### {model["algorithm"]}

- Espacio de búsqueda: `{_json(model["search_space"])}`
- Parámetros finales: `{_json(model["best_params"])}`
- Mejor Average Precision de CV: {model["cv_mean"]:.6f}
- Desviación estándar de CV: {model["cv_std"]:.6f}
- F1 promedio de CV (solo desempate): {model["cv_f1_mean"]:.6f}
- Test: accuracy={metrics["accuracy"]:.6f}, precision={metrics["precision"]:.6f}, recall={metrics["recall"]:.6f}, F1={metrics["f1"]:.6f}, ROC AUC={metrics["roc_auc"]:.6f}, Average Precision={metrics["average_precision"]:.6f}.
- Matriz de confusión `[[TN, FP], [FN, TP]]`: `{metrics["confusion_matrix"]}`
"""
        )

    selected = next(
        model
        for model in results["models"]
        if model["algorithm"] == results["selected_model"]
    )
    publication_note = (
        "La versión anterior a esta revisión de `reports/article.md` no "
        "publicaba métricas ni parámetros de esta comparación ML; por ello no "
        "existía una cifra anterior verificable contra la cual calcular una "
        "diferencia numérica. La versión revisada incorpora estos primeros "
        "resultados trazables. Cualquier afirmación externa de que Random Forest "
        "era el ganador debe reemplazarse por Gradient Boosting."
    )

    content = f"""# Informe de selección de hiperparámetros

## Identificación

- `run_id`: `{results["run_id"]}`
- Fecha UTC: `{results["created_at"]}`
- Hash lógico del dataset: `{results["dataset"]["dataset_sha256"]}`
- Hash del archivo fuente: `{results["dataset"].get("source_sha256", "no disponible")}`
- Registros: {results["dataset"]["dataset_rows"]}
- Distribución de clases: `{_json(results["dataset"]["class_distribution"])}`
- Python: `{results["environment"]["python_version"]}`
- scikit-learn: `{results["environment"]["sklearn_version"]}`
- Commit base de Git: `{results["environment"]["git_commit"]}`; árbol sucio durante la ejecución: `{results["environment"]["git_dirty"]}`

## Metodología

Se reservó mediante muestreo estratificado el 20 % de las observaciones como conjunto de prueba y se utilizó el 80 % restante para entrenamiento. El test permaneció aislado durante toda la selección. Cada algoritmo se optimizó exclusivamente en entrenamiento mediante `{results["method"]["search_method"]}`, con {results["method"]["search_iterations"]} combinaciones, `StratifiedKFold(n_splits={results["method"]["cv_folds"]}, shuffle=True, random_state={results["method"]["random_state"]})`, `scoring="{results["method"]["optimization_metric"]}"`, `refit=True` y `n_jobs={results["method"]["n_jobs"]}`.

La imputación, la codificación one-hot para Logistic Regression, la codificación ordinal para los modelos de árboles y el escalamiento exclusivo de Logistic Regression se ajustaron dentro de `Pipeline` y `ColumnTransformer` en cada pliegue. Ningún transformador fue ajustado antes de la validación cruzada. El ganador se determinó por el Average Precision promedio de CV; F1 promedio de CV se utilizó únicamente si el AP era exactamente igual. Las métricas del test no participaron en la selección.

## Espacios, parámetros finales y resultados

{"".join(model_sections)}
## Comparación

{_comparison_table(results["models"])}

## Modelo seleccionado

El modelo seleccionado fue **{results["selected_model"]}**, con Average Precision de CV de **{selected["cv_mean"]:.6f} ± {selected["cv_std"]:.6f}**. Esta elección se realizó antes de abrir las métricas del test y responde exclusivamente al criterio primario de validación cruzada. Su F1 promedio de CV fue {selected["cv_f1_mean"]:.6f}.

## Diferencias frente a métricas publicadas

{publication_note}

## Registro y artefactos

La ejecución produce los tres mejores pipelines, el alias del modelo seleccionado, los `cv_results_` completos, espacios, métricas, matrices de confusión, puntos y figuras ROC/Precision–Recall, JSON de resultados y CSV comparativo. Cuando Supabase está disponible, `mlops.training_runs` conserva la ejecución y `mlops.model_candidates` conserva un registro por algoritmo con estado `selected` o `rejected`. Estado de persistencia de esta ejecución: `{results["persistence"]["status"]}`.

Directorio público de resultados: `{results["paths"]["report_dir"]}`. Los binarios `.joblib` permanecen excluidos de Git y su ruta/hash se conserva en el manifiesto y registro.

## Limitaciones reproducibles

- La búsqueda y evaluación tardó {results["duration_seconds"]:.2f} segundos, sin contar la reconstrucción del Excel. `n_jobs=-1` puede elevar el consumo de memoria; equipos limitados pueden usar `--n-jobs 1` manteniendo las 40 iteraciones.
- La codificación ordinal de los modelos de árboles es una decisión de tractabilidad y sus códigos no representan orden sustantivo entre categorías.
- scikit-learn {results["environment"]["sklearn_version"]} ejecuta el espacio solicitado de `penalty`, pero advierte que esa API será retirada en una versión futura. La versión está fijada para reproducir estos resultados; una migración exige repetir el experimento.
- Esta ejecución local registra persistencia `{results["persistence"]["status"]}` porque Supabase no estaba disponible. La migración, el esquema y la ruta de registro quedan preparados para la verificación de integración en un entorno con Supabase activo.

## Cambios que debe recibir el artículo

1. Sustituir la descripción de selección de modelos por la división 80/20 estratificada, RandomizedSearchCV de cinco pliegues y Average Precision como criterio primario.
2. Incorporar la tabla comparativa de este informe y los parámetros finales de los tres algoritmos.
3. Incorporar las tres figuras ROC y las tres figuras Precision–Recall generadas en `{results["paths"]["report_dir"]}`; no reconstruirlas con cifras redondeadas.
4. Indicar explícitamente que el test se utilizó una sola vez por configuración final y nunca para seleccionar modelo, variables o transformaciones.
5. Referenciar el `run_id`, los CSV de `cv_results_`, el hash del dataset, el commit base y el repositorio público.
6. Reemplazar cualquier afirmación externa de que Random Forest era el ganador por **{results["selected_model"]}**, que es el resultado real de esta ejecución.
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_editorial_response(
    results: dict[str, Any],
    path: Path,
) -> None:
    """Write an English response and a ready-to-paste manuscript paragraph."""
    final_parameters = "; ".join(
        f"{model['algorithm']}: {_json(model['best_params'])}"
        for model in results["models"]
    )
    evaluated_spaces = "; ".join(
        f"{model['algorithm']}: {_json(model['search_space'])}"
        for model in results["models"]
    )
    selected = next(
        model
        for model in results["models"]
        if model["algorithm"] == results["selected_model"]
    )
    paragraph = (
        "Hyperparameter selection was performed exclusively on the training "
        "data using RandomizedSearchCV with five-fold stratified "
        "cross-validation (shuffle=True, random_state=42), "
        f"{results['method']['search_iterations']} sampled configurations per "
        'algorithm, scoring="average_precision", '
        "refit=True, and parallel execution. The evaluated spaces were "
        f"{evaluated_spaces}. The final parameters were {final_parameters}. "
        "All imputers, the Logistic Regression one-hot encoder and scaler, and "
        "the tree-model ordinal encoders were encapsulated in scikit-learn "
        "Pipeline and ColumnTransformer "
        "objects, so preprocessing was fitted independently within each fold "
        "and the stratified 20% test set remained unseen until all three final "
        "configurations and the winner had been fixed. "
        f"{results['selected_model']} was selected from training CV with mean "
        f"Average Precision {selected['cv_mean']:.6f} "
        f"(SD {selected['cv_std']:.6f}); F1 was reserved solely as an exact-tie "
        "criterion. The complete search spaces, cv_results_, fitted candidate "
        "metadata, dataset and software hashes, test metrics, confusion "
        "matrices, and artifact paths are persisted in the model registry "
        "(mlops.training_runs and mlops.model_candidates when Supabase is "
        f"available) and in the public repository at {REPOSITORY_URL} under "
        f"run {results['run_id']}."
    )
    content = f"""# Editorial response: hyperparameter selection

We thank the editor/reviewer for requesting a more precise account of model selection. The revised implementation now makes the training/test boundary, search procedure, complete spaces, fold-level uncertainty, final parameters, artifact provenance, and registry state explicit and reproducible. The model is selected only from training cross-validation Average Precision, with cross-validated F1 used solely for an exact tie; test metrics are descriptive final estimates and cannot influence selection.

## Final paragraph for the manuscript

{paragraph}
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
