# Informe de selección de hiperparámetros

## Identificación

- `run_id`: `e70cde76-34e4-4a88-ac1f-9c5cdf3e7673`
- Fecha UTC: `2026-07-30T13:23:25.809303+00:00`
- Hash lógico del dataset: `ff6be902980c3c588c04cfdcf4fafe43637d1d649ed1c43aeb7f37f798830d1d`
- Hash del archivo fuente: `fe366924ce44b577c74f72282b042ca7908aedf59445db00893b9a3b2d58848f`
- Registros: 20045
- Distribución de clases: `{"0": 15392, "1": 4653}`
- Python: `3.14.5`
- scikit-learn: `1.9.0`
- Commit base de Git: `5843d019df9f694930314dc2c1fb3e3cf49b77d7`; árbol sucio durante la ejecución: `True`

## Metodología

Se reservó mediante muestreo estratificado el 20 % de las observaciones como conjunto de prueba y se utilizó el 80 % restante para entrenamiento. El test permaneció aislado durante toda la selección. Cada algoritmo se optimizó exclusivamente en entrenamiento mediante `RandomizedSearchCV`, con 40 combinaciones, `StratifiedKFold(n_splits=5, shuffle=True, random_state=42)`, `scoring="average_precision"`, `refit=True` y `n_jobs=-1`.

La imputación, la codificación one-hot para Logistic Regression, la codificación ordinal para los modelos de árboles y el escalamiento exclusivo de Logistic Regression se ajustaron dentro de `Pipeline` y `ColumnTransformer` en cada pliegue. Ningún transformador fue ajustado antes de la validación cruzada. El ganador se determinó por el Average Precision promedio de CV; F1 promedio de CV se utilizó únicamente si el AP era exactamente igual. Las métricas del test no participaron en la selección.

## Espacios, parámetros finales y resultados

### LogisticRegression

- Espacio de búsqueda: `[{"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__max_iter": [500, 1000, 2000], "model__penalty": ["l1"], "model__solver": ["saga"]}, {"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__max_iter": [500, 1000, 2000], "model__penalty": ["l2"], "model__solver": ["lbfgs", "liblinear", "saga"]}, {"model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0], "model__class_weight": [null, "balanced"], "model__l1_ratio": [0.1, 0.25, 0.5, 0.75, 0.9], "model__max_iter": [500, 1000, 2000], "model__penalty": ["elasticnet"], "model__solver": ["saga"]}]`
- Parámetros finales: `{"model__C": 3.0, "model__class_weight": null, "model__max_iter": 1000, "model__penalty": "l2", "model__solver": "liblinear"}`
- Mejor Average Precision de CV: 0.846745
- Desviación estándar de CV: 0.012465
- F1 promedio de CV (solo desempate): 0.743507
- Test: accuracy=0.886256, precision=0.785114, recall=0.702470, F1=0.741497, ROC AUC=0.940536, Average Precision=0.846498.
- Matriz de confusión `[[TN, FP], [FN, TP]]`: `[[2899, 179], [277, 654]]`
### GradientBoostingClassifier

- Espacio de búsqueda: `{"model__learning_rate": [0.01, 0.03, 0.05, 0.1, 0.2], "model__max_depth": [1, 2, 3, 4], "model__min_samples_leaf": [1, 2, 5, 10], "model__min_samples_split": [2, 5, 10, 20], "model__n_estimators": [50, 100, 150, 200, 300], "model__subsample": [0.6, 0.75, 0.9, 1.0]}`
- Parámetros finales: `{"model__learning_rate": 0.2, "model__max_depth": 4, "model__min_samples_leaf": 10, "model__min_samples_split": 20, "model__n_estimators": 300, "model__subsample": 0.6}`
- Mejor Average Precision de CV: 0.916566
- Desviación estándar de CV: 0.004211
- F1 promedio de CV (solo desempate): 0.823544
- Test: accuracy=0.919681, precision=0.848000, recall=0.796992, F1=0.821705, ROC AUC=0.969153, Average Precision=0.916629.
- Matriz de confusión `[[TN, FP], [FN, TP]]`: `[[2945, 133], [189, 742]]`
### RandomForestClassifier

- Espacio de búsqueda: `{"model__class_weight": [null, "balanced", "balanced_subsample"], "model__max_depth": [null, 5, 10, 20, 30], "model__max_features": ["sqrt", "log2", 0.5, null], "model__min_samples_leaf": [1, 2, 4, 8], "model__min_samples_split": [2, 5, 10, 20], "model__n_estimators": [100, 200, 300, 500]}`
- Parámetros finales: `{"model__class_weight": "balanced", "model__max_depth": 30, "model__max_features": "log2", "model__min_samples_leaf": 1, "model__min_samples_split": 10, "model__n_estimators": 300}`
- Mejor Average Precision de CV: 0.914684
- Desviación estándar de CV: 0.005417
- F1 promedio de CV (solo desempate): 0.813988
- Test: accuracy=0.904465, precision=0.739930, recall=0.907626, F1=0.815244, ROC AUC=0.969446, Average Precision=0.917905.
- Matriz de confusión `[[TN, FP], [FN, TP]]`: `[[2781, 297], [86, 845]]`

## Comparación

| Algorithm | CV AP mean | CV AP SD | CV F1 mean | Test AP | Test F1 | Status |
|---|---:|---:|---:|---:|---:|---|
| LogisticRegression | 0.846745 | 0.012465 | 0.743507 | 0.846498 | 0.741497 | rejected |
| GradientBoostingClassifier | 0.916566 | 0.004211 | 0.823544 | 0.916629 | 0.821705 | selected |
| RandomForestClassifier | 0.914684 | 0.005417 | 0.813988 | 0.917905 | 0.815244 | rejected |

## Modelo seleccionado

El modelo seleccionado fue **GradientBoostingClassifier**, con Average Precision de CV de **0.916566 ± 0.004211**. Esta elección se realizó antes de abrir las métricas del test y responde exclusivamente al criterio primario de validación cruzada. Su F1 promedio de CV fue 0.823544.

## Diferencias frente a métricas publicadas

La versión anterior a esta revisión de `reports/article.md` no publicaba métricas ni parámetros de esta comparación ML; por ello no existía una cifra anterior verificable contra la cual calcular una diferencia numérica. La versión revisada incorpora estos primeros resultados trazables. Cualquier afirmación externa de que Random Forest era el ganador debe reemplazarse por Gradient Boosting.

## Registro y artefactos

La ejecución produce los tres mejores pipelines, el alias del modelo seleccionado, los `cv_results_` completos, espacios, métricas, matrices de confusión, puntos y figuras ROC/Precision–Recall, JSON de resultados y CSV comparativo. Cuando Supabase está disponible, `mlops.training_runs` conserva la ejecución y `mlops.model_candidates` conserva un registro por algoritmo con estado `selected` o `rejected`. Estado de persistencia de esta ejecución: `not_requested`.

Directorio público de resultados: `reports/modeling/e70cde76-34e4-4a88-ac1f-9c5cdf3e7673`. Los binarios `.joblib` permanecen excluidos de Git y su ruta/hash se conserva en el manifiesto y registro.

## Limitaciones reproducibles

- La búsqueda y evaluación tardó 665.04 segundos, sin contar la reconstrucción del Excel. `n_jobs=-1` puede elevar el consumo de memoria; equipos limitados pueden usar `--n-jobs 1` manteniendo las 40 iteraciones.
- La codificación ordinal de los modelos de árboles es una decisión de tractabilidad y sus códigos no representan orden sustantivo entre categorías.
- scikit-learn 1.9.0 ejecuta el espacio solicitado de `penalty`, pero advierte que esa API será retirada en una versión futura. La versión está fijada para reproducir estos resultados; una migración exige repetir el experimento.
- Esta ejecución local registra persistencia `not_requested` porque Supabase no estaba disponible. La migración, el esquema y la ruta de registro quedan preparados para la verificación de integración en un entorno con Supabase activo.

## Cambios que debe recibir el artículo

1. Sustituir la descripción de selección de modelos por la división 80/20 estratificada, RandomizedSearchCV de cinco pliegues y Average Precision como criterio primario.
2. Incorporar la tabla comparativa de este informe y los parámetros finales de los tres algoritmos.
3. Incorporar las tres figuras ROC y las tres figuras Precision–Recall generadas en `reports/modeling/e70cde76-34e4-4a88-ac1f-9c5cdf3e7673`; no reconstruirlas con cifras redondeadas.
4. Indicar explícitamente que el test se utilizó una sola vez por configuración final y nunca para seleccionar modelo, variables o transformaciones.
5. Referenciar el `run_id`, los CSV de `cv_results_`, el hash del dataset, el commit base y el repositorio público.
6. Reemplazar cualquier afirmación externa de que Random Forest era el ganador por **GradientBoostingClassifier**, que es el resultado real de esta ejecución.
