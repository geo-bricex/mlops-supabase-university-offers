"""Canonical configuration for reproducible model selection."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, OrdinalEncoder, StandardScaler

from src.ml.quality_risk import PRIMARY_SCENARIO, scenario_definition

RANDOM_STATE = 42
TEST_SIZE = 0.20
CV_FOLDS = 5
SEARCH_ITERATIONS = 40
OPTIMIZATION_METRIC = "average_precision"
SEARCH_METHOD = "RandomizedSearchCV"
PRIMARY_ENCODING = "onehot"
REFERENCE_THRESHOLD = 0.5
THRESHOLD_OBJECTIVE = "F2"
MODEL_NAMES = (
    "LogisticRegression",
    "GradientBoostingClassifier",
    "RandomForestClassifier",
)

ParameterSpace = Mapping[str, Sequence[Any]] | list[Mapping[str, Sequence[Any]]]


@dataclass(frozen=True)
class ModelSearchSpec:
    """Estimator pipeline and valid parameter space for one algorithm."""

    name: str
    pipeline: Pipeline
    parameter_space: ParameterSpace
    scenario: str
    categorical_encoding: str


LOGISTIC_REGRESSION_SPACE: list[dict[str, Sequence[Any]]] = [
    {
        "model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0],
        "model__penalty": ["l1"],
        "model__class_weight": [None, "balanced"],
        "model__solver": ["saga"],
        "model__l1_ratio": [1.0],
        "model__max_iter": [500, 1000, 2000],
    },
    {
        "model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0],
        "model__penalty": ["l2"],
        "model__class_weight": [None, "balanced"],
        "model__solver": ["lbfgs", "liblinear", "saga"],
        "model__l1_ratio": [0.0],
        "model__max_iter": [500, 1000, 2000],
    },
    {
        "model__C": [0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 100.0],
        "model__penalty": ["elasticnet"],
        "model__class_weight": [None, "balanced"],
        "model__solver": ["saga"],
        "model__l1_ratio": [0.1, 0.25, 0.5, 0.75, 0.9],
        "model__max_iter": [500, 1000, 2000],
    },
]

GRADIENT_BOOSTING_SPACE: dict[str, Sequence[Any]] = {
    "model__n_estimators": [50, 100, 150, 200, 300],
    "model__learning_rate": [0.01, 0.03, 0.05, 0.1, 0.2],
    "model__max_depth": [1, 2, 3, 4],
    "model__subsample": [0.6, 0.75, 0.9, 1.0],
    "model__min_samples_split": [2, 5, 10, 20],
    "model__min_samples_leaf": [1, 2, 5, 10],
}

RANDOM_FOREST_SPACE: dict[str, Sequence[Any]] = {
    "model__n_estimators": [100, 200, 300, 500],
    "model__max_depth": [None, 5, 10, 20, 30],
    "model__max_features": ["sqrt", "log2", 0.5, None],
    "model__min_samples_split": [2, 5, 10, 20],
    "model__min_samples_leaf": [1, 2, 4, 8],
    "model__class_weight": [None, "balanced", "balanced_subsample"],
}


def build_cv() -> StratifiedGroupKFold:
    """Return the single cross-validation definition used by all searches."""
    return StratifiedGroupKFold(
        n_splits=CV_FOLDS,
        shuffle=True,
        random_state=RANDOM_STATE,
    )


def build_preprocessor(
    *,
    categorical_features: Sequence[str],
    numeric_features: Sequence[str],
    scale_numeric: bool,
    encoding_strategy: str,
) -> ColumnTransformer:
    """Build an unfitted preprocessing graph for mixed input data."""
    numeric_steps = [("imputer", SimpleImputer(strategy="median"))]
    if scale_numeric:
        numeric_steps.append(("scaler", StandardScaler()))
    if encoding_strategy == "onehot":
        categorical_encoder = OneHotEncoder(
            handle_unknown="ignore",
            sparse_output=True,
        )
    elif encoding_strategy == "ordinal":
        categorical_encoder = OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=-1,
        )
    else:
        raise ValueError("encoding_strategy must be either 'onehot' or 'ordinal'.")

    transformers = []
    if categorical_features:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", categorical_encoder),
                    ]
                ),
                list(categorical_features),
            )
        )
    if numeric_features:
        transformers.append(
            (
                "numeric",
                Pipeline(steps=numeric_steps),
                list(numeric_features),
            )
        )
    return ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        sparse_threshold=1.0 if encoding_strategy == "onehot" else 0.0,
    )


def build_model_specs(
    estimator_n_jobs: int = -1,
    *,
    scenario: str = PRIMARY_SCENARIO,
    encoding_strategy: str = PRIMARY_ENCODING,
) -> list[ModelSearchSpec]:
    """Create the three required, unfitted estimator pipelines."""
    definition = scenario_definition(scenario)
    categorical_features = definition["categorical_features"]
    numeric_features = definition["numeric_features"]
    return [
        ModelSearchSpec(
            name="LogisticRegression",
            pipeline=Pipeline(
                steps=[
                    (
                        "preprocess",
                        build_preprocessor(
                            categorical_features=categorical_features,
                            numeric_features=numeric_features,
                            scale_numeric=True,
                            encoding_strategy=encoding_strategy,
                        ),
                    ),
                    (
                        "model",
                        LogisticRegression(
                            random_state=RANDOM_STATE,
                            tol=1e-3,
                        ),
                    ),
                ]
            ),
            parameter_space=LOGISTIC_REGRESSION_SPACE,
            scenario=scenario,
            categorical_encoding=encoding_strategy,
        ),
        ModelSearchSpec(
            name="GradientBoostingClassifier",
            pipeline=Pipeline(
                steps=[
                    (
                        "preprocess",
                        build_preprocessor(
                            categorical_features=categorical_features,
                            numeric_features=numeric_features,
                            scale_numeric=False,
                            encoding_strategy=encoding_strategy,
                        ),
                    ),
                    (
                        "model",
                        GradientBoostingClassifier(
                            random_state=RANDOM_STATE,
                        ),
                    ),
                ]
            ),
            parameter_space=GRADIENT_BOOSTING_SPACE,
            scenario=scenario,
            categorical_encoding=encoding_strategy,
        ),
        ModelSearchSpec(
            name="RandomForestClassifier",
            pipeline=Pipeline(
                steps=[
                    (
                        "preprocess",
                        build_preprocessor(
                            categorical_features=categorical_features,
                            numeric_features=numeric_features,
                            scale_numeric=False,
                            encoding_strategy=encoding_strategy,
                        ),
                    ),
                    (
                        "model",
                        RandomForestClassifier(
                            random_state=RANDOM_STATE,
                            n_jobs=estimator_n_jobs,
                        ),
                    ),
                ]
            ),
            parameter_space=RANDOM_FOREST_SPACE,
            scenario=scenario,
            categorical_encoding=encoding_strategy,
        ),
    ]


def search_spaces_for_json() -> dict[str, ParameterSpace]:
    """Return the complete discrete spaces in JSON-compatible form."""
    return {
        "LogisticRegression": LOGISTIC_REGRESSION_SPACE,
        "GradientBoostingClassifier": GRADIENT_BOOSTING_SPACE,
        "RandomForestClassifier": RANDOM_FOREST_SPACE,
    }
