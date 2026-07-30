"""Machine-readable catalog for the quality rules that are actually executed."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class QualityRule:
    """Traceable metadata for one deterministic data-quality rule."""

    rule_id: str
    name: str
    dimension: str
    required_columns: tuple[str, ...]
    condition: str
    issue_type: str
    severity: str
    contributes_to_label: bool
    version: str
    description: str
    event_granularity: str

    def as_dict(self) -> dict[str, object]:
        """Return a stable JSON-compatible representation."""
        payload = asdict(self)
        payload["required_columns"] = list(self.required_columns)
        return payload


QUALITY_RULES: tuple[QualityRule, ...] = (
    QualityRule(
        rule_id="DQ-001",
        name="Duplicate natural key",
        dimension="uniqueness",
        required_columns=("natural_key", "row_hash"),
        condition="natural_key occurs more than once in the ingested file",
        issue_type="duplicate_natural_key",
        severity="high",
        contributes_to_label=True,
        version="1.0.0",
        description=(
            "Flags every source row whose canonical business key is duplicated "
            "within the same file."
        ),
        event_granularity="row",
    ),
    QualityRule(
        rule_id="DQ-002",
        name="Missing normalized territory",
        dimension="completeness",
        required_columns=(
            "PROVINCIA",
            "CANTON",
            "provincia_norm",
            "canton_norm",
        ),
        condition=(
            "provincia_norm or canton_norm is null or an empty string after "
            "deterministic territory matching"
        ),
        issue_type="missing_territory_norm",
        severity="high",
        contributes_to_label=True,
        version="1.0.0",
        description=(
            "Flags rows for which the province/canton normalization stage did "
            "not produce both normalized territorial values."
        ),
        event_granularity="row",
    ),
    QualityRule(
        rule_id="DQ-003",
        name="Invalid province-canton pair",
        dimension="referential_integrity",
        required_columns=("provincia_norm", "canton_norm"),
        condition=(
            "the non-missing normalized (province, canton) pair is absent from "
            "the versioned territory catalog"
        ),
        issue_type="invalid_territory_pair",
        severity="high",
        contributes_to_label=True,
        version="1.0.0",
        description=(
            "Checks normalized territorial pairs against the local reference "
            "catalog; it is not a statistical range or outlier rule."
        ),
        event_granularity="row",
    ),
    QualityRule(
        rule_id="DQ-004",
        name="Conflicting offer state",
        dimension="consistency",
        required_columns=("natural_key", "estado_norm"),
        condition=(
            "more than one distinct non-null estado_norm occurs for the same "
            "natural_key"
        ),
        issue_type="conflicting_estado",
        severity="medium",
        contributes_to_label=True,
        version="1.0.0",
        description=(
            "Emits one group-level event when records representing the same "
            "offer disagree on normalized state."
        ),
        event_granularity="group",
    ),
    QualityRule(
        rule_id="DQ-005",
        name="Missing institution name",
        dimension="completeness",
        required_columns=("NOMBRE_IES",),
        condition="NOMBRE_IES is null",
        issue_type="missing_nombre_ies",
        severity="high",
        contributes_to_label=True,
        version="1.0.0",
        description="Flags source rows with no institution name.",
        event_granularity="row",
    ),
    QualityRule(
        rule_id="DQ-006",
        name="Missing program name",
        dimension="completeness",
        required_columns=("NOMBRE_CARRERA",),
        condition="NOMBRE_CARRERA is null",
        issue_type="missing_nombre_carrera",
        severity="high",
        contributes_to_label=True,
        version="1.0.0",
        description="Flags source rows with no academic-program name.",
        event_granularity="row",
    ),
)

RULES_BY_ISSUE_TYPE = {rule.issue_type: rule for rule in QUALITY_RULES}


def rule_catalog_payload() -> list[dict[str, object]]:
    """Return the canonical catalog in its declared execution order."""
    return [rule.as_dict() for rule in QUALITY_RULES]


def label_contributing_issue_types() -> list[str]:
    """Return only issue types that contribute to the operational target."""
    return [rule.issue_type for rule in QUALITY_RULES if rule.contributes_to_label]
