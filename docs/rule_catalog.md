# Executed data-quality rule catalog

Run ID: `8c464366-c5ab-433a-abb0-380bad37683a`

This catalog is generated from the same Python metadata used by the audit engine. It lists only implemented rules. Audit-event counts are not mutually exclusive: one row or `natural_key` may violate multiple rules. ETL rows loaded/skipped, normalization diagnostics, audit events, directly affected rows, and group-propagated positive labels are distinct quantities.

## Run counts

| ID | Issue type | Dimension | Severity | Events | Affected rows | Affected groups | Label-positive rows |
|---|---|---|---|---:|---:|---:|---:|
| DQ-001 | duplicate_natural_key | uniqueness | high | 3706 | 3706 | 1840 | 3706 |
| DQ-002 | missing_territory_norm | completeness | high | 1176 | 1176 | 1050 | 1176 |
| DQ-003 | invalid_territory_pair | referential_integrity | high | 0 | 0 | 0 | 0 |
| DQ-004 | conflicting_estado | consistency | medium | 1801 | 3617 | 1801 | 3617 |
| DQ-005 | missing_nombre_ies | completeness | high | 1 | 1 | 1 | 1 |
| DQ-006 | missing_nombre_carrera | completeness | high | 0 | 0 | 0 | 0 |

## Rule definitions

### DQ-001: Duplicate natural key

- Required columns: `["natural_key", "row_hash"]`
- Condition: natural_key occurs more than once in the ingested file
- Event type/granularity: `duplicate_natural_key` / `row`
- Severity: `high`
- Contributes to `actual_label`: `True`
- Version: `1.0.0`
- Description: Flags every source row whose canonical business key is duplicated within the same file.
### DQ-002: Missing normalized territory

- Required columns: `["PROVINCIA", "CANTON", "provincia_norm", "canton_norm"]`
- Condition: provincia_norm or canton_norm is null or an empty string after deterministic territory matching
- Event type/granularity: `missing_territory_norm` / `row`
- Severity: `high`
- Contributes to `actual_label`: `True`
- Version: `1.0.0`
- Description: Flags rows for which the province/canton normalization stage did not produce both normalized territorial values.
### DQ-003: Invalid province-canton pair

- Required columns: `["provincia_norm", "canton_norm"]`
- Condition: the non-missing normalized (province, canton) pair is absent from the versioned territory catalog
- Event type/granularity: `invalid_territory_pair` / `row`
- Severity: `high`
- Contributes to `actual_label`: `True`
- Version: `1.0.0`
- Description: Checks normalized territorial pairs against the local reference catalog; it is not a statistical range or outlier rule.
### DQ-004: Conflicting offer state

- Required columns: `["natural_key", "estado_norm"]`
- Condition: more than one distinct non-null estado_norm occurs for the same natural_key
- Event type/granularity: `conflicting_estado` / `group`
- Severity: `medium`
- Contributes to `actual_label`: `True`
- Version: `1.0.0`
- Description: Emits one group-level event when records representing the same offer disagree on normalized state.
### DQ-005: Missing institution name

- Required columns: `["NOMBRE_IES"]`
- Condition: NOMBRE_IES is null
- Event type/granularity: `missing_nombre_ies` / `row`
- Severity: `high`
- Contributes to `actual_label`: `True`
- Version: `1.0.0`
- Description: Flags source rows with no institution name.
### DQ-006: Missing program name

- Required columns: `["NOMBRE_CARRERA"]`
- Condition: NOMBRE_CARRERA is null
- Event type/granularity: `missing_nombre_carrera` / `row`
- Severity: `high`
- Contributes to `actual_label`: `True`
- Version: `1.0.0`
- Description: Flags source rows with no academic-program name.
