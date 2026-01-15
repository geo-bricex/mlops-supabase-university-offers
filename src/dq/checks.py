import json
import logging
import uuid

import pandas as pd
from sqlalchemy import text

from src.db.session import get_db_session

logger = logging.getLogger("dq_checks")


class DataQualityChecker:
    def __init__(self, file_id: str, valid_pairs=None, extra_metrics=None):
        self.file_id = file_id
        self.run_id = str(uuid.uuid4())
        self.issues = []
        self.metrics = extra_metrics or {}
        self.valid_pairs = valid_pairs or set()

    def run_checks(self, df: pd.DataFrame):
        """
        Run all checks on the dataframe being ingested.
        df should have 'natural_key', 'provincia_norm', 'canton_norm', 'estado_norm', etc.
        """
        logger.info(f"Starting DQ checks for file {self.file_id}")

        # Check 1: Duplicates in file
        dupes = df[df.duplicated(subset=['natural_key'], keep=False)]
        if not dupes.empty:
            count = len(dupes)
            self.metrics['duplicates_in_file'] = count
            # Store detail
            for idx, row in dupes.iterrows():
                self.add_issue(
                    issue_type="duplicate_natural_key",
                    natural_key=row.get('natural_key'),
                    detail={"row_index": idx, "row_hash": row.get('row_hash')}
                )
        else:
            self.metrics['duplicates_in_file'] = 0

        # Check 2: Invalid Territory (Unmatched)
        # We assume if matching failed, we might have some indicator.
        # But if we rely on normalized cols, we can check if they are empty/null if logic allows.
        # Alternatively, the ingestion logic flags them. Here we just check if critical fields are null.
        invalid_geo_mask = (
            (df['provincia_norm'].isna()) | (df['provincia_norm'] == '') |
            (df['canton_norm'].isna()) | (df['canton_norm'] == '')
        )
        invalid_geo = df[invalid_geo_mask]
        self.metrics['invalid_territory'] = len(invalid_geo)
        for idx, row in invalid_geo.iterrows():
            self.add_issue(
                issue_type="missing_territory_norm",
                natural_key=row.get('natural_key'),
                detail={"provincia_original": row.get('PROVINCIA'), "canton_original": row.get('CANTON')}
            )

        if self.valid_pairs:
            pairs = list(zip(df['provincia_norm'], df['canton_norm']))
            invalid_pair_mask = pd.Series(
                [pair not in self.valid_pairs for pair in pairs],
                index=df.index
            )
            invalid_pairs = df[invalid_pair_mask & ~invalid_geo_mask]
            self.metrics['invalid_territory_pair'] = len(invalid_pairs)
            for idx, row in invalid_pairs.iterrows():
                self.add_issue(
                    issue_type="invalid_territory_pair",
                    natural_key=row.get('natural_key'),
                    detail={
                        "provincia_norm": row.get('provincia_norm'),
                        "canton_norm": row.get('canton_norm')
                    }
                )
        else:
            self.metrics['invalid_territory_pair'] = 0

        # Check 2b: Conflicting states within the same file for a given natural_key
        state_counts = df.groupby('natural_key')['estado_norm'].nunique(dropna=True)
        conflicting = state_counts[state_counts > 1]
        self.metrics['conflicting_estado'] = int(conflicting.shape[0])
        if not conflicting.empty:
            for nk in conflicting.index.tolist():
                states = df.loc[df['natural_key'] == nk, 'estado_norm'].dropna().unique().tolist()
                self.add_issue(
                    issue_type="conflicting_estado",
                    natural_key=nk,
                    detail={"states": states}
                )

        # Check 3: Missing Critical Fields
        required = ['NOMBRE_IES', 'NOMBRE_CARRERA']
        for col in required:
            missing = df[df[col].isna()]
            self.metrics[f'missing_{col}'] = len(missing)
            for idx, row in missing.iterrows():
                self.add_issue(
                    issue_type=f"missing_{col.lower()}",
                    natural_key=row.get('natural_key', f'row_{idx}'),
                    detail={"column": col}
                )

        # Save results
        self.save_results()

    def add_issue(self, issue_type, natural_key, detail):
        self.issues.append({
            "issue_id": str(uuid.uuid4()),
            "run_id": self.run_id,
            "issue_type": issue_type,
            "natural_key": natural_key,
            "detail": detail
        })

    def save_results(self):
        try:
            with get_db_session() as session:
                # 1. Create Run Record
                sql_run = text("""
                    INSERT INTO audit.data_quality_runs (run_id, file_id, metrics)
                    VALUES (:run_id, :file_id, :metrics)
                """)
                session.execute(sql_run, {
                    "run_id": self.run_id,
                    "file_id": self.file_id,
                    "metrics": json.dumps(self.metrics)
                })

                # 2. Insert Inconsistencies
                if self.issues:
                    sql_issue = text("""
                        INSERT INTO audit.inconsistencies (issue_id, run_id, issue_type, natural_key, detail)
                        VALUES (:issue_id, :run_id, :issue_type, :natural_key, :detail)
                    """)
                    # Bulk insert or loop
                    for issue in self.issues:
                        session.execute(sql_issue, {
                            "issue_id": issue['issue_id'],
                            "run_id": issue['run_id'],
                            "issue_type": issue['issue_type'],
                            "natural_key": issue['natural_key'],
                            "detail": json.dumps(issue['detail'])
                        })

            logger.info(f"DQ Run {self.run_id} completed. Metrics: {self.metrics}")
        except Exception as e:
            logger.error(f"Failed to save DQ results: {e}")
            raise
