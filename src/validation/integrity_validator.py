
from pyspark.sql import DataFrame
class IntegrityValidator:
    @staticmethod
    def validate_pk_uniqueness(df: DataFrame, pk_cols):
        total = df.count(); distinct = df.select(*pk_cols).distinct().count()
        return {'check': 'PK_UNIQUENESS', 'status': 'PASS' if total == distinct else 'FAIL', 'total_rows': total, 'distinct_pk_rows': distinct}
    @staticmethod
    def validate_no_orphan_fks(child_df: DataFrame, parent_df: DataFrame, fk_col: str, pk_col: str):
        orphan_cnt = child_df.join(parent_df.select(pk_col), child_df[fk_col] == parent_df[pk_col], 'left_anti').count()
        return {'check': 'NO_ORPHAN_FKS', 'status': 'PASS' if orphan_cnt == 0 else 'FAIL', 'orphan_count': orphan_cnt}
