
from pyspark.sql import DataFrame
from .udf_registry import get_udf_for_column
class DataFrameTransformer:
    def __init__(self, secret_provider=None):
        self.secret_provider = secret_provider
    def apply_masking(self, df: DataFrame, object_plan: dict) -> DataFrame:
        out = df
        for col_plan in object_plan.get('columns', []):
            cn = col_plan['column']
            # if cn=='annual_income':
            #     return col_plan
            # if col_plan.get('algo_impl')==None:
            #     return f'the column having None algo_impl is {cn}'
          
            if cn not in out.columns:
                continue
            
            udf_func = get_udf_for_column(col_plan, self.secret_provider)
            out = out.withColumn(cn, udf_func(cn))
        return out
