
from pyspark.sql import SparkSession

def read_source(spark: SparkSession, fmt: str, source: dict):
    if 'path' in source and source['path']:
        return spark.read.format(fmt).load(source['path'])
    elif 'catalog' in source and 'schema' in source and 'table' in source:
        table_fqn = f"{source['catalog']}.{source['schema']}.{source['table']}"
        return spark.table(table_fqn)
    else:
        raise ValueError("Source must define either 'path' or 'catalog/schema/table'")
