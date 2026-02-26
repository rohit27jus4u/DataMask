from pyspark.sql import DataFrame

def write_target(df: DataFrame, fmt: str, target: dict, mode: str = 'overwrite'):
    if 'path' in target and target['path']:
        return df.write.format(fmt).mode(mode).save(target['path'])
    elif 'catalog' in target and 'schema' in target and 'table' in target:
        table_fqn = f"{target['catalog']}.{target['schema']}.{target['table']}"
        df.write.format(fmt).mode(mode).option('overwriteSchema', True).saveAsTable(table_fqn)
        return None
    else:
        raise ValueError("Target must define either 'path' or 'catalog/schema/table'")
