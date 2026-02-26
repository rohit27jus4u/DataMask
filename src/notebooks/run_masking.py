import os
import sys
sys.path.append(os.path.abspath('/Workspace/Users/nandish.bheemashetty@accenture.com/DataMaskingAgentversion2'))

from pyspark.sql import SparkSession
from src.plan.plan_resolver_v2 import PlanResolverV2
from src.io.source_reader import read_source
from src.io.target_writer import write_target
from src.transform.dataframe_transformer import DataFrameTransformer
from src.validation.integrity_validator import IntegrityValidator
from src.akv.kv_client import build_kv_client, SecretProvider
import yaml
ENGINE_PATH='/Workspace/Users/nandish.bheemashetty@accenture.com/DataMaskingAgentversion2/configs/engine.json'; USER_PATH='/Workspace/Users/nandish.bheemashetty@accenture.com/DataMaskingAgentversion2/configs/app_user_configurable.json'; ENV_PATH='/Workspace/Users/nandish.bheemashetty@accenture.com/DataMaskingAgentversion2/configs/env_dev.yaml'
RUN_ID='RUN_DAILY_DEV'; DRY_RUN=False
spark = SparkSession.builder.appName('DataMasking').getOrCreate()
try: dbutils
except NameError: dbutils=None
env_cfg=yaml.safe_load(open(ENV_PATH))
# kv_client=build_kv_client(env_cfg, dbutils=dbutils)
# print(kv_client)
# allow_dummy=bool(env_cfg.get('key_vault',{}).get('allow_dummy_secret',True)); salt=env_cfg.get('key_vault',{}).get('dummy_secret_salt','')

# print(allow_dummy)
# print(salt)
# secret_provider=SecretProvider(kv_client, allow_dummy=allow_dummy, salt=salt)
# print(secret_provider)
resolver=PlanResolverV2(ENGINE_PATH, ENV_PATH, USER_PATH)
plan=resolver.resolve(RUN_ID)
transformer=DataFrameTransformer(secret_provider=None); validator=IntegrityValidator()
masked_registry={}; ri_rows=[]
for obj_plan in plan['objects']:
    fmt=obj_plan['format']; src=obj_plan['source']; tgt=obj_plan['target']
    df=read_source(spark, fmt, src)
    filters=[p.get('filter') for p in obj_plan.get('partitions',[]) if p.get('filter')]
    if filters:
        where_clause=' OR '.join(f'({f})' for f in filters); df=df.where(where_clause)
    # Minimal fix: skip non-callable udf_func
    # masked = df
    # print(obj_plan)
    
    masked=transformer.apply_masking(df, obj_plan)
    display(masked)

#     if not DRY_RUN: write_target(masked, fmt, tgt, mode='overwrite')
#     masked_registry[obj_plan['object_id']]=masked
#     pk=obj_plan.get('primary_key',[])
#     if pk:
#         res_pk=validator.validate_pk_uniqueness(masked, pk)
#         ri_rows.append({'run_id': plan['run']['run_id'], 'object_id': obj_plan['object_id'], 'check': res_pk['check'], 'status': res_pk['status'], 'total_rows': res_pk['total_rows'], 'distinct_pk_rows': res_pk['distinct_pk_rows']})
#     for fk in obj_plan.get('foreign_keys', []):
#         child_df=masked; parent_df=masked_registry.get(fk['ref_object'])
#         if parent_df is None and not DRY_RUN:
#             parent_df=spark.table(f"{tgt['catalog']}.{tgt['schema']}.{fk['ref_object'].lower()}_masked")
#         res_fk=validator.validate_no_orphan_fks(child_df, parent_df, fk_col=fk['fk_columns'][0], pk_col=fk['ref_pk_columns'][0])
#         ri_rows.append({'run_id': plan['run']['run_id'], 'object_id': obj_plan['object_id'], 'check': res_fk['check'], 'status': res_fk['status'], 'orphan_count': res_fk['orphan_count'], 'parent_object': fk['ref_object'], 'fk_column': fk['fk_columns'][0], 'pk_column': fk['ref_pk_columns'][0]})
# print('RI Validation Summary:', ri_rows)
# log_table=env_cfg.get('libraries',{}).get('log_table')
# if log_table and not DRY_RUN:
#     ri_df=spark.createDataFrame(ri_rows)
#     cat,db,tbl=log_table.split('.')
#     spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat}")
#     spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{db}")
#     spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} USING DELTA AS SELECT * FROM (SELECT CAST(NULL AS STRING) AS run_id, CAST(NULL AS STRING) AS object_id, CAST(NULL AS STRING) AS check, CAST(NULL AS STRING) AS status, CAST(NULL AS BIGINT) AS total_rows, CAST(NULL AS BIGINT) AS distinct_pk_rows, CAST(NULL AS BIGINT) AS orphan_count, CAST(NULL AS STRING) AS parent_object, CAST(NULL AS STRING) AS fk_column, CAST(NULL AS STRING) AS pk_column) WHERE 1=0")
#     ri_df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(log_table)
#     print(f"RI results appended to {log_table}")

