
#!/usr/bin/env python3
import sys, json, yaml, argparse
ERRORS=[]

def e(msg):
    ERRORS.append(msg); print(f"[ERROR] {msg}")

def validate_engine(engine):
    algos = engine.get('algorithms', []); ids = {a.get('algo_id') for a in algos}
    if not ids: e('engine.algorithms is empty')
    for a in algos:
        if not a.get('impl'): e(f"Algorithm {a.get('algo_id')} missing impl")

def validate_env(env):
    for k in ['env_id','storage','unity_catalog','libraries']:
        if k not in env: e(f"env missing '{k}'")
    for kg_id,kg in env.get('key_groups_env',{}).items():
        if not kg.get('secret_name'): e(f"key_groups_env.{kg_id} missing secret_name")

def validate_user(user, engine):
    algo_ids={a['algo_id'] for a in engine.get('algorithms',[])}
    objects=user.get('masking_master',{}).get('objects',[])
    if not objects: e('user.masking_master.objects empty')
    obj_index={o['object_id']:o for o in objects}
    for o in objects:
        if not o.get('source') or not (o['source'].get('table') or o['source'].get('path')): e(f"[{o['object_id']}] source must define table or path")
        if not o.get('target') or not (o['target'].get('table') or o['target'].get('path')): e(f"[{o['object_id']}] target must define table or path")
        if not o.get('primary_key'): e(f"[{o['object_id']}] primary_key empty")
        for fk in o.get('foreign_keys', []):
            ref=fk.get('ref_object')
            if ref not in obj_index: e(f"[{o['object_id']}] FK ref_object '{ref}' not found in user objects")
            else:
                ref_pk=obj_index[ref].get('primary_key',[])
                for pk in fk.get('ref_pk_columns',[]):
                    if pk not in ref_pk: e(f"[{o['object_id']}] FK refs non-PK '{pk}' on '{ref}'")
        lp=o.get('logical_partitions',[]); pf=o.get('partition_filters',{})
        for p in lp:
            if p not in pf: e(f"[{o['object_id']}] Missing partition_filter for logical_partition '{p}'")
        defaults=o.get('default_algorithms',{})
        for col in o.get('pii_columns',[]):
            pt=col.get('pii_type')
            if pt:
                algo=defaults.get(pt)
                if not algo: e(f"[{o['object_id']}] No default algorithm for pii_type '{pt}'")
                elif algo not in algo_ids: e(f"[{o['object_id']}] Default algorithm '{algo}' not found in engine catalog")

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--engine',required=True); ap.add_argument('--env',required=True); ap.add_argument('--user',required=True); args=ap.parse_args()
    with open(args.engine) as f: engine=json.load(f)
    with open(args.env) as f: env=yaml.safe_load(f)
    with open(args.user) as f: user=json.load(f)
    validate_engine(engine); validate_env(env); validate_user(user, engine)
    if ERRORS:
        print(f"
Validation completed with {len(ERRORS)} error(s).")
        sys.exit(1))
    else:
        print('Validation OK'); sys.exit(0)
if __name__=='__main__': main()
