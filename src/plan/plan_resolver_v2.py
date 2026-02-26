import os
import sys
sys.path.append(os.path.abspath('/Workspace/Users/nandish.bheemashetty@accenture.com/DataMaskingAgentversion2/src'))

from typing import Dict, Any
from config.composite_loader import ConfigLoaderV2

class PlanResolverV2:
    def __init__(self, engine_path: str, env_path: str, user_path: str):
        self.loader = ConfigLoaderV2(engine_path, env_path, user_path)
        self.cfg = self.loader.load()

    def _substitute_tokens(self, text: str, runtime_params: Dict[str, Any]) -> str:
        if text is None:
            return None
        out = text
        for k, v in runtime_params.items():
            out = out.replace(f"${{{k}}}", str(v))
        def flatten(prefix, d, acc):
            for key, val in d.items():
                if isinstance(val, dict):
                    flatten(f"{prefix}.{key}", val, acc)
                else:
                    acc[f"{prefix}.{key}"] = val
        env_flat = {}
        flatten('env.storage', self.cfg.storage, env_flat)
        flatten('env.unity_catalog', self.cfg.unity_catalog, env_flat)
        for k, v in env_flat.items():
            out = out.replace(f"${{{k}}}", str(v))
        return out

    def resolve(self, run_id: str) -> Dict[str, Any]:
        run = next((r for r in self.cfg.runs if r.get('run_id') == run_id), None)
        if not run:
            raise ValueError(f"Run '{run_id}' not found")
        overrides = run.get('env_overrides', {})
        self.cfg.storage.update(overrides.get('storage', {}))
        if 'unity_catalog' in overrides:
            uc = overrides['unity_catalog']
            self.cfg.unity_catalog['catalog'] = uc.get('catalog', self.cfg.unity_catalog.get('catalog'))
            schemas = self.cfg.unity_catalog.get('schemas', {})
            schemas.update(uc.get('schemas', {}))
            self.cfg.unity_catalog['schemas'] = schemas
        profile_id = run.get('profile_id')
        profile = next((p for p in self.cfg.profiles if p.get('profile_id') == profile_id), None)
        runtime_params = run.get('runtime_params', {})
        object_ids = run.get('objects', [])
        plans = [self._resolve_object(oid, profile, runtime_params) for oid in object_ids]
        return {'run': {'run_id': run_id, 'env_id': self.cfg.env_id, 'profile_id': profile_id, 'runtime_params': runtime_params}, 'objects': plans}

    def _resolve_object(self, object_id: str, profile: Dict[str, Any], runtime_params: Dict[str, Any]) -> Dict[str, Any]:
        obj = self.cfg.objects.get(object_id)
        prof_obj = next((o for o in profile.get('objects', []) if o.get('object_id') == object_id), None)
        part_names = prof_obj.get('partitions') if prof_obj and prof_obj.get('partitions') else obj.logical_partitions
        partitions = []
        for pn in part_names:
            pf = obj.partition_filters.get(pn)
            partitions.append({'partition_id': pn, 'filter': self._substitute_tokens(pf, runtime_params) if pf else None})
        included_cols = prof_obj.get('columns') if prof_obj and prof_obj.get('columns') not in (None, 'ALL') else [c['column'] for c in obj.pii_columns]
        overrides = prof_obj.get('overrides', {}) if prof_obj else {}
        meta = {c['column']: c for c in obj.pii_columns}
        col_plans = []
        for cn in included_cols:
            base = meta.get(cn)
            if not base:
                continue
            pt = base.get('pii_type'); kg = base.get('key_group_id')
            algo_id = overrides.get(cn, {}).get('algo_id') if overrides.get(cn) else obj.default_algorithms.get(pt)
            params_override = overrides.get(cn, {}).get('params', {}) if overrides.get(cn) else {}
            algo_spec = self.cfg.algorithms_catalog.get(algo_id)
            env_defaults = self.cfg.algorithms_defaults.get(algo_spec.impl, {}) if algo_spec else {}
            params_effective = {**(algo_spec.default_params if algo_spec else {}), **env_defaults, **params_override}
            secret_name = None
            if kg:
                kg_binding = self.cfg.key_groups_env.get(kg)
                secret_name = kg_binding.secret_name if kg_binding else None
            col_plans.append({'column': cn, 'pii_type': pt, 'algo_id': algo_id, 'algo_impl': (algo_spec.impl if algo_spec else None), 'algo_requires_secret': (algo_spec.requires_secret if algo_spec else False), 'params_effective': params_effective, 'key_group_id': kg, 'secret_name': secret_name})
        def subst_block(block):
            out = {}
            for k, v in block.items():
                out[k] = self._substitute_tokens(v, runtime_params) if isinstance(v, str) else v
            return out
        return {'object_id': object_id, 'format': obj.format, 'source': subst_block(obj.source), 'target': subst_block(obj.target), 'partitions': partitions, 'columns': col_plans, 'primary_key': obj.primary_key, 'foreign_keys': [{'fk_columns': fk.fk_columns, 'ref_object': fk.ref_object, 'ref_pk_columns': fk.ref_pk_columns} for fk in obj.foreign_keys]}
