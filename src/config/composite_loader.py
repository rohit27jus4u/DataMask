
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import json, yaml

@dataclass
class AlgorithmSpec:
    algo_id: str
    impl: str
    requires_secret: bool
    reversible: bool
    default_params: Dict[str, Any] = field(default_factory=dict)

@dataclass
class KeyGroupEnvBinding:
    key_group_id: str
    secret_name: str

@dataclass
class ObjectFK:
    fk_columns: List[str]
    ref_object: str
    ref_pk_columns: List[str]

@dataclass
class ObjectDef:
    object_id: str
    system: str
    zone: str
    format: str
    source: Dict[str, Any]
    target: Dict[str, Any]
    primary_key: List[str]
    foreign_keys: List[ObjectFK]
    logical_partitions: List[str]
    partition_filters: Dict[str, Optional[str]]
    pii_columns: List[Dict[str, Any]]
    default_algorithms: Dict[str, str]

@dataclass
class CompositeRuntimeConfig:
    env_id: str
    application: str
    variables: Dict[str, Any]
    storage: Dict[str, Any]
    unity_catalog: Dict[str, Any]
    libraries: Dict[str, Any]
    algorithms_defaults: Dict[str, Any]
    key_groups_env: Dict[str, KeyGroupEnvBinding]
    key_vault: Dict[str, Any]
    algorithms_catalog: Dict[str, AlgorithmSpec]
    objects: Dict[str, ObjectDef]
    profiles: List[Dict[str, Any]]
    runs: List[Dict[str, Any]]

class ConfigLoaderV2:
    def __init__(self, engine_path: str, env_path: str, user_path: str):
        self.engine_path = engine_path
        self.env_path = env_path
        self.user_path = user_path

    def load(self) -> CompositeRuntimeConfig:
        engine = self._load_engine(self.engine_path)
        env = self._load_env(self.env_path)
        user = self._load_user(self.user_path)

        algo_catalog = {
            a["algo_id"]: AlgorithmSpec(
                algo_id=a["algo_id"], impl=a["impl"],
                requires_secret=a.get("requires_secret", False),
                reversible=a.get("reversible", False),
                default_params=a.get("default_params", {})
            ) for a in engine.get("algorithms", [])
        }
        kge = {kg_id: KeyGroupEnvBinding(key_group_id=kg_id, secret_name=kg["secret_name"]) for kg_id, kg in env.get("key_groups_env", {}).items()}

        objects = {}
        for obj in user.get("masking_master", {}).get("objects", []):
            fks = [ObjectFK(fk_columns=fk.get("fk_columns", []), ref_object=fk.get("ref_object"), ref_pk_columns=fk.get("ref_pk_columns", [])) for fk in obj.get("foreign_keys", [])]
            objects[obj["object_id"]] = ObjectDef(
                object_id=obj["object_id"], system=obj.get("system"), zone=obj.get("zone"), format=obj.get("format"),
                source=obj.get("source", {}), target=obj.get("target", {}), primary_key=obj.get("primary_key", []),
                foreign_keys=fks, logical_partitions=obj.get("logical_partitions", []), partition_filters=obj.get("partition_filters", {}),
                pii_columns=obj.get("pii_columns", []), default_algorithms=obj.get("default_algorithms", {})
            )

        crc = CompositeRuntimeConfig(
            env_id=env.get("env_id"), application=env.get("application"), variables=env.get("variables", {}),
            storage=env.get("storage", {}), unity_catalog=env.get("unity_catalog", {}), libraries=env.get("libraries", {}),
            algorithms_defaults=env.get("algorithms_defaults", {}), key_groups_env=kge, key_vault=env.get("key_vault", {}),
            algorithms_catalog=algo_catalog, objects=objects, profiles=user.get("profiles", []), runs=user.get("runs", [])
        )
        self._validate(crc)
        return crc

    def _load_engine(self, path: str) -> Dict[str, Any]:
        with open(path) as f: return json.load(f)
    def _load_env(self, path: str) -> Dict[str, Any]:
        with open(path) as f: return yaml.safe_load(f)
    def _load_user(self, path: str) -> Dict[str, Any]:
        with open(path) as f: return json.load(f)

    def _validate(self, cfg: CompositeRuntimeConfig):
        for o in cfg.objects.values():
            if not o.primary_key:
                raise ValueError(f"[{o.object_id}] primary_key must not be empty")
            for fk in o.foreign_keys:
                if fk.ref_object not in cfg.objects:
                    raise ValueError(f"[{o.object_id}] FK ref_object '{fk.ref_object}' not found")
                ref_obj = cfg.objects[fk.ref_object]
                for pk in fk.ref_pk_columns:
                    if pk not in ref_obj.primary_key:
                        raise ValueError(f"[{o.object_id}] FK references non-PK '{pk}' on '{fk.ref_object}'")
        for o in cfg.objects.values():
            for p in o.logical_partitions:
                if p not in o.partition_filters:
                    raise ValueError(f"[{o.object_id}] Missing partition_filter for logical_partition '{p}'")
        valid_algos = set(cfg.algorithms_catalog.keys())
        for o in cfg.objects.values():
            for col in o.pii_columns:
                pt = col.get("pii_type")
                if pt is None: continue
                algo_id = o.default_algorithms.get(pt)
                if not algo_id:
                    raise ValueError(f"[{o.object_id}] No default algorithm for pii_type '{pt}'")
                if algo_id not in valid_algos:
                    raise ValueError(f"[{o.object_id}] Default algorithm '{algo_id}' not found in engine catalog")
        kg_ids = set(cfg.key_groups_env.keys())
        for o in cfg.objects.values():
            for col in o.pii_columns:
                kg = col.get("key_group_id")
                if kg and kg not in kg_ids:
                    raise ValueError(f"[{o.object_id}] key_group_id '{kg}' missing in env key_groups_env")
        for o in cfg.objects.values():
            for k in ("source", "target"):
                block = o.__dict__[k]
                if not block or ("table" not in block and "path" not in block):
                    raise ValueError(f"[{o.object_id}] Missing {k} definition (need table or path)")
