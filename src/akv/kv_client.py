
import os, hashlib
class DummyKVClient:
    def get_secret(self, name: str):
        class S: pass
        s = S(); s.value = ''
        return s

def build_kv_client(env_cfg: dict, dbutils=None):
    kv = env_cfg.get('key_vault', {})
    vault_url = kv.get('vault_url')
    creds = kv.get('credentials', {})
    if not vault_url:
        return None
    try:
        from azure.identity import ClientSecretCredential
        from azure.keyvault.secrets import SecretClient
        tenant_id = None; client_id = None; client_secret = None
        if dbutils and creds:
            tenant_id = dbutils.secrets.get(scope=creds.get('tenant_id_secret_scope'), key=creds.get('tenant_id_secret_key'))
            client_id = dbutils.secrets.get(scope=creds.get('client_id_secret_scope'), key=creds.get('client_id_secret_key'))
            client_secret = dbutils.secrets.get(scope=creds.get('client_secret_secret_scope'), key=creds.get('client_secret_secret_key'))
        tenant_id = tenant_id or os.getenv('AZURE_TENANT_ID')
        client_id = client_id or os.getenv('AZURE_CLIENT_ID')
        client_secret = client_secret or os.getenv('AZURE_CLIENT_SECRET')
        if not (tenant_id and client_id and client_secret):
            return DummyKVClient()
        credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        return SecretClient(vault_url=vault_url, credential=credential)
    except Exception:
        return DummyKVClient()

class SecretProvider:
    def __init__(self, kv_client, allow_dummy: bool = True, salt: str = ''):
        self.kv_client = kv_client
        self.allow_dummy = allow_dummy
        self.salt = salt or ''
    def resolve(self, secret_name: str) -> bytes:
        try:
            if self.kv_client:
                val = self.kv_client.get_secret(secret_name).value
                if val:
                    return val.encode()
        except Exception:
            pass
        if not self.allow_dummy:
            raise ValueError(f"Secret '{secret_name}' not found and dummy fallback disabled")
        return hashlib.sha256((secret_name + self.salt).encode()).digest()
