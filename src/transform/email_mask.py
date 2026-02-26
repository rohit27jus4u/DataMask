
import hashlib
class EmailMasker:
    def __init__(self, **params):
        self.preserve_domain = params.get('preserve_domain', True)
    def mask(self, value: str):
        if value is None:
            return None
        if '@' not in value:
            return 'user@example.com'
        local, domain = value.rsplit('@', 1)
        hashed = hashlib.md5(local.encode()).hexdigest()[:10]
        return f"{hashed}@{domain}" if self.preserve_domain else f"{hashed}@example.com"
