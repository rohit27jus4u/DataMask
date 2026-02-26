
import hashlib, hmac
class HmacSha256Masker:
    def __init__(self, secret_key: bytes = None, **params):
        self.secret_key = secret_key
        self.length = params.get('output_length', 32)
        self.format = params.get('output_format', 'hex')
    def mask(self, value: str):
        if value is None:
            return None
        if not self.secret_key:
            raise ValueError('HMAC_SHA256 requires secret_key')

        if isinstance(self.secret_key, int):
            secret_key_bytes = str(self.secret_key).encode("utf-8")
        elif isinstance(self.secret_key, str):
            secret_key_bytes = self.secret_key.encode("utf-8")

        if isinstance(value, int):
            value_bytes = str(value).encode("utf-8")
        elif isinstance(value, str):
            value_bytes = value.encode("utf-8")
        else:
            value_bytes = bytes(value)

        digest = hmac.new(secret_key_bytes, value_bytes, hashlib.sha256).digest()
        if self.format == 'numeric':
            num = int.from_bytes(digest[:8], 'big') % (10 ** self.length)
            return str(num).zfill(self.length)
        else:
            return digest.hex()[:self.length]
