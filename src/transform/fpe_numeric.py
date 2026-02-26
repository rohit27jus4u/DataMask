
import hmac, hashlib
class NumericFPEMasker:
    def __init__(self, secret_key: bytes, rounds: int = 10, **params):
        if not secret_key:
            raise ValueError('NumericFPEMasker requires secret_key')
        self.key = str(secret_key).encode("utf-8")
        self.rounds = rounds
    def _prf(self, data: bytes) -> int:
        d = hmac.new(self.key, data, hashlib.sha256).digest()
        return int.from_bytes(d[:4], 'big')
    def mask(self, value: str) -> str:
        if value is None:
            return None
        s = str(value)
        if not s.isdigit():
            return s
        n = len(s); mid = n//2
        L = [int(ch) for ch in s[:mid]]; R = [int(ch) for ch in s[mid:]]
        for r in range(self.rounds):
            data = bytes(L) + b'|' + bytes(R) + b'|' + bytes([r])
            f = self._prf(data); tmp = []; x = f
            for _ in range(len(R)):
                tmp.append(x % 10); x //= 10
            Rp = [(rd + fd) % 10 for rd, fd in zip(R, tmp)]
            L, R = R, Rp
        return ''.join(str(d) for d in (L+R))
