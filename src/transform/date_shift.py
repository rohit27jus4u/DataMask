
import hashlib
from datetime import datetime, timedelta
class DateShiftMasker:
    def __init__(self, secret_key: bytes = None, **params):
        self.secret_key = secret_key
        self.shift_days_max = params.get('shift_days_max', 365)
    def mask(self, value: str):
        if value is None:
            return None
        try:
            date_obj = datetime.strptime(str(value), '%Y-%m-%d').date()
        except Exception:
            return value
        if self.secret_key:
            shift = int(hashlib.md5(self.secret_key + str(value).encode()).hexdigest(), 16) % self.shift_days_max
        else:
            shift = 100
        shifted = date_obj + timedelta(days=shift)
        return shifted.strftime('%Y-%m-%d')
