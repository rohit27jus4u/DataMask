
class NumericNoiseMasker:
    def __init__(self, **params):
        self.noise_percent = float(params.get('noise_percent', 10))
    def mask(self, value):
        if value is None:
            return None
        try:
            v = float(value)
        except Exception:
            return None
        return round(v * (1 + self.noise_percent/100.0), 2)
