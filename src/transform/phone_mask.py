
import random, re
class PhoneMasker:
    def __init__(self, **params):
        self.preserve_country_code = params.get('preserve_country_code', True)
        self.preserve_length = params.get('preserve_length', True)
    def mask(self, value: str):
        if value is None:
            return None
        s = str(value)
        digits = [c for c in s if c.isdigit()]
        if not digits:
            return s
        country_code = ''
        remaining_digits = digits[:]
        if self.preserve_country_code and s.startswith('+'):
            m = re.match(r"\+(\d{1,3})", s)
            if m:
                cc_len = len(m.group(1))
                country_code = m.group(1)
                remaining_digits = remaining_digits[cc_len:]
        masked_digits = [str(random.randint(0,9)) for _ in remaining_digits]
        out = []
        digit_iter = iter(([d for d in country_code] + masked_digits) if country_code else masked_digits)
        for ch in s:
            if ch.isdigit():
                out.append(next(digit_iter))
            else:
                out.append(ch)
        res = ''.join(out)
        if self.preserve_length and len(res) != len(s):
            res = res[:len(s)]
        return res
