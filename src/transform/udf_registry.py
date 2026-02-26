import os
import sys

import requests

sys.path.append(os.path.abspath('/Workspace/Users/nandish.bheemashetty@accenture.com/DataMaskingAgentversion2/src/transform'))

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType
from transform.hmac_sha256 import HmacSha256Masker
from transform.email_mask import EmailMasker
from transform.fake_name import FakeNameMasker
from transform.date_shift import DateShiftMasker
from transform.numeric_noise import NumericNoiseMasker
from transform.fpe_numeric import NumericFPEMasker
from transform.phone_mask import PhoneMasker

ALGO_IMPL_MAP = {
    'HMAC_SHA256': HmacSha256Masker,
    'EMAIL_MASK': EmailMasker,
    'FAKE_NAME': FakeNameMasker,
    'DATE_SHIFT': DateShiftMasker,
    'NUMERIC_NOISE': NumericNoiseMasker,
    'FPE': NumericFPEMasker,
    'PHONE_MASK': PhoneMasker,
}


# class HttpSecretsProvider:
#     def __init__(self, base_url):
#         self.base_url=base_url.rstrip("/")

#     def get_secret(self,scope,key):
#         url=f"{self.base_url}/secret/{scope}/{key}"
#         r=requests.get(url,timeout=5)
#         r.raise_for_status()
#         return r.json()["value"]

def get_udf_for_column(col_plan: dict, secret_provider=None):
    impl = col_plan.get('algo_impl')
    params = col_plan.get('params_effective', {})
    # secret_name = None
    requires_secret = col_plan.get('algo_requires_secret', False)
    if impl not in ALGO_IMPL_MAP:
        raise ValueError(f"Unknown algorithm implementation: {impl}")

    if requires_secret:
        global secret_name
        secret_name=col_plan.get('secret_name')
        if secret_name is None:
            secret_name=params.get('secret_name') 

    if secret_name is None:
        print(f"Secret name not found for {impl}")

    # secrets=HttpSecretsProvider("https://davina-pedunculate-welly.ngrok-free.dev/")
    # secret_key=secrets.get_secret('secret_keys',secret_name)

    secret_bytes = None
    if requires_secret:
        if not secret_name:
            raise ValueError(f"Secret required for {impl} but no secret_name provided")
        # if not secret_provider:
        #     raise ValueError(f"Secret required for {impl} but no SecretProvider provided")
        # secret_bytes = secret_provider(secret_name)


    cls = ALGO_IMPL_MAP[impl]
    return_type = DoubleType() if impl == 'NUMERIC_NOISE' else StringType()
    masker = cls(secret_name, **params) if requires_secret else cls(**params)

    @udf(return_type)
    def mask_udf(value):
        return masker.mask(value)
        # try:
        #     return masker.mask(value)
        # except Exception as e:
        #     return f"Error in {impl}"
    return mask_udf
