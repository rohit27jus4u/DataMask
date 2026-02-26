
import random
FIRST_NAMES = ["James","Mary","Robert","Patricia","Michael","Jennifer","William","Linda","David","Barbara","Richard","Susan"]
LAST_NAMES = ["Smith","Johnson","Brown","Davis","Miller","Wilson","Moore","Taylor","Anderson","Thomas","Jackson","White"]
class FakeNameMasker:
    def __init__(self, **params):
        self.gender = params.get('gender','random')
    def mask(self, value: str):
        if value is None:
            return None
        return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
