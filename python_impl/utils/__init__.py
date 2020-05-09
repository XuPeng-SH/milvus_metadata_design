from functools import wraps

def singleton(cls):
    instances = {}
    @wraps(cls)
    def getinstance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return getinstance

LSN = 0
def get_lsn():
    global LSN
    LSN += 1
    return LSN

def set_lsn(lsn):
    global LSN
    LSN = lsn


class ReferenceContext:
    def __init__(self, caller, *tags):
        self.caller = caller
        self.default_tags = list(*tags)
        self.tags = []

    def log(self):
        logs =  f'{self.caller}'
        tags = self.default_tags + self.tags
        for tag in tags:
            logs += f'-{tag}'
        self.clear_tags()
        return logs

    def add_tag(self, *tag):
        self.tags.extend(tag)
        return self

    def clear_tags(self):
        self.tags[:] = []

    def clone(self):
        c = ReferenceContext(self.caller, *self.default_tags)
        c.add_tag(*self.tags)
        return c
