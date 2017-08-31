#coding=utf-8

"""
@varsion: ??
@author: 张帅男
@file: tbktredis.py
@time: 2017/8/31 17:04
"""

"""

存储格式:
为了通用, KEY也是字符串而且不能有空格, VALUE数据约定用JSON格式.

用法:
rd.henan.user.set(630445, 'hello')
print rd.henan.user.get(630445)
"""

import datetime
import simplejson as json
from simplejson.encoder import JSONEncoder
import redis

class Struct(dict):
    """
    - 为字典加上点语法. 例如:
    >>> o = Struct({'a':1})
    >>> o.a
    >>> 1
    >>> o.b
    >>> None
    """
    def __init__(self, *e, **f):
        if e:
            self.update(e[0])
        if f:
            self.update(f)

    def __getattr__(self, name):
        # Pickle is trying to get state from your object, and dict doesn't implement it.
        # Your __getattr__ is being called with "__getstate__" to find that magic method,
        # and returning None instead of raising AttributeError as it should.
        if name.startswith('__'):
            raise AttributeError
        return self.get(name)

    def __setattr__(self, name, val):
        self[name] = val

    def __delattr__(self, name):
        self.pop(name, None)

    def __hash__(self):
        return id(self)

class XJSONEncoder(JSONEncoder):
    """
    JSON扩展: 支持datetime和date类型
    """
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, datetime.date):
            return o.strftime('%Y-%m-%d')
        else:
            return JSONEncoder.default(self, o)


def encodev(o):
    return json.dumps(o, cls=XJSONEncoder, encoding='utf-8')

def decode(data):
    if isinstance(data, dict):
        return Struct(data)
    elif isinstance(data, (list, tuple)):
        return [decode(d) for d in data]
    return data

def decodev(o):
    if not isinstance(o, basestring):
        return o
    data = json.loads(o)
    return decode(data)

class Store:
    def __init__(self, rd, prefix=""):
        self.rd = rd
        self.prefix = prefix

    def get(self, name):
        v = self.rd.get(name)
        try:
            return decodev(v)
        except json.JSONDecodeError:
            return v

    def __getattr__(self, name):
        print 11111111
        if self.prefix:
            name = self.prefix + '_' + name
        return StoreProxy(self, name)

class StoreProxy:
    """
    存进去的值都强制传为json格式.
    """
    def __init__(self, store, prefix):
        self.rd = store.rd
        self.prefix = prefix

    def make_key(self, arg):
        if isinstance(arg, (tuple, list)):
            arg = '_'.join(str(a) for a in arg)
        if arg is None:
            return self.prefix
        return '%s_%s' % (self.prefix, arg)

    def get(self, arg=None):
        if not self.rd:
            return

        key = self.make_key(arg)
        v = self.rd.get(key)
        return decodev(v)

    def set(self, arg, value, timeout=None):
        if not self.rd:
            return False

        key = self.make_key(arg)
        value = encodev(value)
        return self.rd.set(key, value, ex=timeout)

    def get_many(self, args):
        if not self.rd:
            return {}

        keys = [self.make_key(arg) for arg in args]
        results = self.rd.mget(keys)
        results = [decodev(v) for v in results]
        return dict(zip(args, results))

    def delete(self, *args):
        if not self.rd:
            return 0

        if not args:
            args = [self.make_key(None)]
        else:
            args = [self.make_key(a) for a in args]
        return self.rd.delete(*args)

    def sadd(self, arg, *values):
        if not self.rd:
            return 0

        key = self.make_key(arg)
        return self.rd.sadd(key, *values)

    def spop(self, arg=None):
        if not self.rd:
            return

        key = self.make_key(arg)
        return self.rd.spop(key)

class StoreProxy2:
    """
    存进去的值都强制传为json格式.
    """
    def __init__(self, rd, prefix):
        self.rd = rd
        self.prefix = prefix

    def make_key(self, arg):
        if isinstance(arg, (tuple, list)):
            arg = '_'.join(str(a) for a in arg)
        if arg is None:
            return self.prefix
        return '%s_%s' % (self.prefix, arg)

    def get(self, arg=None):
        if not self.rd:
            return

        key = self.make_key(arg)
        v = self.rd.get(key)
        return decodev(v)

    def set(self, arg, value, timeout=None):
        if not self.rd:
            return False

        key = self.make_key(arg)
        value = encodev(value)
        return self.rd.set(key, value, ex=timeout)

    def get_many(self, args):
        if not self.rd:
            return {}

        keys = [self.make_key(arg) for arg in args]
        results = self.rd.mget(keys)
        results = [decodev(v) for v in results]
        return dict(zip(args, results))

    def delete(self, *args):
        if not self.rd:
            return 0

        if not args:
            args = [self.make_key(None)]
        else:
            args = [self.make_key(a) for a in args]
        return self.rd.delete(*args)

    def sadd(self, arg, *values):
        if not self.rd:
            return 0

        key = self.make_key(arg)
        return self.rd.sadd(key, *values)

    def spop(self, arg=None):
        if not self.rd:
            return

        key = self.make_key(arg)
        return self.rd.spop(key)


REDIS_HOST = "192.168.7.250"
REDIS_PORT = "6379"
REDIS_DB = "1"
REDIS_PASSWORD = "jxtbkt2013!"

POOL = redis.ConnectionPool(host=REDIS_HOST,
                            port=REDIS_PORT,
                            db=REDIS_DB,
                            password=REDIS_PASSWORD,
                )

class Slice:
    def __init__(self):
        self.rd = redis.Redis(connection_pool=POOL)

    def __getattr__(self, prefix):
        return Store(self.rd, prefix)

    def __getitem__(self, prefix):
        return Store(self.rd, prefix)


rd = Slice()
