import datetime
import functools
import hashlib
import json
import sys
import pwd
import grp
import urllib.parse
import uuid
import logging
import time


MB = 1024 * 1024

log = logging.getLogger(__name__)


class Timer:
    """Used for timing the performance of code

    Plain usage:

        timer = Timer("foo")
        do_something()
        print("This took", timer.msecs_str)

    Usage in a context:

        with Timer("foo") as timer:
            do_something()
            print("This took", timer.msecs_str)

    Usage as a decorator:

        @Timer("do_something timer")
        def do_something():
            ...
    """

    def __init__(self, name="untitled"):
        self.t0 = time.time()
        self.name = name

    def reset(self):
        self.t0 = time.time()

    @property
    def secs(self):
        return time.time() - self.t0

    @property
    def secs_str(self):
        return "{:,.1f} s".format(self.secs)

    @property
    def msecs(self):
        return 1000 * (time.time() - self.t0)

    @property
    def msecs_str(self):
        return "{:,.1f} ms".format(self.msecs)

    def log_secs(self, label):
        log.debug('Timer %s: %s :: %s', self.name, self.secs_str, label)

    def log_msecs(self, label):
        log.debug('Timer %s: %s :: %s', self.name, self.msecs_str, label)

    # 'with' context manager

    def __enter__(self):
        self.reset()
        return self

    def __exit__(self, *args):
        log.debug('Timer %s: exited context after %s',
                  self.name, self.secs_str)

    # function decorator

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper


def parse_age(s):
    """
    :param str s: like "7d", "4h", "15m" or "30s"
    :rtype: datetime.timedelta
    :raises ValueError: if wrong format
    """
    if not s:
        raise ValueError("Empty string")

    try:
        int(s)
    except ValueError:
        pass
    else:
        raise ValueError("No unit specified behind number (d/h/m/s)")

    unit = s[-1].lower()
    try:
        num = int(s[:-1])
    except ValueError:
        raise

    if unit == 's':
        return datetime.timedelta(seconds=num)
    elif unit == 'm':
        return datetime.timedelta(seconds=num * 60)
    elif unit == 'h':
        return datetime.timedelta(seconds=num * 3600)
    elif unit == 'd':
        return datetime.timedelta(days=num)
    else:
        raise ValueError("Invalid age unit: {}".format(unit))


def object_bucket_dirs():
    """
    :return: iterable of '00'...'ff'
    """
    for i in range(256):
        yield '{:02x}'.format(i)

def printerr(*args, **kwargs):
    kwargs['file'] = sys.stderr
    print(*args, **kwargs)

def encode_namespace(namespace):
    return urllib.parse.quote(namespace).replace('%', '=')

def decode_namespace(encoded_namespace):
    return urllib.parse.unquote(encoded_namespace.replace('=', '%'))

def json_line(data):
    return json.dumps(data, ensure_ascii=False) + '\n'

def temp_filename():
    return str(uuid.uuid1())

def filehash(fpath, *, bufsize=1*MB):
    h = hashlib.md5()
    with open(fpath, 'rb') as f:
        buf = f.read(bufsize)
        while buf:
            h.update(buf)
            buf = f.read(bufsize)
    return str(h.hexdigest())

def copy_and_hash_fo(src, dst, *, bufsize=1*MB, progress=None):
    copied = 0
    h = hashlib.md5()
    buf = src.read(bufsize)
    while buf:
        h.update(buf)
        dst.write(buf)
        if progress:
            copied += len(buf)
            progress(copied)
        buf = src.read(bufsize)
    return str(h.hexdigest())

def copy_and_hash(src_path, dst_path, *, bufsize=1*MB, progress=None):
    with open(src_path, 'rb') as src:
        with open(dst_path, 'wb') as dst:
            return copy_and_hash_fo(
                src, dst, bufsize=bufsize, progress=progress)

class CachedUserLookup:
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def get(self, key):
        try:
            return self.cache[key]
        except KeyError:
            try:
                val = self.func(key)[0]
            except KeyError:
                val = None
            self.cache[key] = val
            return val

lookup_user = CachedUserLookup(pwd.getpwuid)
lookup_group = CachedUserLookup(grp.getgrgid)

