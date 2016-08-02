import hashlib
import json
import sys
import pwd
import grp
import urllib.parse
import uuid

MB = 1024 * 1024

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

