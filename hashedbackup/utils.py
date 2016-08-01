import json
import sys
import os
import pwd
import grp
import urllib.parse
import uuid


def printerr(*args, **kwargs):
    kwargs['file'] = sys.stderr
    print(*args, **kwargs)

def check_destination_valid(dst):
    if not os.path.exists(os.path.join(dst, 'manifests')):
        raise FileNotFoundError(
            "Invalid backup destination (did you run `hashedbackup init`?)")

def encode_namespace(namespace):
    return urllib.parse.quote(namespace).replace('%', '=')

def json_line(data):
    return json.dumps(data, ensure_ascii=False) + '\n'

def temp_filename():
    return str(uuid.uuid1())

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

