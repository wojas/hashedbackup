import os
import hashlib
import stat
import json
import logging

from xattr import xattr

from hashedbackup.utils import lookup_user, lookup_group

log = logging.getLogger(__name__)

MB = 1024 * 1024
TO_NANO = 1000000000
ATTR = 'nl.wojas.hashedbackup'


class FileInfo:

    def __init__(self, fpath):
        self.fpath = fpath
        # can raise exceptions
        self.st = os.stat(fpath)
        self.xattr = xattr(fpath)
        self._hash = None
        self.hash_from_cache = None

    @property
    def is_regular(self):
        return stat.S_ISREG(self.st.st_mode)

    @property
    def size(self):
        return self.st.st_size

    @property
    def mode(self):
        return int(oct(stat.S_IMODE(self.st.st_mode))[2:]) # strip '0o'

    def _load_xattr(self):
        try:
            cached = json.loads(self.xattr.get(ATTR).decode('ascii'))
        except IOError:
            return None
        else:
            mtime_ns = cached['mt'] * TO_NANO + cached['mtns']
            size = cached['size']
            if size == self.size and self.st.st_mtime_ns == mtime_ns:
                return cached['md5']
        return None

    def _save_xattr(self, fhash):
        new_cached = dict(
            mt=self.st.st_mtime_ns // TO_NANO,
            mtns=self.st.st_mtime_ns % TO_NANO,
            md5=fhash,
            size=self.size
        )
        try:
            self.xattr.set(ATTR, json.dumps(new_cached).encode('ascii'))
        except IOError:
            log.warn('Could not write xattr to %s', self.fpath)

    def _calc_filehash(self, bufsize=1*MB):
        h = hashlib.md5()
        with open(self.fpath, 'rb') as f:
            buf = f.read(bufsize)
            while buf:
                h.update(buf)
                buf = f.read(bufsize)
        return str(h.hexdigest())

    def filehash(self):
        if self._hash:
            return self._hash

        # Try from xattr
        self._hash = self._load_xattr()
        if self._hash:
            self.hash_from_cache = True
            return self._hash

        # Hash file and save to xattr
        self._hash = self._calc_filehash()
        self._save_xattr(self._hash)
        self.hash_from_cache = False
        return self._hash

    def stat_dict(self):
        st = self.st
        return dict(
            mode=self.mode,
            uid=st.st_uid,
            gid=st.st_gid,
            user=lookup_user.get(st.st_uid),
            group=lookup_group.get(st.st_gid),
            # atime and ctime are not very useful
            mtime=st.st_mtime_ns // TO_NANO,
            mtime_ns=st.st_mtime_ns % TO_NANO,
        )

