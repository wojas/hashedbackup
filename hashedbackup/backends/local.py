import os
import logging

from hashedbackup.backends.base import BackendBase
from hashedbackup.utils import copy_and_hash, object_bucket_dirs

log = logging.getLogger(__name__)


class LocalBackend(BackendBase):

    def try_mkdir(self, path):
        try:
            os.mkdir(path)
            return True
        except OSError:
            return False

    def exists(self, path):
        log.debug('exists(%r)', path)
        return os.path.exists(path)

    def open(self, *args, **kwargs):
        return open(*args, **kwargs)

    def rename(self, src, dst):
        log.debug('rename(%r, %r)', src, dst)
        os.rename(src, dst)

    def delete(self, path):
        log.debug('delete(%r)', path)
        os.unlink(path)

    def add_object(self, fhash, fpath):
        log.debug('add_object(%r, %r)', fhash, fpath)
        objpath = self.object_path(fhash)
        if os.path.exists(objpath):
            return False
        os.makedirs(os.path.dirname(objpath), exist_ok=True)

        if self.options.symlink:
            os.symlink(fpath, objpath)
        elif self.options.hardlink:
            os.link(fpath, objpath)
        else:
            tmp = self.temppath()
            tmphash = copy_and_hash(fpath, tmp)
            if tmphash != fhash:
                # TODO: can we recover by retrying process_file() ?
                os.unlink(tmp)
                raise ValueError(
                    'File {} hash does not match after copy!'.format(fpath))
            os.rename(tmp, objpath)

        return True

    def listdir(self, path):
        return os.listdir(path)

    def isdir(self, path):
        return os.path.isdir(path)

    def get_object_hashes(self):
        hashes = set()
        for bucket in object_bucket_dirs():
            bucket_path = os.path.join(self.path, 'objects', bucket)
            for fname in os.listdir(bucket_path):
                if fname.startswith('.'):
                    continue
                if len(fname) == 32:
                    hashes.add(fname)
                else:
                    log.debug('Invalid hash in list, skipping: %s', fname)
        return hashes
