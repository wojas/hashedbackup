import datetime
import logging
import os
from bz2 import BZ2Compressor

from hashedbackup.utils import encode_namespace, json_line

log = logging.getLogger(__name__)


class ManifestWriter:
    """File wrapper that writes to a temporary file and then atomically moves
    it in place once done.
    """
    file = None
    compressor = None

    def __init__(self, backend, namespace):
        """
        :type backend: hashedbackup.backends.base.BackendBase
        :param str namespace: manifest namespace
        """
        manifest_dir = os.path.join(
            backend.path, 'manifests', encode_namespace(namespace))
        backend.try_mkdir(manifest_dir)

        self.dt = datetime.datetime.utcnow()
        self.manifest_path = os.path.join(
            manifest_dir, '{:%Y%m%d-%H%M%S}.manifest.bz2'.format(self.dt))

        self.tmp_path = backend.temppath()
        log.debug('Manifest temp file: %s', self.tmp_path)
        self.file = backend.open(self.tmp_path, 'wb')
        self.compressor = BZ2Compressor(9)
        self.backend = backend

    def write(self, buf):
        self.file.write(self.compressor.compress(buf))

    def add(self, **data):
        self.write(json_line(data).encode('utf-8'))

    def commit(self):
        flush_data = self.compressor.flush()
        if flush_data:
            self.file.write(flush_data)
        self.file.close()
        self.backend.rename(self.tmp_path, self.manifest_path)

    def cancel(self):
        self.file.close()
        self.backend.delete(self.tmp_path)


