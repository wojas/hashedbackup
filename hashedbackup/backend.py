import datetime
import os
import logging
import abc
import stat
import time
from bz2 import BZ2Compressor

import paramiko
from paramiko.config import SSH_PORT

from hashedbackup.utils import copy_and_hash_fo, temp_filename, copy_and_hash, \
    encode_namespace, json_line

log = logging.getLogger(__name__)


class ManifestWriter:
    """File wrapper that writes to a temporary file and then atomically moves
    it in place once done.
    """
    file = None
    compressor = None

    def __init__(self, backend, namespace):
        """
        :type backend: BackendBase
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


class BackendBase(abc.ABC):

    def __init__(self, path, options):
        self.path = path
        self.options = options

    @abc.abstractmethod
    def try_mkdir(self, path): pass

    @abc.abstractmethod
    def exists(self, path): pass

    @abc.abstractmethod
    def open(self, *args, **kwargs): pass

    @abc.abstractmethod
    def rename(self, src, dst): pass

    @abc.abstractmethod
    def delete(self, path): pass

    @abc.abstractmethod
    def add_object(self, fhash, fpath): pass

    @abc.abstractmethod
    def listdir(self, path): pass

    @abc.abstractmethod
    def isdir(self, path): pass

    @abc.abstractmethod
    def get_object_hashes(self): pass

    def temppath(self):
        return os.path.join(self.path, 'tmp', temp_filename())

    def object_path(self, fhash):
        return os.path.join(self.path, 'objects', fhash[0:2], fhash[2:4], fhash)

    def check_destination_valid(self):
        if not self.exists(os.path.join(self.path, 'manifests')):
            raise FileNotFoundError(
                "Invalid backup destination (did you run `hashedbackup init`?)")


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


class SFTPBackend(BackendBase):

    sftp = None
    last_actual_transfer_time = None

    def __init__(self, remote_path, options):
        """
        :param str remote_path: like what `scp` accepts:
            user@host:/foo
            hostalias:backup/pictures
        :param str password: password or passphrase
        """
        self.options = options
        self.password = None # TODO: implement in options
        self.hostname, self.path = remote_path.split(':', 1)
        self.user = None
        if '@' in self.hostname:
            self.user, self.hostname = self.hostname.split('@')
        super().__init__(self.path, options)

        ssh_config = paramiko.SSHConfig()
        try:
            ssh_config.parse(open(os.path.expanduser('~/.ssh/config')))
        except FileNotFoundError:
            pass

        self.config = ssh_config.lookup(self.hostname)
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self._connect()

    def _connect(self):
        if 'proxycommand' in self.config:
            proxy = paramiko.ProxyCommand(self.config['proxycommand'])
            # TODO: check this code, needed?
            #subprocess.check_output(
            #    [os.environ['SHELL'], '-c',
            #        'echo %s' % self.config['proxycommand']]
            #).strip()
        else:
            proxy = None

        # noinspection PyTypeChecker
        self.client.connect(
            self.config.get('hostname', self.hostname),
            username=self.user or self.config.get('user', None),
            password=self.password,
            port=self.config.get('port', SSH_PORT),
            sock=proxy)

        self.sftp = self.client.open_sftp()

    def rename(self, src, dst):
        self.sftp.rename(src, dst)

    def delete(self, path):
        self.sftp.unlink(path)

    def open(self, *args, **kwargs):
        f = self.sftp.open(*args, **kwargs)
        f.set_pipelined(True)
        return f

    def exists(self, path):
        try:
            self.sftp.stat(path)
            return True
        except OSError:
            return False

    def try_mkdir(self, path):
        try:
            self.sftp.mkdir(path)
            return True
        except OSError:
            return False

    def add_object(self, fhash, fpath):
        dst_path = self.object_path(fhash)
        if self.exists(dst_path):
            return False

        size = os.path.getsize(fpath)

        # Checking for existence just adds roundtrips
        self.try_mkdir(
            os.path.join(self.path, 'objects', fhash[0:2]))
        self.try_mkdir(
            os.path.join(self.path, 'objects', fhash[0:2], fhash[2:4]))

        tmp = os.path.join(self.path, 'tmp', temp_filename())

        t0 = time.time()
        with open(fpath, 'rb') as src:
            with self.open(tmp, 'wb') as dst:
                tmphash = copy_and_hash_fo(src, dst)
        t1 = time.time()
        self.last_actual_transfer_time = t1 - t0

        if tmphash != fhash:
            # TODO: can we recover by retrying process_file() ?
            self.sftp.unlink(tmp)
            raise ValueError(
                'File {} hash does not match after copy!'.format(fpath))

        self.sftp.rename(tmp, dst_path)

        # Confirm remote size
        s = self.sftp.stat(dst_path)
        if s.st_size != size:
            raise IOError('size mismatch in put!  %d != %d' % (s.st_size, size))

        return True

    def listdir(self, path):
        return self.sftp.listdir(path)

    def isdir(self, path):
        return stat.S_ISDIR(self.sftp.stat(path).st_mode)

    def get_object_hashes(self):
        objects_root = os.path.join(self.path, 'objects')
        hashes = set()

        for dir1 in self.listdir(objects_root):
            if dir1.startswith('.') or len(dir1) != 2:
                continue

            for dir2 in self.listdir(os.path.join(objects_root, dir1)):
                if dir2.startswith('.') or len(dir2) != 2:
                    continue

                for h in self.listdir(os.path.join(objects_root, dir1, dir2)):
                    if len(h) != 32:
                        continue
                    hashes.add(h)

        return hashes


def get_backend(path, options):
    """
    :param str path: path or url as passed by user
    :type options: dict
    :rtype: BackendBase
    """
    if ':' in path:
        return SFTPBackend(path, options=options)
    else:
        return LocalBackend(path, options=options)

