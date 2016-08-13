import os
import stat
import time
import logging

import paramiko
from paramiko.config import SSH_PORT

from hashedbackup.backends.base import BackendBase
from hashedbackup.utils import temp_filename, copy_and_hash_fo, MB, Timer, \
    object_bucket_dirs

log = logging.getLogger(__name__)


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

        # Will allow us to skip some remote mkdir calls
        self._existing_object_dirs = set()

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

        # Connect to server
        # Compression will not speedup picture transfers, but will help for
        # the initial remote hash download and for files that are compressible.
        # noinspection PyTypeChecker
        self.client.connect(
            self.config.get('hostname', self.hostname),
            username=self.user or self.config.get('user', None),
            password=self.password,
            port=self.config.get('port', SSH_PORT),
            sock=proxy,
            compress=True)

        transport = self.client.get_transport()
        # https://github.com/paramiko/paramiko/issues/175
        transport.window_size = 2147483647
        # 512MB -> 4GB, this is a security degradation
        transport.packetizer.REKEY_BYTES = pow(2, 32)

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

        # Checking remotely for existence just adds roundtrips
        if fhash[:2] not in self._existing_object_dirs:
            # FIXME: We assume that the only reason for failure is it already
            # exists
            self.try_mkdir(
                os.path.join(self.path, 'objects', fhash[:2]))
            self._existing_object_dirs.add(fhash[:2])

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
        """Get object hashes on server

        This executes a remote shell command to get a list of hashes, since
        recursively listing the contents of objects/ through SFTP would be
        extremely slow, because it requires three remote sync calls for up
        256 directories. This could be improved upon by hacking the SFTP
        interface for async operation (maybe someday).

        TODO: This used to be ~65k dirs, with 256 it might be faster than
              stat in situations with many files.

        If the server does not allow executing shell commands, this method
        returns an empty set and each object will be checked using one remote
        stat() call.

        :return: set of hex hashes on server
        :rtype: set[str]
        """
        # TODO: implement remote listdir
        hashes = set()
        cmd = """find '{}/objects' -type f | sed 's|.*/||'""".format(
            self.path.replace("'", r"\'"))
        log.verbose('Fetching remote file hashes using exec_command: %s', cmd)

        try:
            stdin, stdout, stderr = self.client.exec_command(cmd, bufsize=1*MB)
        except paramiko.SSHException as e:
            log.warn('Executing remote command to fetch hashes failed, '
                     'falling back to slow SFTP stat (%s)', e)
            return set()

        for line in stdout:
            line = line.strip()
            if len(line) == 32:
                hashes.add(line)
                self._existing_object_dirs.add(line[:2])
            else:
                log.debug('Invalid hash in list, skipping: %s', line)

        stdin.close()
        stdout.close()
        stderr.close()
        return hashes
