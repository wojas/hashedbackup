import datetime
import os
import socket
import logging
import bz2

from hashedbackup.fileinfo import FileInfo
from hashedbackup.utils import printerr, check_destination_valid, \
    encode_namespace, json_line, temp_filename, copy_and_hash

MB = 1024 * 1024

log = logging.getLogger(__name__)


class BackupCommand:

    options = None
    root = None
    dst = None

    totalsize = 0
    n_cached = 0
    n_updated = 0
    n_objects_added = 0
    n_objects_exist = 0

    manifest_path = None
    manifest_tmp = None
    manifest = None

    def __init__(self, options):
        self.options = options

        if options.symlink and options.hardlink:
            raise ValueError('Cannot combine --symlink and --hardlink')

        self.root = os.path.abspath(options.src)
        self.dst = options.dst

    def temppath(self):
        return os.path.join(self.dst, 'tmp', temp_filename())

    def open_manifest(self):
        manifest_dir = os.path.join(
            self.dst, 'manifests', encode_namespace(self.options.namespace))

        if not os.path.exists(manifest_dir):
            log.warn('Creating new manifest namespace: %s',
                     self.options.namespace)
            os.mkdir(manifest_dir)

        dt = datetime.datetime.utcnow()
        self.manifest_path = os.path.join(
            manifest_dir, '{:%Y%m%d-%H%M%S}.manifest.bz2'.format(dt))
        self.manifest_tmp = self.temppath()
        log.debug('Manifest temp file: %s', self.manifest_tmp)
        self.manifest = bz2.open(self.manifest_tmp, 'wt', encoding='utf-8')

        self.add_to_manifest(
            version=0,
            created=dt.replace(tzinfo=datetime.timezone.utc).timestamp(),
            created_human=str(dt),
            hostname=socket.gethostname(),
            root=self.root
        )

    def add_to_manifest(self, **data):
        self.manifest.write(json_line(data))

    def close_manifest(self):
        self.add_to_manifest(
            eof=True
        )
        self.manifest.close()
        os.rename(self.manifest_tmp, self.manifest_path)
        log.info('Manifest saved to %s', self.manifest_path)

    def process_dir(self, relpath):
        dpath = os.path.join(self.root, relpath)

        try:
            info = FileInfo(dpath)
        except FileNotFoundError:
            printerr('Skipping dir (cannot stat):', relpath)
            return

        self.add_to_manifest(
            path=relpath,
            type='d',
            stat=info.stat_dict()
        )

    def object_path(self, fhash):
        return os.path.join(self.dst, 'objects', fhash[0:2], fhash[2:4], fhash)

    def add_object(self, fhash, fpath):
        objpath = self.object_path(fhash)
        if os.path.exists(objpath):
            self.n_objects_exist += 1
            return
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

        self.n_objects_added += 1

    def process_file(self, relpath):
        fpath = os.path.join(self.root, relpath)

        try:
            info = FileInfo(fpath)
        except FileNotFoundError:
            printerr('Skipping broken symlink:', relpath)
            return

        if not info.is_regular:
            printerr('Skipping non-regular file:', relpath)
            return

        self.totalsize += info.size

        # We need the hash before we do any copying, because the decision to
        # copy depends on it. Otherwise, we could have used the hash from
        # copy_and_hash.
        # If network transfer is slower than local reads and/or the OS will
        # cache the whole file, this is not an issue.
        fhash = info.filehash()
        if info.hash_from_cache:
            self.n_cached += 1
        else:
            self.n_updated += 1

        printerr(
            self.root, relpath,
            info.mode,
            info.st.st_mtime,
            info.size,
            fhash, '*' if not info.hash_from_cache else '')

        self.add_object(fhash, fpath)

        self.add_to_manifest(
            path=relpath,
            type='f',
            size=info.size,
            hash=fhash,
            stat=info.stat_dict()
        )

    def walk_root(self):
        for dirname, dirs, files in os.walk(self.root):
            assert isinstance(dirname, str)
            assert dirname.startswith(self.root)
            reldir = dirname[len(self.root):].lstrip('/')

            for dname in dirs:
                relpath = os.path.join(reldir, dname)
                self.process_dir(relpath)

            for fname in files:
                relpath = os.path.join(reldir, fname)
                self.process_file(relpath)

    def run(self):
        check_destination_valid(self.dst)

        try:
            self.open_manifest()
            self.walk_root()
            self.close_manifest()
        except KeyboardInterrupt:
            printerr('INTERRUPTED')

        printerr('Total size (MB):', self.totalsize / MB)
        printerr(self.n_cached, 'cached,', self.n_updated, 'hashed')
        printerr(self.n_objects_added, 'added,',
                 self.n_objects_exist, 'already in repository')


def backup(options):
    BackupCommand(options).run()

