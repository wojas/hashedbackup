import datetime
import sys
import os
import socket
import logging
import time

from xattr import xattr

from hashedbackup.fileinfo import FileInfo
from hashedbackup.manifests import ManifestWriter
from hashedbackup.backends import get_backend

MB = 1024 * 1024

# These are system files/dirs that are unsafe or useless to backup
IGNORED_ENTRIES = {'.DS_Store', '.Trashes' '.fseventsd', '.Spotlight-V100'}
EXCLUDE_XATTR = [
    'com.apple.metadata:com_apple_backup_excludeItem',
    'nl.wojas.hashedbackup.exclude',
]

log = logging.getLogger(__name__)


class BackupCommand:

    options = None
    root = None
    dst = None
    sftp = False

    totalsize = 0
    n_cached = 0
    n_updated = 0
    n_objects_added = 0
    n_objects_exist = 0
    uploaded = 0

    manifest_path = None
    manifest_tmp = None
    manifest = None

    def __init__(self, options):
        self.options = options
        self.start_time = time.time()

        if options.symlink and options.hardlink:
            raise ValueError('Cannot combine --symlink and --hardlink')

        self.root = os.path.abspath(options.src)
        self.dst = options.dst
        self.hashes = set()

        self.backend = get_backend(self.dst, options)
        log.debug('Storage backend is %s', self.backend.__class__.__name__)

    def open_manifest(self):
        self.manifest = ManifestWriter(self.backend, self.options.namespace)
        # TODO: move to ManifestWriter?
        self.manifest.add(
            version=0,
            created=self.manifest.dt.replace(
                tzinfo=datetime.timezone.utc).timestamp(),
            created_human=str(self.manifest.dt),
            hostname=socket.gethostname(),
            root=self.root
        )

    def close_manifest(self):
        # TODO: move to ManifestWriter?
        self.manifest.add(
            eof=True
        )
        self.manifest.commit()
        log.verbose('Manifest saved to %s', self.manifest.manifest_path)

    def process_dir(self, relpath):
        dpath = os.path.join(self.root, relpath)

        try:
            info = FileInfo(dpath)
        except FileNotFoundError:
            log.warn('Skipping dir (cannot stat): %s', relpath)
            return

        self.manifest.add(
            path=relpath,
            type='d',
            stat=info.stat_dict()
        )

    def process_file(self, relpath):
        fpath = os.path.join(self.root, relpath)

        try:
            info = FileInfo(fpath)
        except FileNotFoundError:
            log.warn('Skipping broken symlink: %s', relpath)
            return

        if not info.is_regular:
            log.warn('Skipping non-regular file: %s', relpath)
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

        log.verbose("[%6i] %s%s %s  %s",
            self.n_objects_added + self.n_objects_exist + 1,
            fhash,
            '+' if not info.hash_from_cache else ' ',
            '{:12,}'.format(info.size),
            relpath)

        t0 = time.time()
        if fhash in self.hashes:
            added = False
        else:
            added = self.backend.add_object(fhash, fpath)
        t1 = time.time()
        if added:
            speed = '{:10,.1f} kB/s'.format(info.size / (t1 - t0) / 1024)
            log.verbose('Upload speed: %s', speed)
            self.n_objects_added += 1
            self.uploaded += info.size
        else:
            self.n_objects_exist += 1

        self.manifest.add(
            path=relpath,
            type='f',
            size=info.size,
            hash=fhash,
            stat=info.stat_dict()
        )

    def on_walk_error(self, exc):
        assert isinstance(exc, OSError)
        log.warn('Could not list directory, skipping: %s', exc.filename)

    def exclude_file(self, reldir, name, *, xa=None):
        """Check if a file or dir needs to be excluded"""
        path = os.path.join(self.root, reldir, name)
        if name in IGNORED_ENTRIES:
            log.verbose('Skipped (in IGNORED_ENTRIES): %s', path)
            return True

        if name.startswith('._'):
            log.verbose('Skipped (xattr storage): %s', path)
            return True

        if not xa:
            xa = xattr(path)
        for attr in EXCLUDE_XATTR:
            try:
                xa.get(attr)
            except IOError:
                pass
            else:
                log.verbose('Skipped (%s): %s', attr, path)
                return True

        return False

    def walk_root(self):
        for dirname, dirs, files in os.walk(
                    self.root, onerror=self.on_walk_error, followlinks=True):
            assert isinstance(dirname, str)
            assert dirname.startswith(self.root)
            reldir = dirname[len(self.root):].lstrip('/')

            for dname in dirs[:]:
                if self.exclude_file(reldir, dname):
                    # The dirs list is editable. Removing entries will prevent
                    # recursing into them. This is officially supported by
                    # os.walk().
                    dirs.remove(dname)
                    continue

                relpath = os.path.join(reldir, dname)
                self.process_dir(relpath)

            for fname in files:
                if self.exclude_file(reldir, fname):
                    continue

                relpath = os.path.join(reldir, fname)
                self.process_file(relpath)

    def run(self):
        self.backend.check_destination_valid()

        if not os.path.exists(self.root):
            log.error('Location to backup does not exist: %s', self.root)
            sys.exit(1)
        if not os.path.isdir(self.root):
            log.error('Location to backup is not a directory: %s', self.root)
            sys.exit(1)
        if not os.listdir(self.root):
            log.error('Location to backup is empty: %s', self.root)
            sys.exit(1)

        # To faster skip already uploaded objects, fetch hashes from server
        t0 = time.time()
        self.hashes = self.backend.get_object_hashes()
        t1 = time.time()
        log.verbose('Fetching repository hashes took %.1fs for %i hashes',
                    t1 - t0, len(self.hashes))

        try:
            self.open_manifest()
            self.walk_root()
            self.close_manifest()
        except KeyboardInterrupt:
            log.error('INTERRUPTED - NO MANIFEST WAS WRITTEN!')

        log.info('Total size (MB): %.1f', self.totalsize / MB)
        log.info('%i cached, %i hashed', self.n_cached, self.n_updated)
        log.info('%i added, %i already in repository',
                 self.n_objects_added, self.n_objects_exist)
        log.info('%.1f MB uploaded', self.uploaded / MB)
        log.info('Execution time: %.1fs', time.time() - self.start_time)


def backup(options):
    BackupCommand(options).run()

