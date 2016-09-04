import datetime
import sys
import os
import socket
import logging
import time

import progressbar
from xattr import xattr

from hashedbackup.cmd_list_manifests import get_remote_manifests
from hashedbackup.fileinfo import FileInfo
from hashedbackup.manifests import ManifestWriter
from hashedbackup.backends import get_backend
from hashedbackup.utils import Timer

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
    estimate = None
    progressbar = None

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

        if options.progress:
            self.progressbar = progressbar.ProgressBar(
                redirect_stderr=True,
                redirect_stdout=True,
                widgets=[
                    progressbar.widgets.Percentage(),
                    ' | ', progressbar.widgets.SimpleProgress(),
                    ' | ', lambda *args: str(self.n_objects_added), ' new',
                    ' ', progressbar.widgets.Bar(),
                    ' ', progressbar.widgets.Timer(format='Time: %(elapsed)s'),
                    ' ', progressbar.widgets.AdaptiveETA(samples=10),
                ]
            )

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

        total = ''
        if self.estimate:
            total = '/{:6}'.format(self.estimate['total_files'])

        log_fileinfo = (
            "[%6i%s] %s%s %s  %s",
            self.n_objects_added + self.n_objects_exist + 1,
            total,
            fhash,
            '+' if not info.hash_from_cache else ' ',
            '{:12,}'.format(info.size),
            relpath
        )
        log.verbose(*log_fileinfo)

        t0 = time.time()
        if fhash in self.hashes:
            added = False
        else:
            added = self.backend.add_object(fhash, fpath)
        t1 = time.time()
        if added:
            if self.options.uploaded:
                log.info(*log_fileinfo)
            speed = '{:10,.1f} kB/s'.format(info.size / (t1 - t0) / 1024)
            log.verbose('Upload speed: %s', speed)
            self.n_objects_added += 1
            self.uploaded += info.size
        else:
            self.n_objects_exist += 1

        if self.progressbar:
            self.progressbar.update(self.n_objects_exist + self.n_objects_added)

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

    def exclude_file(self, reldir, name, *, xa=None, quiet=False):
        """Check if a file or dir needs to be excluded"""
        path = os.path.join(self.root, reldir, name)
        if name in IGNORED_ENTRIES:
            if not quiet:
                log.debug('Skipped (in IGNORED_ENTRIES): %s', path)
            return True

        if name.startswith('._'):
            if not quiet:
                log.debug('Skipped (xattr storage): %s', path)
            return True

        if not xa:
            xa = xattr(path)
        for attr in EXCLUDE_XATTR:
            try:
                xa.get(attr)
            except IOError:
                pass
            else:
                if not quiet:
                    log.verbose('Skipped (%s): %s', attr, path)
                return True

        return False

    def walk_root(self, quiet=False):
        """
        :param bool quiet: disable logging (used by prescan)
        :return: Iterable of (dirs, files) with both as path relative to
                 the root
        :rtype: iterable[tuple[str,str]]
        """
        onerror = None if quiet else self.on_walk_error
        for dirname, dirs, files in os.walk(
                self.root, onerror=onerror, followlinks=True):
            assert isinstance(dirname, str)
            assert dirname.startswith(self.root)
            reldir = dirname[len(self.root):].lstrip('/')

            reldirs = []
            relfiles = []

            for dname in dirs[:]:
                if self.exclude_file(reldir, dname, quiet=quiet):
                    # The dirs list is editable. Removing entries will prevent
                    # recursing into them. This is officially supported by
                    # os.walk().
                    # FIXME: this does not seem to work correctly
                    dirs.remove(dname)
                    continue

                relpath = os.path.join(reldir, dname)
                reldirs.append(relpath)

            for fname in files:
                if self.exclude_file(reldir, fname, quiet=quiet):
                    continue

                relpath = os.path.join(reldir, fname)
                relfiles.append(relpath)

            yield reldirs, relfiles

    @Timer("process_root")
    def process_root(self):
        for dirs, files in self.walk_root():
            for relpath in dirs:
                self.process_dir(relpath)

            for relpath in files:
                self.process_file(relpath)

    @Timer("estimate_work")
    def estimate_work(self):
        total_files = 0
        for dirs, files in self.walk_root(quiet=True):
            total_files += len(files)
        return dict(total_files=total_files)

    @Timer("run")
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

        # Skip backup if recent enough
        if self.options.if_older_than:
            manifests = get_remote_manifests(self.options)
            for name, items in manifests.items():
                assert name == self.options.namespace
                if items:
                    last_backup_age = items[-1]['age']
                    if last_backup_age < self.options.if_older_than:
                        log.info('Backup skipped, because the last backup is '
                                 'recent enough (%s < %s)',
                                 str(last_backup_age).split('.')[0],
                                 self.options.if_older_than)
                        return

        # To faster skip already uploaded objects, fetch hashes from server
        with Timer("fetch repository hashes") as timer:
            self.hashes = self.backend.get_object_hashes()
            log.info('Fetching repository hashes took %s for %i hashes',
                    timer.secs_str, len(self.hashes))

        try:
            self.open_manifest()

            if self.options.progress:
                log.info('Estimating total number of files for progress bar...')
                self.estimate = self.estimate_work()
                log.info('Estimated total number of files: %i',
                         self.estimate['total_files'])
                self.estimate = self.estimate_work()
                self.progressbar.start(self.estimate['total_files'])

            log.info('Backing up files...')
            self.process_root()

            self.close_manifest()
            if self.progressbar:
                self.progressbar.finish()

        except KeyboardInterrupt:
            log.error('INTERRUPTED - NO MANIFEST WAS WRITTEN!')

        def display(num, float=False):
            if float:
                return '{:,.1f}'.format(num)
            else:
                return '{:,}'.format(num)

        log.info('Total size (MB): %s',
            display(self.totalsize / MB, float=True))
        log.info('File hashes: %s cached, %s hashed',
            display(self.n_cached), display(self.n_updated))
        log.info('File data: %s added, %s already in repository',
                 display(self.n_objects_added), display(self.n_objects_exist))
        log.info('%s MB uploaded', display(self.uploaded / MB, float=True))
        log.info('Execution time: %ss',
            display(time.time() - self.start_time, float=True))


def backup(options):
    BackupCommand(options).run()

