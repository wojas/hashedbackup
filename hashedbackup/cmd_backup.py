import os
import time
import socket
import logging
import bz2

from hashedbackup.fileinfo import FileInfo
from hashedbackup.utils import printerr, check_destination_valid, \
    encode_namespace, json_line, temp_filename

MB = 1024 * 1024

log = logging.getLogger(__name__)


def backup(options):
    root = os.path.abspath(options.src)

    dst = options.dst
    check_destination_valid(dst)
    manifest_dir = os.path.join(
        dst, 'manifests', encode_namespace(options.namespace))
    if not os.path.exists(manifest_dir):
        log.warn('Creating new manifest namespace: %s', options.namespace)
        os.mkdir(manifest_dir)

    manifest_ts = int(time.time())
    manifest_path = os.path.join(
        manifest_dir, '{}.manifest.bz2'.format(manifest_ts))
    manifest_tmp = os.path.join(dst, 'tmp', temp_filename())
    log.debug('Manifest temp file: %s', manifest_tmp)
    manifest = bz2.open(manifest_tmp, 'wt', encoding='utf-8')

    manifest.write(json_line(dict(
        version=0,
        created=time.time(),
        hostname=socket.gethostname(),
        root=root
    )))

    totalsize = 0
    n_cached = 0
    n_updated = 0
    try:
        for dirname, dirs, files in os.walk(root):
            assert dirname.startswith(root)
            reldir = dirname[len(root):].lstrip('/')

            # TODO: records dirs to recreate empty ones and with proper perms

            for dname in dirs:
                relpath = os.path.join(reldir, dname)
                dpath = os.path.join(root, relpath)

                try:
                    info = FileInfo(dpath)
                except FileNotFoundError:
                    printerr('Skipping dir (cannot stat):', relpath)
                    continue

                print(json.dumps(dict(
                    path=relpath,
                    type='d',
                    stat=info.stat_dict()
                )))

            for fname in files:
                relpath = os.path.join(reldir, fname)
                fpath = os.path.join(root, relpath)

                try:
                    info = FileInfo(fpath)
                except FileNotFoundError:
                    printerr('Skipping broken symlink:', relpath)
                    continue

                if not info.is_regular:
                    printerr('Skipping non-regular file:', relpath)
                    continue

                totalsize += info.size

                fhash = info.filehash()
                if info.hash_from_cache:
                    n_cached += 1
                else:
                    n_updated += 1

                printerr(
                    root, relpath,
                    info.mode,
                    info.st.st_mtime,
                    info.size,
                    fhash, '*' if not info.hash_from_cache else '')

                manifest.write(json_line(dict(
                    path=relpath,
                    type='f',
                    size=info.size,
                    hash=fhash,
                    stat=info.stat_dict()
                )))

    except KeyboardInterrupt:
        printerr('INTERRUPTED')

    manifest.write(json_line(dict(
        eof=True
    )))
    manifest.close()
    os.rename(manifest_tmp, manifest_path)
    log.info('Manifest saved to %s', manifest_path)

    printerr('Total size (MB):', totalsize / MB)
    printerr(n_cached, 'cached,', n_updated, 'hashed')

