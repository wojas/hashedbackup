import os
import datetime
import logging

from tabulate import tabulate

from hashedbackup.backends import get_backend
from hashedbackup.utils import decode_namespace


log = logging.getLogger(__name__)


def get_remote_manifests(options):
    backend = get_backend(options.dst, options)
    backend.check_destination_valid()

    manifest_dict = {}
    manifests = os.path.join(backend.path, 'manifests')
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    for ns in sorted(backend.listdir(manifests)):
        try:
            name = decode_namespace(ns)
        except ValueError:
            log.warn('Cannot parse namespace directory name: %s', ns)
            continue

        if options.namespace and name != options.namespace:
            continue

        try:
            filenames = sorted(backend.listdir(os.path.join(manifests, ns)))
        except (NotADirectoryError, OSError):
            # .DS_Store, etc
            continue

        manifest_dict[name] = []

        for fname in filenames:
            if not fname.endswith('.manifest.bz2'):
                continue

            try:
                dt_str = fname.split('.', 1)[0]
                dt = datetime.datetime.strptime(dt_str, '%Y%m%d-%H%M%S')
            except ValueError:
                log.warn('Cannot parse filename: %s', fname)
                continue
            dt = dt.replace(tzinfo=datetime.timezone.utc)
            dt_local = dt.astimezone(None)

            age = now - dt
            mm, ss = divmod(age.seconds, 60)
            hh, mm = divmod(mm, 60)

            manifest_dict[name].append(dict(
                filename=fname,
                id=dt_str,
                utc=dt,
                utc_str=str(dt).split('+')[0],
                local=dt_local,
                local_str=str(dt_local).split('+')[0],
                age=age,
                age_str='{:3}d {:2}h {:2}m'.format(age.days, hh, mm)
            ))

    return manifest_dict


def list_manifests(options):
    manifests = get_remote_manifests(options)

    headers = ['Namespace', 'ID', 'Timestamp (UTC)', 'Timestamp (local)',
               'Age']
    rows = []

    for name, items in sorted(manifests.items()):
        for manifest in items:
            rows.append([
                name,
                manifest['id'],
                manifest['utc_str'],
                manifest['local_str'],
                manifest['age_str'],
            ])

    if not rows:
        print('No manifests found.')
        return

    print()
    print(tabulate(rows, headers=headers))

