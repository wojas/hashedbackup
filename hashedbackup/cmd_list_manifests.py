import os
import datetime
import logging

from tabulate import tabulate

from hashedbackup.backend import get_backend
from hashedbackup.utils import decode_namespace


log = logging.getLogger(__name__)


def list_manifests(options):
    backend = get_backend(options.dst, options)
    backend.check_destination_valid()

    headers = ['Namespace', 'ID', 'Timestamp (UTC)', 'Timestamp (local)',
               'Age']
    rows = []
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

        for fname in sorted(backend.listdir(os.path.join(manifests, ns))):
            if not fname.endswith('.manifest.bz2'):
                continue

            try:
                dt_str = fname.split('.', 1)[0]
                dt = datetime.datetime.strptime(dt_str, '%Y%m%d-%H%M%S')
            except ValueError:
                log.warn('Cannot parse filename: %s', fname)
                continue
            dt = dt.replace(tzinfo=datetime.timezone.utc)

            age = now - dt
            mm, ss = divmod(age.seconds, 60)
            hh, mm = divmod(mm, 60)

            rows.append([
                name,
                dt_str,
                str(dt).split('+')[0],
                str(dt.astimezone(None)).split('+')[0],
                '{:3}d {:2}h {:2}m'.format(age.days, hh, mm)
            ])

    if not rows:
        print('No manifests found.')
        return

    print()
    print(tabulate(rows, headers=headers))

