import os

from hashedbackup.backends import get_backend
from hashedbackup.utils import printerr

import logging
log = logging.getLogger(__name__)

README = """This is a hashedbackup backup repository.
"""

def init_repo(backend):
    """
    :type backend: hashedbackup.backends.base.BackendBase
    """
    dst = backend.path
    log.info('Creating new repository in %s', dst)
    if not backend.exists(dst):
        if not backend.try_mkdir(dst):
            raise OSError("Count not create {}".format(dst))
    for dirname in ('manifests', 'objects', 'tmp'):
        path = os.path.join(dst, dirname)
        if not backend.try_mkdir(path):
            raise OSError("Count not create {}".format(path))

    with backend.open(os.path.join(dst, 'README.txt'), 'w') as f:
        f.write(README)
    log.info('Repository successfully created')


def init(options):
    backend = get_backend(options.dst, options)
    dst = backend.path

    if not backend.exists(dst):
        if not backend.exists(os.path.dirname(backend.path)):
            printerr("ERROR: Parent directory of {} does not exist".format(dst))
        else:
            init_repo(backend)
    else:
        if not backend.isdir(dst):
            printerr("ERROR: {} is not a directory".format(dst))
        elif backend.listdir(dst):
            printerr("ERROR: {} is not empty".format(dst))
        else:
            init_repo(backend)
