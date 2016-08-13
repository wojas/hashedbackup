import json
import os

from hashedbackup.backends import get_backend
from hashedbackup.utils import printerr, object_bucket_dirs

import logging
log = logging.getLogger(__name__)

README = """This is a hashedbackup backup repository.

More info: https://github.com/wojas/hashedbackup
"""

def init_repo(backend):
    """
    :type backend: hashedbackup.backends.base.BackendBase
    """
    dst = backend.path
    log.info('Creating new repository in %s', dst)
    if not backend.exists(dst):
        if not backend.try_mkdir(dst):
            raise OSError("Could not create {}".format(dst))
    for dirname in ('objects', 'tmp'):
        path = os.path.join(dst, dirname)
        if not backend.try_mkdir(path):
            raise OSError("Could not create {}".format(path))

    for dirname in object_bucket_dirs():
        path = os.path.join(dst, 'objects', dirname)
        if not backend.try_mkdir(path):
            raise OSError("Could not create {}".format(path))

    with backend.open(os.path.join(dst, 'README.txt'), 'w') as f:
        f.write(README)

    # This was previously used to detect valid repositories
    path = os.path.join(dst, 'manifests')
    if not backend.try_mkdir(path):
        raise OSError("Could not create {}".format(path))

    # Do this last, because we use it to detect a valid repository
    with backend.open(os.path.join(dst, 'hashedbackup.json'), 'w') as f:
        repo_config = {
            'version': 1,
        }
        f.write(json.dumps(repo_config, ensure_ascii=True, indent=2))

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
