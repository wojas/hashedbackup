import os

from hashedbackup.utils import printerr

import logging
log = logging.getLogger(__name__)

README = """This is a hashedbackup backup repository.
"""

def init_repo(dst):
    log.info('Creating new repository in %s', dst)
    if not os.path.exists(dst):
        os.mkdir(dst)
    for dirname in ('manifests', 'objects', 'tmp'):
        os.mkdir(os.path.join(dst, dirname))
    with open(os.path.join(dst, 'README.txt'), 'w') as f:
        f.write(README)
    log.info('Repository successfully created')


def is_empty_dir(dst):
    return not os.listdir(dst)


def init(options):
    dst = options.dst
    if not os.path.exists(dst):
        if not os.path.exists(os.path.dirname(dst)):
            printerr("ERROR: Parent directory of {} does not exist".format(dst))
        else:
            init_repo(dst)
    else:
        if not os.path.isdir(dst):
            printerr("ERROR: {} is not a directory".format(dst))
        elif not is_empty_dir(dst):
            printerr("ERROR: {} is not empty".format(dst))
        else:
            init_repo(dst)
