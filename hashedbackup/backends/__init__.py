from hashedbackup.backends.local import LocalBackend
from hashedbackup.backends.sftp import SFTPBackend


def get_backend(path, options):
    """
    :param str path: path or url as passed by user
    :type options: dict
    :rtype: BackendBase
    """
    if ':' in path:
        return SFTPBackend(path, options=options)
    else:
        return LocalBackend(path, options=options)
