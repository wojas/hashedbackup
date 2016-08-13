from hashedbackup.backends.local import LocalBackend
from hashedbackup.backends.sftp import SFTPBackend

_backend_cache = {}

def get_backend(path, options, *, nocache=False):
    """
    :param str path: path or url as passed by user
    :type options: dict
    :param bool nocache: do not return a cached backend
    :rtype: BackendBase
    """
    if not nocache and path in _backend_cache:
        return _backend_cache[path]

    if ':' in path:
        backend = SFTPBackend(path, options=options)
    else:
        backend = LocalBackend(path, options=options)

    _backend_cache[path] = backend
    return backend
