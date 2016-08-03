import abc
import os

from hashedbackup.utils import temp_filename


class BackendBase(abc.ABC):

    def __init__(self, path, options):
        self.path = path
        self.options = options

    @abc.abstractmethod
    def try_mkdir(self, path): pass

    @abc.abstractmethod
    def exists(self, path): pass

    @abc.abstractmethod
    def open(self, *args, **kwargs): pass

    @abc.abstractmethod
    def rename(self, src, dst): pass

    @abc.abstractmethod
    def delete(self, path): pass

    @abc.abstractmethod
    def add_object(self, fhash, fpath): pass

    @abc.abstractmethod
    def listdir(self, path): pass

    @abc.abstractmethod
    def isdir(self, path): pass

    @abc.abstractmethod
    def get_object_hashes(self): pass

    def temppath(self):
        return os.path.join(self.path, 'tmp', temp_filename())

    def object_path(self, fhash):
        return os.path.join(self.path, 'objects', fhash[0:2], fhash[2:4], fhash)

    def check_destination_valid(self):
        if not self.exists(os.path.join(self.path, 'manifests')):
            raise FileNotFoundError(
                "Invalid backup destination (did you run `hashedbackup init`?)")