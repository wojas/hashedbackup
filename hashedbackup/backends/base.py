import abc
import json
import os
import logging
import sys

from hashedbackup.messages import UPGRADE_TO_REPOSITORY_V1
from hashedbackup.utils import temp_filename, printerr

log = logging.getLogger(__name__)


class BackendBase(abc.ABC):

    def __init__(self, path, options):
        self.path = path
        self.options = options
        self.repo_config = None

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
        return os.path.join(self.path, 'objects', fhash[0:2], fhash)

    def check_destination_valid(self):
        # TODO: create our own exception types
        repo_config_path = os.path.join(self.path, 'hashedbackup.json')
        try:
            with self.open(repo_config_path, 'rb') as f:
                self.repo_config = json.loads(f.read().decode('utf-8'))
                if not 'version' in self.repo_config:
                    raise Exception("Invalid hashedbackup.json: no version key")

                version = self.repo_config['version']
                if version != 1:
                    raise Exception(
                        "Repository version is {}, while this version of "
                        "hashedbackup only supports version 1".format(version))
        except OSError:
            if self.exists(repo_config_path):
                raise Exception("{} is not readable".format(repo_config_path))

            if self.exists(os.path.join(self.path, 'manifests')):
                log.error("Version 0 repository formats no longer supported")
                printerr(UPGRADE_TO_REPOSITORY_V1)
                sys.exit(1)
            else:
                raise FileNotFoundError(
                    "Invalid backup destination "
                    "(did you run `hashedbackup init`?)")