import os
from configparser import ConfigParser
import logging

import sys
from tabulate import tabulate

from .cmd_backup import backup

log = logging.getLogger(__name__)

EXAMPLE = """[pictures]
src=~/Pictures
dst=myserver:backups/pictures
namespace=laptop-pictures"""

HELP = """No profiles found in ~/.hashedbackup/profiles

You can create this file and add sections like this for all your
different backups:

{}

Now you only need to run this to perform another backup:

hashedbackup backup-profile pictures
""".format(EXAMPLE)

REQUIRED_KEYS = ['src', 'dst', 'namespace']

def read_profiles():
    config = ConfigParser()
    path = os.path.expanduser('~/.hashedbackup/profiles')
    if os.path.exists(path):
        config.read(path)
    return config


def show_profiles(profiles):
    headers = ['profile', 'src', 'dst', 'namespace']
    rows = []
    for name in profiles.sections():
        section = profiles[name]
        rows.append([
            name,
            section.get('src'),
            section.get('dst'),
            section.get('namespace'),
        ])

    if rows:
        print(tabulate(rows, headers=headers))
    else:
        print(HELP)


def backup_profile(options):
    profiles = read_profiles()
    name = options.profile_name

    if not name:
        show_profiles(profiles)
    elif not name in profiles:
        log.error('Profile %s not found.')
        show_profiles(profiles)
    else:
        profile = profiles[name]
        for key in REQUIRED_KEYS:
            if not key in profile or not profile[key]:
                log.error('Profile missing key %s', key)
                print('Example section:')
                print(EXAMPLE)
                sys.exit(1)

        options.src = os.path.expanduser(profile['src'])
        if profile['dst'].startswith('~'):
            options.dst = os.path.expanduser(profile['dst'])
        else:
            options.dst = profile['dst']
        options.namespace = os.path.expanduser(profile['namespace'])
        options.symlink = profile.getboolean('symlink', fallback=False)
        options.hardlink = profile.getboolean('hardlink', fallback=False)

        backup(options)
