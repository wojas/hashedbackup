import os
from configparser import ConfigParser
import logging
import sys

from tabulate import tabulate

from hashedbackup.cmd_list_manifests import get_remote_manifests
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


def age_for_profiles(profiles, options):
    remotes = set()
    for name in profiles.sections():
        section = profiles[name]
        dst = section.get('dst', fallback=None)
        if dst:
            remotes.add(dst)

    ages = {}
    for remote in remotes:
        log.verbose('Fetching list of manifests from remote %s', remote)
        ages[remote] = {}

        # TODO: this is hacky - refactor get_remote_manifests
        options.dst = remote
        options.namespace = None
        manifests = get_remote_manifests(options)

        for name, items in manifests.items():
            if not items:
                continue
            ages[remote][name] = items[-1]['age_str']

    return ages


def show_profiles(profiles, options):
    headers = ['profile', 'src', 'dst', 'namespace']
    if options.age:
        ages = age_for_profiles(profiles, options)
        headers.append('age of last backup')
    else:
        ages = {}

    rows = []
    for name in profiles.sections():
        section = profiles[name]
        dst = section.get('dst')
        namespace = section.get('namespace')
        row = [
            name,
            section.get('src'),
            dst,
            namespace,
        ]
        if options.age:
            row.append(ages.get(dst, {}).get(namespace, None))

        rows.append(row)

    if rows:
        print(tabulate(rows, headers=headers))
    else:
        print(HELP)


def backup_profile(options):
    profiles = read_profiles()
    name = options.profile_name

    if not name:
        show_profiles(profiles, options)
    elif not name in profiles:
        log.error('Profile %s not found.', name)
        show_profiles(profiles, options)
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
