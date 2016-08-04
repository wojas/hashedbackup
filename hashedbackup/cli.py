import argparse
import sys
import logging
import colorlog

# Add extra VERBOSE log level between DEBUG and INFO
VERBOSE = 15
logging.addLevelName(VERBOSE, "VERBOSE")
def verbose(self, message, *args, **kws):
    if self.isEnabledFor(VERBOSE):
        self._log(VERBOSE, message, args, **kws)
logging.Logger.verbose = verbose

from hashedbackup import cmd_init, cmd_backup, cmd_list_manifests, \
    cmd_backup_profile


log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(prog='hashedbackup')
parser.add_argument('-v', '--verbose', action='store_true',
    help='Enable more verbose output')
parser.add_argument('--debug', action='store_true',
    help='Enable debug output')

subparsers = parser.add_subparsers(
    dest='command',
    title='subcommands',
    description='valid subcommands',
    help='sub-command help')

p = subparsers.add_parser('init',
    help='Initialize a backup repository. This is used to initialize the '
         'backup destination.')
p.add_argument('dst', type=str, help='backup destination')

p = subparsers.add_parser('list-manifests',
    help='List manifests in backup repository')
p.add_argument('dst', type=str, help='backup destination')
p.add_argument('-n', '--namespace', type=str,
    help='backup namespace (allows backups of different folders '
         'to share the same hash database)')

p = subparsers.add_parser('backup', help='backup a directory')
p.add_argument('src', type=str, help='directory to backup')
p.add_argument('dst', type=str, help='backup destination')
p.add_argument('-n', '--namespace', type=str, required=True,
    help='backup namespace (allows backups of different folders '
         'to share the same hash database)')
p.add_argument('--symlink', action='store_true',
    help='Add symbolic links into the repository instead of copying data. '
         'WARNING: This is useless as a backup. Only useful for testing or '
         'when using another tool that follows symlinks for tha actual backup.')
p.add_argument('--hardlink', action='store_true',
    help='Add hard links into the repository instead of copying data. '
         'This only works on the same filesystem, which makes it not very '
         'useful as a backup. Application can also modify the data through the '
         'other link, corrupting the repository. '
         'WARNING: Hard links are unreliable on OS X. On 10.11 (El Capitan) '
         'editing a file in Preview.app (and probably in other apps too) '
         'will destroy the data for the other link!')

p = subparsers.add_parser('backup-profile',
    help='Run a backup profile defined in ~/.hashedbackup/profiles')
p.add_argument('profile_name', nargs='?', type=str, help='profile to use')
p.add_argument('--age', action='store_true',
    help='Check age of last backup when listing profiles '
         '(requires connecting to repositories)')


def setup_logging(options):
    handler = colorlog.StreamHandler()
    if sys.stderr.isatty():
        formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(levelname)-8s%(reset)s %(message_log_color)s%(message)s",
            datefmt=None,
            reset=True,
            log_colors={
                'DEBUG': 'bold_black',
                'INFO': 'green',
                'WARNING': 'bold_yellow',
                'ERROR': 'bold_red',
                'CRITICAL': 'bold_red,bg_white',
            },
            secondary_log_colors={
                'message': {
                    'DEBUG': 'bold_black',
                    'ERROR': 'bold_red',
                    'CRITICAL':'bold_red'
                }
            },
            style='%'
        )
    else:
        formatter = logging.Formatter(
            "%(levelname)-8s %(message)s",
            style='%'
        )
    handler.setFormatter(formatter)
    if options.debug:
        level = logging.DEBUG
    elif options.verbose:
        level = VERBOSE
    else:
        level = logging.INFO
    logging.basicConfig(level=level, handlers=[handler])


def main():
    options = parser.parse_args()
    if not options.command:
        print(parser.format_help(), file=sys.stderr)
        return

    setup_logging(options)
    log.debug("Command options: %s", options)

    if options.command == 'init':
        cmd_init.init(options)
    elif options.command == 'backup':
        cmd_backup.backup(options)
    elif options.command == 'backup-profile':
        cmd_backup_profile.backup_profile(options)
    elif options.command == 'list-manifests':
        cmd_list_manifests.list_manifests(options)
    else:
        raise NotImplementedError(options.command)
