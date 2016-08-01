import argparse
import sys
import logging

from hashedbackup import cmd_init, cmd_backup


log = logging.getLogger(__name__)

parser = argparse.ArgumentParser(prog='hashedbackup')
#parser.add_argument('--foo', action='store_true', help='foo help')

subparsers = parser.add_subparsers(
    dest='command',
    title='subcommands',
    description='valid subcommands',
    help='sub-command help')

p = subparsers.add_parser('init',
    help='Initialize a backup repository. This is used to initialize the '
         'backup destination.')
p.add_argument('dst', type=str, help='backup destination')

p = subparsers.add_parser('backup', help='backup a directory')
p.add_argument('src', type=str, help='directory to backup')
p.add_argument('dst', type=str, help='backup destination')
p.add_argument('-n', '--namespace', type=str, required=True,
    help='backup namespace (allows backups of different folders '
         'to share the same hash database)')


def main():
    options = parser.parse_args()
    if not options.command:
        print(parser.format_help(), file=sys.stderr)
        return

    logformat = '%(message)s'
    logging.basicConfig(level=logging.DEBUG, format=logformat)

    log.debug("Command options: %s", options)

    if options.command == 'init':
        cmd_init.init(options)
    elif options.command == 'backup':
        cmd_backup.backup(options)
    else:
        raise NotImplementedError(options.command)

