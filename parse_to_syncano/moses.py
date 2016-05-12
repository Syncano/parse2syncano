#!/usr/bin/python
# -*- coding: utf-8 -*-
import argparse

from parse_to_syncano import log
from parse_to_syncano.config import CONFIG_VARIABLES_NAMES, P2S_CONFIG_PATH, config
from parse_to_syncano.migrations.transfer import SyncanoTransfer

COMMANDS = {}


def command(func):
    COMMANDS[func.func_name] = func
    return func


def argument(*args, **kwargs):
    def wrapper(f):
        if not hasattr(f, 'arguments'):
            f.arguments = []
        f.arguments.append((args, kwargs))
        return f
    return wrapper


def force_config_value(config_var_name, section='P2S'):
    config_var = raw_input('{}: '.format(config_var_name))
    config.set(section, config_var_name, config_var)


def check_config_value(config_var_name, silent, section='P2S'):
    config_var = config.get(section, config_var_name)
    if not config_var:
        force_config_value(config_var_name, section)
    else:
        if not silent:
            print("{config_var_name}: \t\t{config_var}".format(
                config_var_name=config_var_name,
                config_var=config_var
            ))


def write_config_to_file():
    with open(P2S_CONFIG_PATH, 'wb') as config_file:
        config.write(config_file)


def check_configuration(silent=False):
    for config_var_name in CONFIG_VARIABLES_NAMES:
        check_config_value(config_var_name, silent=silent)

    write_config_to_file()


def force_configuration_overwrite():
    for config_var_name in CONFIG_VARIABLES_NAMES:
        force_config_value(config_var_name)

    write_config_to_file()


def print_configuration():
    for config_var_name in CONFIG_VARIABLES_NAMES:
        print("{config_var_name}: \t\t{config_var}".format(
            config_var_name=config_var_name,
            config_var=config.get('P2S', config_var_name) or "-- NOT SET --"
        ))


@command
def sync(namespace):
    """
    Synchronize the parse data object with syncano data objects;
    """
    check_configuration(silent=True)

    confirmation = raw_input('Are you sure you want to copy your data from PARSE application ({application_id})'
                             'to the syncano isntance ({instance_name})? Y/N [Y]: ') or 'Y'

    if confirmation != 'Y':
        return

    transfer = SyncanoTransfer()
    transfer.through_the_red_sea()


@command
@argument('-c', '--current', action='store_true', help="Show current configuration.")
@argument('-f', '--force', action='store_true', help="Froce to overwrite previous config.")
def configure(namespace):
    """
    Configure the data needed for connection to the parse and syncano;
    """
    if namespace.current:
        print_configuration()
    elif namespace.force:
        force_configuration_overwrite()
    else:
        check_configuration()


def parse2syncano():
    parser = argparse.ArgumentParser(
        description='Parse -> Syncano transfer tool.'
    )

    parser.add_argument('--config', default=P2S_CONFIG_PATH,
                        help='Parse to Syncano configuration file path.')

    subparsers = parser.add_subparsers(
        title='subcommands',
        description='valid subcommands'
    )

    for fname, func in COMMANDS.iteritems():
        subparser = subparsers.add_parser(fname, description=func.__doc__)
        for args, kwargs in getattr(func, 'arguments', []):
            subparser.add_argument(*args, **kwargs)
        subparser.set_defaults(func=func)
    namespace = parser.parse_args()
    try:
        namespace.func(namespace)
    except Exception as e:
        log.error(e)
        log.error('An error occurred. Please contact with the Syncano team.')


if __name__ == '__main__':
    parse2syncano()