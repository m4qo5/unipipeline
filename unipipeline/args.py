import json
import os.path
from argparse import ArgumentParser, ArgumentTypeError


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    raise ArgumentTypeError('Boolean value expected.')


def file(v):
    if os.path.isfile(v):
        return v
    rel = os.path.join(os.getcwdb(), os.path.normpath(v))
    if os.path.isfile(rel):
        return rel
    raise ArgumentTypeError(f'file {v} is not exists')


def json_data(v):
    try:
        return json.loads(v)
    except Exception as e:
        raise ArgumentTypeError(f'INVALID JSON: {e}')


CMD_INIT = 'init'
CMD_CHECK = 'check'
CMD_CONSUME = 'consume'
CMD_PRODUCE = 'produce'
CMD_CRON = 'cron'


parser = ArgumentParser(description='cli tool for unipipeline')

parser.add_argument('--config-file', '-f', default='./unipipeline.yml', type=file, dest='config_file', help='path to config file', required=True)
parser.add_argument('--verbose', default=False, type=str2bool, const=True, nargs='?', dest='verbose')

subparsers = parser.add_subparsers(help='sub-command help', required=True, dest='cmd')

check_parser = subparsers.add_parser(CMD_CHECK)
check_parser.add_argument('--create', type=str2bool, nargs='?', const=True, default=False, dest='check_create')

init_parser = subparsers.add_parser(CMD_INIT)
init_parser.add_argument('--everything', type=str2bool, nargs='?', const=True, default=True, dest='init_everything', help='init everything')
init_parser.add_argument('--workers', '-w', type=str, nargs='+', default=[], required=False, dest='init_workers', help='workers list for initialization')
init_parser.add_argument('--create', type=str2bool, nargs='?', const=True, default=True, dest='init_create')

consume_parser = subparsers.add_parser(CMD_CONSUME)
consume_parser.add_argument('--workers', '-w', type=str, nargs='+', required=True, dest='consume_workers', help='worker list for consuming')

consume_parser = subparsers.add_parser(CMD_CRON)

produce_parser = subparsers.add_parser(CMD_PRODUCE)
produce_parser.add_argument('--alone', '-a', type=str2bool, nargs='?', const=True, default=False, dest='produce_alone')
produce_parser.add_argument('--worker', '-w', type=str, required=True, dest='produce_worker')
produce_parser.add_argument('--data', '-d', type=json_data, required=True, dest='produce_data')


def parse_args():
    return parser.parse_args()
