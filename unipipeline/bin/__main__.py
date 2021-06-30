import logging

from .args import args, CMD_INIT, CMD_CHECK, CMD_PRODUCE, CMD_CONSUME
from .. import Uni


def run_check(args) -> None:
    u = Uni(args.config_file)
    u.check(args.check_create)


def run_init(args) -> None:
    u = Uni(args.config_file)
    for wn in args.init_workers:
        u.init_producer_worker(wn)
    u.initialize(everything=args.init_everything, create=args.init_create)


def run_consume(args) -> None:
    u = Uni(args.config_file)
    for wn in args.consume_workers:
        u.init_consumer_worker(wn)
    u.initialize()
    u.start_consuming()


def run_produce(args) -> None:
    u = Uni(args.config_file)
    u.init_producer_worker(args.produce_worker)
    u.initialize()
    u.send_to(args.produce_worker, args.produce_data, alone=args.produce_alone)


args_cmd_map = {
    CMD_INIT: run_init,
    CMD_CHECK: run_check,
    CMD_PRODUCE: run_produce,
    CMD_CONSUME: run_consume,
}

if args.verbose:
    logger = logging.getLogger('unipipeline')
    logger.setLevel(logging.DEBUG)

args_cmd_map[args.cmd](args)

print('DONE')
