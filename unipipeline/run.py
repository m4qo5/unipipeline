import logging
import sys

from unipipeline import Uni
from unipipeline.args import CMD_INIT, CMD_CHECK, CMD_CRON, CMD_PRODUCE, CMD_CONSUME, parse_args
from unipipeline.utils.log import children_loggers


def run_check(args) -> None:
    u = Uni(args.config_file)
    u.check(args.check_create)


def run_cron(args) -> None:
    u = Uni(args.config_file)
    u.init_cron()
    u.initialize()
    u.start_cron()


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
    CMD_CRON: run_cron,
    CMD_PRODUCE: run_produce,
    CMD_CONSUME: run_consume,
}


def main():
    args = parse_args()

    if args.verbose:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s'))

        logging.basicConfig(format='%(asctime)s | %(name)s | %(levelname)s | %(message)s', level=logging.DEBUG, stream=sys.stdout)

        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.disabled = False
        logger.propagate = True

        for ln in children_loggers:
            logger = logging.getLogger(ln)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
            logger.disabled = False
            logger.propagate = True

    args_cmd_map[args.cmd](args)
