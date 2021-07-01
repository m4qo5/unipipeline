import logging

from unipipeline import Uni
from unipipeline.args import CMD_INIT, CMD_CHECK, CMD_CRON, CMD_PRODUCE, CMD_CONSUME, parse_args


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
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )
        logger = logging.getLogger('unipipeline')
        logger.setLevel(logging.DEBUG)

    args_cmd_map[args.cmd](args)
