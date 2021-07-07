import os.path

from unipipeline import Uni


# import logging
# logging.basicConfig(
#     level=os.environ.get('LOGLEVEL', logging.DEBUG),
#     format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
# )

CWD = os.path.dirname(os.path.abspath(__file__))

u = Uni(f"{CWD}/dag.yml")

u.scaffold()

u.check()

u.init_producer_worker('input_worker')

# u.init_consumer_worker("input_worker")

u.init_consumer_worker("my_super_cron_worker")

u.initialize()

u.send_to("input_worker", dict())

u.start_consuming()

u.start_cron()
