import logging
import os.path

from unipipeline import Uni


logging.basicConfig(
    level=os.environ.get('LOGLEVEL', logging.DEBUG),
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

CWD = os.path.dirname(os.path.abspath(__file__))

u = Uni(f"{CWD}/dag.yml")

u.check_load_all(create=True)

u.send_to_worker("input_worker", dict())

u.consume("input_worker")

u.consume("my_super_cron_worker")

u.initialize(create=True)

u.start_consuming()

u.start_cron()
