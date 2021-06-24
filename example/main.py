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

u.get_worker("input_worker").send(dict())

u.get_worker("input_worker").consume()

u.get_worker("my_super_cron_worker").consume()

print("it works!")

u.start_cron()
