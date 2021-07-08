import os.path

from example.workers.ender_second_worker import EnderSecondWorker
from unipipeline import Uni, UniMessageMeta

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
u.init_consumer_worker("ender_second_worker")

u.initialize()

u.send_to("input_worker", dict())

u.start_consuming()

w = u._mediator.get_worker('my_super_cron_worker')
w._uni_current_meta = UniMessageMeta.create_new({})
answ = w.send_to(EnderSecondWorker, {"some_prop": "hello!"})
print(answ)
exit()

u.start_cron()
