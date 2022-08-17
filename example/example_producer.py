import os.path
import sys
from datetime import datetime
from time import sleep

CWD = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.dirname(CWD))

from example.args import args
from unipipeline.modules.uni import Uni

u = Uni(f"{CWD}/dag-{args.type}.yml")

u.init_producer_worker('input_worker')
u.init_producer_worker('auto_retry_worker')
u.initialize()

for i in range(args.produce_count):
    batch = []
    for j in range(args.produce_batch_count):
        some = "Привет World"  # * 1000000
        batch.append(dict(value=i, some=some))
    u.send_to("input_worker", batch)
    # if i % 1000 == 0:
    u.send_to("auto_retry_worker", batch)
    #     sleep(14)
    print('>> SENT at', datetime.now())  # noqa
    sleep(args.delay)

# u.exit()
