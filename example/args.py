import argparse

import logging
from sys import stdout

logging.getLogger('amqp').setLevel(logging.DEBUG)
logging.getLogger('amqp.connection.Connection.heartbeat_tick').setLevel(logging.DEBUG)
logging.getLogger('amqp.connection.Connection').setLevel(logging.DEBUG)
logging.getLogger('amqp.abstract_channel').setLevel(logging.DEBUG)
logging.getLogger('amqp.basic_message').setLevel(logging.DEBUG)
logging.getLogger('amqp.channel').setLevel(logging.DEBUG)
logging.getLogger('amqp.connection').setLevel(logging.DEBUG)
logging.getLogger('amqp.exceptions').setLevel(logging.DEBUG)
logging.getLogger('amqp.method_framing').setLevel(logging.DEBUG)
logging.getLogger('amqp.platform').setLevel(logging.DEBUG)
logging.getLogger('amqp.protocol').setLevel(logging.DEBUG)
logging.getLogger('amqp.sasl').setLevel(logging.DEBUG)
logging.getLogger('amqp.serialization').setLevel(logging.DEBUG)
logging.getLogger('amqp.spec').setLevel(logging.DEBUG)
logging.getLogger('amqp.transport').setLevel(logging.DEBUG)
logging.getLogger('amqp.utils').setLevel(logging.DEBUG)
logging.getLogger('pamqp').setLevel(logging.DEBUG)
logging.getLogger('pamqp.frame').setLevel(logging.DEBUG)
logging.getLogger('socket').setLevel(logging.DEBUG)

logging.basicConfig(level=logging.DEBUG, handlers=[logging.StreamHandler(stream=stdout)])

parser = argparse.ArgumentParser()
parser.add_argument('--type', dest='type', choices=('mem', 'kafka', 'rmq'), type=str)
parser.add_argument('--count', dest='produce_count', type=int, required=False, default=3)
parser.add_argument('--delay', dest='delay', type=int, required=False, default=1)
parser.add_argument('--worker', dest='worker', type=str, required=False, default=None)

args = parser.parse_args()
