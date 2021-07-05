# unipipeline
simple way to build the declarative and distributed data pipelines. 

## Why you should use it
- Declarative strict config
- Scaffolding
- Fully typed
- Python support 3.6+ 
- Brokers support
    - kafka
    - rabbitmq
    - inmemory simple pubsub
- Interruption handling = safe user code transactions
- CLI

## How to Install
```bash
$ pip3 install unipipeline
```

## Example
```yml
# dag.yml
---

service:
  name: "example"
  echo_colors: true
  echo_level: error


external:
  service_name: {}


brokers:
  default_broker:
    import_template: "unipipeline.brokers.uni_memory_broker:UniMemoryBroker"

  ender_broker:
    import_template: "example.brokers.uni_log_broker:LogBroker"


messages:
  __default__:
    import_template: "example.messages.{{name}}:{{name|camel}}"

  input_message: {}

  inetermediate_message: {}

  ender_message: {}


cron:
  my_super_task:
    worker: my_super_cron_worker
    when: 0/1 * * * *

  my_mega_task:
    worker: my_super_cron_worker
    when: 0/2 * * * *

  my_puper_task:
    worker: my_super_cron_worker
    when: 0/3 * * * *


waitings:
  __default__:
    import_template: example.waitings.{{name}}_wating:{{name|camel}}Waiting

  common_db: {}


workers:
  __default__:
    import_template: "example.workers.{{name}}:{{name|camel}}"

  my_super_cron_worker:
    input_message: uni_cron_message

  input_worker:
    input_message: input_message
    waiting_for:
      - common_db

  intermediate_first_worker:
    input_message: inetermediate_message
    output_workers:
      - ender_second_worker
    waiting_for:
      - common_db

  intermediate_second_worker:
    input_message: inetermediate_message
    external: service_name
    output_workers:
      - ender_frist_worker

  ender_frist_worker:
    input_message: ender_message

  ender_second_worker:
    input_message: ender_message
    broker: ender_broker
    waiting_for:
      - common_db
```

## Get Started

1) create `./unipipeline.yml` such as example above

2) run cli command
```shell
unipipeline -f ./unipipeline.yml scaffold
```
It should create all structure of your workers, brokers and so on

3) remove error raising from workers

4) correct message structure for make more usefull

5) correct broker connection (if need)

6) run cli command to run your consumer
```shell
unipipeline -f ./unipipeline.yml consume input_worker
```
or with python
```python
from unipipeline import Uni
u = Uni(f'./unipipeline.yml')
u.init_consumer_worker(f'input_worker')
u.initialize()
u.start_consuming()
```

7) produce some message to the message broker by your self or with tools
```shell
unipipeline -f ./unipipeline.yml produce --worker input_worker --data='{"some": "prop"}'
```
or with python
```python
# main.py
from unipipeline import Uni

u = Uni(f'./unipipeline.yml')
u.init_producer_worker(f'input_worker')
u.initialize()
u.send_to(f'input_worker', dict(some='prop'))
```

## Definition

### Service
```yaml
service:
  name: some_name       # need for health-check file name
  echo_level: warning   # level of uni console logs (debug, info, warning, error)
  echo_colors: true     # show colors in console
```

### External
```yml
external:
  some_name_of_external_service: {}
```
- no props

- it needs for declarative grouping the external workers with service

### Worker
```yaml
workers:
  __default__:                                        # each worker get this default props if defined
    retry_max_count: 10
    
  some_worker_name:
    retry_max_count: 3                                # just counter. message move to /dev/null if limit has reached 
    retry_delay_s: 1                                  # delay before retry
    topic: "{{name}}"                                 # template string
    error_payload_topic: "{{topic}}__error__payload"  # template string
    error_topic: "{{topic}}__error"                   # template string
    broker: "default_broker"                          # broker name. reference to message transport 
    external: null                                    # name of external service. reference in this config file 
    ack_after_success: true                           # automatic ack after process message
    waiting_for:                                      # list of references
      - some_waiting_name                             # name of block. this worker must wait for connection of this external service if need
    output_workers:                                   # list of references
      - some_other_worker_name                        # allow worker sending messages to this worker
    
    inport_template: "some.module.hierarchy.to.worker.{{name}}:{{name|camel}}OfClass"   # required module and classname for import

    input_message: "name_of_message"                  # required reference of input message type 
```

### Waiting
```yaml
waitings:
  some_blocked_service_name:
    retry_max_count: 3                         # the same semantic as worker.retry_max_count
    retry_delay_s: 10                          # the same semantic as worker.retry_delay_s
    import_template: "some.module:SomeClass"   # required. the same semantic as worker.import_template
```

### Broker
```yaml
brokers:
  some_name_of_broker:
    retry_max_count: 3                         # the same semantic as worker.retry_max_count
    retry_delay_s: 10                          # the same semantic as worker.retry_delay_s
    content_type: application/json             # content type
    compression: null                          # compression (null, application/x-gzip, application/x-bz2, application/x-lzma)
    import_template: "some.module:SomeClass"   # required. the same semantic as worker.import_template
```

### Message
```yaml
messages:
  name_of_message:
    import_template: "some.module:SomeClass"   # required. the same semantic as worker.import_template
```

build in messages:
```yaml
messages:
  uni_cron_message:
    import_template: unipipeline.messages.uni_cron_message:UniCronMessage
```

## CLI

### unipipeline
```shell 
usage: unipipeline --help

UNIPIPELINE: simple way to build the declarative and distributed data pipelines. this is cli tool for unipipeline

positional arguments:
  {check,scaffold,init,consume,cron,produce}
                        sub-commands
    check               check loading of all modules
    scaffold            create all modules and classes if it is absent. no args
    init                initialize broker topics for workers
    consume             start consuming workers. connect to brokers and waiting for messages
    cron                start cron jobs, That defined in config file
    produce             publish message to broker. send it to worker

optional arguments:
  -h, --help            show this help message and exit
  --config-file CONFIG_FILE, -f CONFIG_FILE
                        path to unipipeline config file (default: ./unipipeline.yml)
  --verbose [VERBOSE]   verbose output (default: false)

```

### unipipeline check
```
usage: 
    unipipeline -f ./unipipeline.yml check
    unipipeline -f ./unipipeline.yml --verbose=yes check

check loading of all modules

optional arguments:
  -h, --help  show this help message and exit
```

### unipipeline init
```
usage: 
    unipipeline -f ./unipipeline.yml init
    unipipeline -f ./unipipeline.yml --verbose=yes init
    unipipeline -f ./unipipeline.yml --verbose=yes init --workers some_worker_name_01 some_worker_name_02

initialize broker topics for workers

optional arguments:
  -h, --help            show this help message and exit
  --workers INIT_WORKERS [INIT_WORKERS ...], -w INIT_WORKERS [INIT_WORKERS ...]
                        workers list for initialization (default: [])
```


### unipipeline scaffold
```
usage: 
    unipipeline -f ./unipipeline.yml scaffold
    unipipeline -f ./unipipeline.yml --verbose=yes scaffold

create all modules and classes if it is absent. no args

optional arguments:
  -h, --help  show this help message and exit
```

### unipipeline consume

```
usage: 
    unipipeline -f ./unipipeline.yml consume
    unipipeline -f ./unipipeline.yml --verbose=yes consume
    unipipeline -f ./unipipeline.yml consume --workers some_worker_name_01 some_worker_name_02
    unipipeline -f ./unipipeline.yml --verbose=yes consume --workers some_worker_name_01 some_worker_name_02

start consuming workers. connect to brokers and waiting for messages

optional arguments:
  -h, --help            show this help message and exit
  --workers CONSUME_WORKERS [CONSUME_WORKERS ...], -w CONSUME_WORKERS [CONSUME_WORKERS ...]
                        worker list for consuming
```

### unipipeline produce
```
usage: 
    unipipeline -f ./unipipeline.yml produce --worker some_worker_name_01 --data {"some": "json", "value": "for worker"}
    unipipeline -f ./unipipeline.yml --verbose=yes produce --worker some_worker_name_01 --data {"some": "json", "value": "for worker"}
    unipipeline -f ./unipipeline.yml produce --alone --worker some_worker_name_01 --data {"some": "json", "value": "for worker"}
    unipipeline -f ./unipipeline.yml --verbose=yes produce --alone --worker some_worker_name_01 --data {"some": "json", "value": "for worker"}

publish message to broker. send it to worker

optional arguments:
  -h, --help            show this help message and exit
  --alone [PRODUCE_ALONE], -a [PRODUCE_ALONE]
                        message will be sent only if topic is empty
  --worker PRODUCE_WORKER, -w PRODUCE_WORKER
                        worker recipient
  --data PRODUCE_DATA, -d PRODUCE_DATA
                        data for sending
```

### unipipeline cron
```
usage: 
    unipipeline -f ./unipipeline.yml cron
    unipipeline -f ./unipipeline.yml --verbose=yes cron

start cron jobs, That defined in config file

optional arguments:
  -h, --help  show this help message and exit
```
