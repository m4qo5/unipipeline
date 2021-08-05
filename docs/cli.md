# Command Line Interface

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

## check
```
usage: 
    unipipeline -f ./unipipeline.yml check
    unipipeline -f ./unipipeline.yml --verbose=yes check

check loading of all modules

optional arguments:
  -h, --help  show this help message and exit
```

## init
```
usage: 
    unipipeline -f ./unipipeline.yml init
    unipipeline -f ./unipipeline.yml --verbose=yes init

initialize broker topics for workers

optional arguments:
  -h, --help            show this help message and exit
  --workers INIT_WORKERS [INIT_WORKERS ...], -w INIT_WORKERS [INIT_WORKERS ...]
                        workers list for initialization (default: [])
```

## scaffold
```
usage: 
    unipipeline -f ./unipipeline.yml scaffold
    unipipeline -f ./unipipeline.yml --verbose=yes scaffold

create all modules and classes if it is absent. no args

optional arguments:
  -h, --help  show this help message and exit
```

## consume
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

## produce
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

## cron
```
usage: 
    unipipeline -f ./unipipeline.yml cron
    unipipeline -f ./unipipeline.yml --verbose=yes cron

start cron jobs, That defined in config file

optional arguments:
  -h, --help  show this help message and exit
```
