# Get Started

## How to Install
```bash
$ pip3 install unipipeline
```

## Create simple config file
```yaml
# dag.yml
---
service:
  name: "example"

brokers:
  default_broker:
    import_template: "unipipeline.brokers.uni_memory_broker:UniMemoryBroker"

messages:
  __default__:
    import_template: "example.messages.{{name}}:{{name|camel}}"

  input_message: {}

cron:
  my_super_task:
    worker: my_super_cron_worker
    when: 0/1 * * * *

workers:
  __default__:
    import_template: "example.workers.{{name}}:{{name|camel}}"

  my_super_cron_worker:
    input_message: uni_cron_message

  input_worker:
    input_message: input_message
    waiting_for:
      - common_db
```


## Scaffold
```bash
$ unipipeline -f ./dag.yml scaffold
```


## Run

```bash
$ unipipeline -f ./dag.yml consume input_worker
```


```bash
$ unipipeline -f ./dag.yml produce input_worker --data='{"some": "prop"}'
```
