# unipipeline
simple way to build the declarative and distributed data pipelines. 

## Why you should use it
- Declarative config
- Fully typed
- Multi-broker support
    - kafka
    - rabbitmq
    - inmemory pubsub


## How to Install
```bash
$ pip3 install unipipeline
```

## Example
```yml
# dag.yml
brokers:
  default_broker:
    import_template: "some.module.broker:MyBroker"

messages:
  first_message:
    import_template: "some.module.first_message:FirstMessage"

  second_message:
    import_template: "some.module.second_message:SecondMessage"

workers:
  __default__:
    broker: default_broker
    
  first_worker:
    input_message: first_message
    inport_template: "some.module.first_worker:FirstWorker"

  second_worker:
    input_message: second_message
    import_template: "some.module.second_worker:SecondWorker"
```
```python
# ./some/module/second_message.py
from unipipeline import UniMessage

class SecondMessage(UniMessage):
    some_prop: bool
    some_other_prop: str
```
```python
# ./some/module/first_worker.py
from unipipeline import UniWorker
from some.module.second_message import SecondMessage

class MyWorker(UniWorker[SecondMessage]):
  
    def handle_message(self, message: SecondMessage) -> None:
        print("hello ", message.some_other_prop)
```
```python
# main.py
from unipipeline import Uni

u = Uni("dag.yml")
u.check_load_all(create=True)
w = u.get_worker("name_of_worker")
w.send(
  some_prop=True,
  some_other_prop="World!"
)
```
