Uses kafka-python package to write a Kafka broker for dramatiq.

```python

import dramatiq

from dramatiq.middleware import AgeLimit, TimeLimit, Callbacks, Pipelines, Prometheus, Retries
from dramatiq_kafka import KafkaBroker

broker = KafkaBroker(
    bootstrap_servers="localhost:9092",
    topic="testdramatiq_topic", # default is `default`
    group_id="testdramatiq_consumergroup", # optional, default is `default`
    middleware=[
        Prometheus(),
        AgeLimit(),
        TimeLimit(),
        Callbacks(),
        Pipelines(),
        Retries(min_backoff=1000, max_backoff=900000, max_retries=96),
    ],
)
dramatiq.set_broker(broker)
```

Use dramatiq to send a message to a Kafka topic with the following format:

```python
{
    "queue_name": "testdramatiq_topic",
    "actor_name": "email_customer",
    "args": ["testing@gmail.com"],
    "kwargs": {},
    "options": {},
    "message_id": "e566b8d8-4e3d-437d-989f-939dd4f8ee04",
    "message_timestamp": 1686877955147
}
```

Write tasks as described in django_dramatiq's documentation (i.e. create dramatiq actors in the task.py files) and modify settings as shown in `django_example` to get started. To send messages, view the views.py file for an example
