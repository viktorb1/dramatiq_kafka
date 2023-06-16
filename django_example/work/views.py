from django.shortcuts import render
from django.http import HttpResponse

from dramatiq import Message
from dramatiq_kafka import KafkaBroker
import dramatiq
from dramatiq.broker import get_broker


def send_message(request):
    # broker = KafkaBroker(bootstrap_servers="localhost:9092", topic="testdramatiq_topic")
    # dramatiq.set_broker(broker)

    message = Message(
        queue_name="testdramatiq_topic",
        actor_name="email_customer",
        args=("testing@gmail.com",),
        kwargs={},
        options={},
    )

    broker = get_broker()
    broker.enqueue(message)

    return HttpResponse("success")
