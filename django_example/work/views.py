from django.shortcuts import render
from django.http import HttpResponse

from dramatiq import Message
from dramatiq.broker import get_broker


def send_message(request):
    message = Message(
        queue_name="rita",
        actor_name="email_customer",
        args=("testing@gmail.com",),
        kwargs={},
        options={},
    )

    broker = get_broker()
    print(broker.topic)
    # broker.enqueue(message)
    return HttpResponse("success")
