from django.shortcuts import render
from django.http import HttpResponse

from dramatiq import Message
from dramatiq.broker import get_broker


def send_message(request):
    message = Message(
        queue_name="bob",  # specify which kafka queue to write to
        actor_name="email_customer",  # specify which function should run
        args=("testing@gmail.com",),  # specify function arguments
        kwargs={},
        options={},
    )

    broker = get_broker()
    broker.enqueue(message)
    return HttpResponse("success")
