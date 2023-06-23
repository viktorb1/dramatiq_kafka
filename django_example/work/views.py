from django.shortcuts import render
from django.http import HttpResponse

from dramatiq import Message
from dramatiq.broker import get_broker


def send_message(request):
    message = Message(
        queue_name="email",  # specify which kafka queue to write to
        actor_name="send_emails",  # specify which function should run
        args=("testing@gmail.com",),  # specify function arguments
        kwargs={},
        options={},
    )

    broker = get_broker()
    broker.enqueue(message)
    return HttpResponse("success")
