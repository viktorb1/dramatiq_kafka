from django.contrib import admin
from django.urls import path

from .views import send_message

urlpatterns = [
    path("sendmessage/", send_message),
]
