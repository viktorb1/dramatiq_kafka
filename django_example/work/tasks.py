import dramatiq


@dramatiq.actor(queue_name="rita")
def email_customer(email):
    print("I received a message from rita queue", email)
