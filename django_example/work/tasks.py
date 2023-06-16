import dramatiq


@dramatiq.actor(queue_name="test")
def email_customer(email):
    print("TESTING")
    print()
    print("Here is the email", email)
