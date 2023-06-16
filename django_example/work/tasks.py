import dramatiq


@dramatiq.actor(queue_name="testdramatiq_topic")
def email_customer(email):
    print("TESTING")
    print()
    print("Here is the email", email)
