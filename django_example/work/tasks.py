import dramatiq


@dramatiq.actor(queue_name="json")
def email_customer(test):
    print("LOLOLOLOLOL testing")
    print()
    print("HERES THE DATA", test)
