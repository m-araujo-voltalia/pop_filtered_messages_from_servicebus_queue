import json
from azure.servicebus import ServiceBusMessage
from config import SB_MESSAGES_TO_READ
from service_bus import (
    get_receiver,
    get_sender,
    get_servicebus_client,
    read_messages,
    send_messages,
)


def write_and_read_messages():
    # with open('email_message_personalized.json') as file_content:
    with open('email_message.json') as file_content:
        message = ServiceBusMessage(json.dumps(json.load(file_content)))

        servicebus_client = get_servicebus_client()
        sender = get_sender(servicebus_client)
        receiver = get_receiver(servicebus_client)

        send_messages([message], sender)

        messages = read_messages(receiver, SB_MESSAGES_TO_READ)
        print(messages)


def send():
    write_and_read_messages()


send()
