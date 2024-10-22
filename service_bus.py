from typing import List
from azure.servicebus import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from config import (
    SB_CONNECTION_STR, EMAIL_QUEUE, SB_MAX_WAIT_TIME
)


def get_servicebus_client():
    servicebus_client = ServiceBusClient.from_connection_string(
        conn_str=SB_CONNECTION_STR)
    return servicebus_client


def get_sender(servicebus_client, queue_name: str = EMAIL_QUEUE):
    receiver = servicebus_client.get_queue_sender(queue_name=queue_name)
    return receiver


def get_receiver(servicebus_client, queue_name: str = EMAIL_QUEUE):
    sender = servicebus_client.get_queue_receiver(queue_name=queue_name)
    return sender


def send_messages(messages: List[ServiceBusMessage], sender):
    sender.send_messages(messages)


def read_messages(receiver, messages_amount: int = 1, max_wait_time: int = SB_MAX_WAIT_TIME):
    messages = receiver.receive_messages(
        max_wait_time=max_wait_time,
        max_message_count=messages_amount)

    return messages


def pop_messages(messages: List[ServiceBusMessage], receiver):
    for message in messages:
        receiver.complete_message(message)
