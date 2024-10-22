import json
from config import QUEUE_NAME, SB_MAX_WAIT_TIME, SB_MESSAGES_TO_READ
from service_bus import (
    get_servicebus_client,
    pop_messages,
    read_messages,
)

SYSTEM_TO_DELETE_FROM_QUEUE = 'alert_contract_approved'
TO_EMAIL_TO_DELETE_FROM_QUEUE = 'pw.lima@voltalia.com'


def pop_all_messages():
    servicebus_client = get_servicebus_client()
    receiver = servicebus_client.get_queue_receiver(
        queue_name=QUEUE_NAME)
    messages = read_messages(receiver, SB_MESSAGES_TO_READ, SB_MAX_WAIT_TIME)
    while messages:
        for message in messages:
            try:
                message_dict = json.loads(str(messages[0]))
                system = message_dict.get('system')
                to_email = message_dict.get('to_email')
                if system == SYSTEM_TO_DELETE_FROM_QUEUE and to_email == TO_EMAIL_TO_DELETE_FROM_QUEUE:
                    pop_messages([message], receiver)
            except:
                continue
        messages = read_messages(
            receiver, SB_MESSAGES_TO_READ, SB_MAX_WAIT_TIME)


pop_all_messages()
