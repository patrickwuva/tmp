from google.cloud import pubsub_v1
from add_offenders import insert_offenders
from get_offenders import get_offenders
import json

project_id = "your-project-id"
subscription_id = "your-subscription-id"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):

    print(f"Received message: {message.data.decode('utf-8')}")
    data_str = message.data.decode('utf-8')
    zip_codes = json.loads(data_str)
    offenders = get_offenders(zip_codes)
    insert_offenders(offenders)
    print(f'done with {zip_codes}')
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull_future.result()  # Block and listen for messages
except KeyboardInterrupt:
    streaming_pull_future.cancel()
