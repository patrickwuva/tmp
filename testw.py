from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

project_id = "global-sun-431221-s9"
subscription_id = "scrape"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    try:
        print(f"Received message: {message.data}")
        message.ack()
    except Exception as e:
        print(f"Error processing message: {e}")

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
