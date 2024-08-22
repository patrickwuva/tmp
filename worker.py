from google.cloud import pubsub_v1
from add_offenders import insert_offenders
from get_offenders import get_offenders
import json

project_id = "global-sun-431221-s9"
subscription_id = "scrape"
ack_topic_id = 'taskmaster-acks'

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
ack_publisher = pubsub_v1.PublisherClient()
ack_topic_path = ack_publisher.topic_path(project_id, ack_topic_id)

def callback(message):

    #print(f"Received message: {message.data.decode('utf-8')}")
    data_str = message.data.decode('utf-8')
    zip_codes = json.loads(data_str)
    offenders = get_offenders(zip_codes)
    if offenders != None:
        insert_offenders(offenders)
        print(f'done with {zip_codes}')
    print(f'none for zip {zip_codes}')
    ack_publisher.publish(ack_topic_path, b'',zip_codes=json.dumps(zip_codes))
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull_future.result()  # Block and listen for messages
except KeyboardInterrupt:
    streaming_pull_future.cancel()
