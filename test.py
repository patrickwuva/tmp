import boto3
import json
from get_images import get_images

sqs = boto3.resource('sqs', region_name='us-east-1')

def send_ack():
    queue = sqs.get_queue_by_name(QueueName='ack')
    response = queue.send_message(MessageBody='done')

def get_images():
    tasks = sqs.get_queue_by_name(QueueName='tasks')

    messages = tasks.receive_messages(
        MaxNumberOfMessages=1,
        WaitTimeSeconds=2
    )

    if messages:
        for m in messages:
            links = json.loads(m.body())
            try:
                get_images(links)
                send_ack()
                m.delete()
            except Exception as e:
                print(f'error: {e}')

while True:
    get_images()

