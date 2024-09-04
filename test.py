import boto3
import json
from get_images import get_images

sqs = boto3.resource('sqs', region_name='us-east-1')

def send_ack():
    ack_queue = sqs.get_queue_by_name(QueueName='ack')
    response = ack_queue.send_message(MessageBody='done')

def process_messages():
    tasks_queue = sqs.get_queue_by_name(QueueName='tasks')

    # Infinite loop to continuously poll the queue for messages
    while True:
        # Receive one message at a time with long polling
        messages = tasks_queue.receive_messages(
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10  # Use long polling to reduce unnecessary API calls
        )

        if messages:
            for m in messages:
                try:
                    # Load the list of links from the message body
                    links = json.loads(m.body)
                    print(f"Processing links: {links}")
                    
                    # Process the images (assuming get_images handles the links)
                    get_images(links)
                    
                    # Acknowledge the completion of the task
                    send_ack()
                    
                    # Delete the message from the queue after successful processing
                    m.delete()
                    print("Message processed and deleted")
                except Exception as e:
                    print(f"Error processing message: {e}")
        else:
            print("No messages received. Waiting for new messages...")

if __name__ == '__main__':
    process_messages()
