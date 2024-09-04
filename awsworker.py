import boto3
import json
from get_images import get_images

sqs = boto3.resource('sqs', region_name='us-east-1')

def send_ack():
    ack_queue = sqs.get_queue_by_name(QueueName='ack')
    response = ack_queue.send_message(MessageBody='done')

def process_messages():
    tasks_queue = sqs.get_queue_by_name(QueueName='tasks')
    
    # Counter to track how many times we've waited without receiving messages
    empty_poll_count = 0
    max_empty_polls = 3  # Define how many empty polls to allow before exiting

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
                    # Reset the empty poll counter on receiving a message
                    empty_poll_count = 0
                    
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
            empty_poll_count += 1

        # Exit the loop if we have waited for messages multiple times without receiving any
        if empty_poll_count >= max_empty_polls:
            print(f"No messages received after {max_empty_polls} attempts. Exiting.")
            break

if __name__ == '__main__':
    process_messages()

