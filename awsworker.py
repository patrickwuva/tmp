import boto3
import json
from get_images import get_images
from add_offenders import insert_offenders
from get_offenders import get_offenders

sqs = boto3.resource('sqs', region_name='us-east-1')

def send_ack():
    ack_queue = sqs.get_queue_by_name(QueueName='ack')
    response = ack_queue.send_message(MessageBody='done')

def get_links():
    tasks_queue = sqs.get_queue_by_name(QueueName='tasks')
    empty_poll_count = 0
    max_empty_polls = 1

    while True:
        messages = tasks_queue.receive_messages(
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        if messages:
            for m in messages:
                try:
                    empty_poll_count = 0
                    links = json.loads(m.body)
                    print(f"Processing links: {links}")

                    get_images(links)

                    send_ack()

                    m.delete()
                    print("Message processed and deleted")
                except Exception as e:
                    print(f"Error processing message: {e}")
        else:
            print("No messages received. Waiting for new messages...")
            empty_poll_count += 1

        if empty_poll_count >= max_empty_polls:
            print(f"No messages received after {max_empty_polls} attempts. Exiting.")
            break

def get_off():
    tasks_queue = sqs.get_queue_by_name(QueueName='tasks')
    empty_poll_count = 0
    max_empty_polls = 1

    while True:
        messages = tasks_queue.receive_messages(
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )

        if messages:
            for m in messages:
                try:
                    empty_poll_count = 0
                    zipcodes = json.loads(m.body)
                    print(f"Processing offenders")

                    offenders = get_offenders(zipcodes)
                    if offenders != None:
                        insert_offenders(offenders)

                    send_ack()
                    m.delete()
                    print("Message processed and deleted")
                except Exception as e:
                    print(f"Error processing message: {e}")
        else:
            print("No messages received. Waiting for new messages...")
            empty_poll_count += 1

        if empty_poll_count >= max_empty_polls:
            print(f"No messages received after {max_empty_polls} attempts. Exiting.")
            break


if __name__ == '__main__':
    get_off()

