from google.cloud import pubsub_v1

project_id = "global-sun-431221-s9"
topic_id = "taskmaster"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

m = "Hi!"
m = m.encode("utf-8")
f = publisher.publish(topic_path, m)

print(f'{f.result()}')
