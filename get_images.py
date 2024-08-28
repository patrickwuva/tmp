from google.cloud import storage
import requests
import io
from based import based
import os
import glob
import threading
import requests

def upload(bucket_name, file_name, destination):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(file_name)
    print(f'Image uploaded to {blob_name}')


def download_image(link):
    os.makedirs('images', exist_ok=True)

    offender_id = link.split('srn=')[-1]

    response = requests.get(link)
    if response.status_code == 200:
        with open(f'images/{offender_id}.jpg', 'wb') as file:
            file.write(response.content)
    else:
        print(f'Failed to download {link}, status code: {response.status_code}')

def get_images(links):
    threads = []
    for link in links:
        thread = threading.Thread(target=download_image, args=(link,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def process_images():
    for file in glob.glob('images/*'):
        upload('offender-images',file, f'images/{file}')
        os.remove(file)

