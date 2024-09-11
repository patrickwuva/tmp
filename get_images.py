from google.cloud import storage
import requests
import io
import json
from based import based
import os
import glob
import threading
import requests
#from deepface import DeepFace
import re
import imghdr
import time
def upload_json(bucket_name, data, destination):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_string(json.dumps(data), content_type='application/json')
    print('json uploaded')

def upload(bucket_name, local_file, destination):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(local_file)
    print(f'Image uploaded ')

def download(bucket_name, blob_name, file):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(file)

def download_image(link):
    os.makedirs('images', exist_ok=True)

    #offender_id = link.split('sid=')[-1]
    #offender_id = os.path.basename(link)
    offender_id = link.split('/')[-1]
    username = 'spxwhjvleu'
    password = 'Bydk9qPurElL5_3q1v'
    proxy = f"http://{username}:{password}@gate.smartproxy.com:7000"
    response = requests.get(link, proxies = proxy)
    if response.status_code == 200:
        image_type = imghdr.what(None, h=response.contnet)
        if image_type:
            with open(f'images/{offender_id}.{image_type}', 'wb') as file:
                file.write(response.content)
    else:
        time.sleep(2)
        download_image(link)
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
    new_embeddings = {}
    for file in glob.glob('images/*'):
        try:
            upload('offender-images',file, f'{file}')
            os.remove(file)
        except Exception as e:
            print(f'error processing {file}: {e}')
def main():
    db = based()
    db.connect()
    links = db.get_image_links('VA')
    get_images(links)


if __name__ == '__main__':
    main()