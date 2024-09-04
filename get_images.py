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
    match = re.search(r"Offender/(\d+)/avatar", link)
    
    offender_id = match.group(1) 
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

"""
def get_embedding(path):
    return DeepFace.represent(img_path=path, model_name="Facenet", enforce_detection=False)[0]['embedding']

def load_embeddings(bucket_name, blob_name, local_file):
    download(bucket_name, blob_name, local_file)
    with open(local_file, 'r') as f:
        embeddings = json.load(f)
    return embeddings

def update_embeddings(embeddings, new_embeddings):
    embeddings.update(new_embeddings)
    return embeddings
"""
def process_images():
    new_embeddings = {}
    for file in glob.glob('images/*'):
        try:
            upload('offender-images',file, f'{file}')
            os.remove(file)
        except Exception as e:
            print(f'error processing {file}: {e}')

