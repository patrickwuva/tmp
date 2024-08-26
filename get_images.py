from google.cloud import storage
import requests
import io
from based import based
import os
import glob

def upload(bucket_name, file_name, destination):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(file_name)
    print(f'Image uploaded to {blob_name}')

def get_images(links):
    for link in links:
        name = link.split('/')[-1]
        response = requests.get(link)
        if response.status_code == 200:
            with open(f'images/{name}', 'wb') as file:
                file.write(response.content)
        else:
            print(f'failed to download {link} statuscode: {response.status_code}')

def process_images():
    for file in glob.glob('images/*'):
        upload('offender-images',file, f'images/{file}')
        os.remove(file)

