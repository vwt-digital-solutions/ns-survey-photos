import os
import logging
import json
import base64
import requests
import datetime
import config
from google.cloud import storage, kms_v1
from requests_oauthlib import OAuth1


def get_authentication_secret():
    authentication_secret_encrypted = base64.b64decode(os.environ['AUTHENTICATION_SECRET_ENCRYPTED'])
    kms_client = kms_v1.KeyManagementServiceClient()
    crypto_key_name = kms_client.crypto_key_path_path(os.environ['PROJECT_ID'], os.environ['KMS_REGION'], os.environ['KMS_KEYRING'],
                                                      os.environ['KMS_KEY'])
    decrypt_response = kms_client.decrypt(crypto_key_name, authentication_secret_encrypted)
    return decrypt_response.plaintext.decode("utf-8").replace('\n', '')


def get_data_from_store(bucket_name, source):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(source)
    content = blob.download_as_string().decode("utf-8")
    try:
        data = json.loads(content)
        if data['elements']:
            return {
                'survey': data['elements'][0]['info']['formId'], 'registrations': [{
                    'registration': single['meta']['serialNumber'],
                    'attachments': parse_survey(single['data'], [])
                } for single in data['elements']]
            }
    except Exception as e:
        logging.error(f"Failure processing survey, skip. Reason {e}")


def download_photo_if_absent(form, registration, photos):
    client = storage.Client()
    bucket = client.get_bucket(config.GOOGLE_STORAGE_BUCKET)
    for photo in photos:
        photo_name = f"{config.PHOTO_PATH}/{form}/{registration}/{photo}"
        blob = storage.Blob(bucket=bucket, name=photo_name)
        status = blob.exists(client)
        if status:
            logging.info(f"photo {photo_name} already downloaded, skip")
        else:
            store_photo(blob, photo)
    pass


def store_photo(blob, photo):
    logging.info(f"downloading photo {photo}")
    consumer_secret = get_authentication_secret()
    consumer_key = config.CONSUMER_KEY
    oauth_1 = OAuth1(
        consumer_key,
        consumer_secret,
        signature_method='HMAC-SHA1'
    )

    data_response = requests.get(
        f"{config.MORE_APP_DOWNLOAD_URL_PREFIX}{photo}{config.MORE_APP_DOWNLOAD_URL_SUFFIX}",
        auth=oauth_1,
        headers={}
    )

    b64_data = base64.b64encode(data_response.content)
    # logging.info(f"downloaded photo {b64_data}")
    blob.upload_from_string(data=b64_data, content_type="text/plain")


def parse_survey(content, photos):
    if type(content).__name__ == 'list':
        for element in content:
            parse_survey(element, photos)
    elif type(content).__name__ == 'dict':
        for element in content:
            parse_survey(content[element], photos)
    elif type(content).__name__ == 'str' and content.startswith("gridfs://registrationFiles/"):
        photos.append(content.split('/')[-1])
    return photos


def process_survey_attachments(data, context):
    bucket = data['bucket']
    source = data['name']

    if source.startswith('source'):
        refs = get_data_from_store(bucket, source)
        if refs:
            for ref in refs['registrations']:
                download_photo_if_absent(refs['survey'], ref['registration'], ref['attachments'])
                logging.info(
                    f"survey {refs['survey']} registration id: {ref['registration']}, attachments {ref['attachments']}")
