import os
import logging
import json
import base64
import tempfile
import requests

import config

from google.cloud import storage, kms_v1, secretmanager_v1
from google.api_core.exceptions import ServiceUnavailable

from requests_oauthlib import OAuth1
from retry import retry

client = storage.Client()


def get_authentication_secret():
    authentication_secret_encrypted = base64.b64decode(os.environ['AUTHENTICATION_SECRET_ENCRYPTED'])
    kms_client = kms_v1.KeyManagementServiceClient()
    crypto_key_name = kms_client.crypto_key_path_path(os.environ['PROJECT_ID'], os.environ['KMS_REGION'], os.environ['KMS_KEYRING'],
                                                      os.environ['KMS_KEY'])
    decrypt_response = kms_client.decrypt(crypto_key_name, authentication_secret_encrypted)
    return decrypt_response.plaintext.decode("utf-8").replace('\n', '')


def get_secret():
    secret_manager_client = secretmanager_v1.SecretManagerServiceClient()

    secret_name = secret_manager_client.secret_version_path(
        os.environ['PROJECT_ID'],
        os.environ['SECRET_NAME'],
        'latest')

    response = secret_manager_client.access_secret_version(secret_name)
    payload = response.payload.data.decode('UTF-8')

    return payload


def get_data_from_store(bucket_name, source):
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(source)
    content = blob.download_as_string().decode("utf-8")
    try:
        data = json.loads(content)
        if data.get('elements', []):
            return {
                'survey': data['elements'][0]['info']['formId'], 'registrations': [{
                    'registration': single['meta']['serialNumber'],
                    'attachments': parse_survey(single['data'], [])
                } for single in data['elements']]
            }
        else:
            return {}
    except Exception:
        logging.exception(f"Failure processing survey {source}, skipped.")


@retry(ServiceUnavailable, tries=3, delay=2)
def download_photo_if_absent(form, registration, images):
    bucket = client.get_bucket(config.GOOGLE_STORAGE_BUCKET)
    for image in images:
        photo_name = f"{config.PHOTO_PATH}/{form}/{registration}/{image}"
        blob = storage.Blob(bucket=bucket, name=photo_name)
        if blob.exists(client):
            logging.info(f"image {photo_name} already downloaded, skip")
        else:
            store_photo(blob, image)
    pass


def store_photo(blob, image):
    """
    Store foto as a png
    :param blob:
    :param image:
    """
    logging.info(f"downloading image {image}")
    consumer_secret = get_secret()
    consumer_key = config.CONSUMER_KEY
    oauth_1 = OAuth1(
        consumer_key,
        consumer_secret,
        signature_method='HMAC-SHA1'
    )

    data_response = requests.get(
        f"{config.MORE_APP_DOWNLOAD_URL_PREFIX}{image}{config.MORE_APP_DOWNLOAD_URL_SUFFIX}",
        auth=oauth_1,
        headers={}
    )

    content_type = data_response.headers['content-type'].split(';')[0]
    image_format = content_type.split('/')[1]

    registration_image = f'{tempfile.gettempdir()}/{image}.{image_format}'
    image = open(registration_image, 'w+b')
    image.write(bytearray(data_response.content))
    image.close()
    blob.upload_from_filename(registration_image, content_type=content_type)


def parse_survey(content, images):
    if type(content).__name__ == 'list':
        for element in content:
            parse_survey(element, images)
    elif type(content).__name__ == 'dict':
        for element in content:
            parse_survey(content[element], images)
    elif type(content).__name__ == 'str' and content.startswith("gridfs://registrationFiles/"):
        images.append(content.split('/')[-1])
    return images


def process_survey_attachments(data, context):
    bucket = data['bucket']
    source = data['name']
    logging.info(f'New Blob: {source}')

    if not source.startswith('source'):
        logging.info(f'Skipping {source}, already processed')
        return

    prefix = "/".join(source.split("/")[:3])
    previous_source = list(client.list_blobs(bucket, prefix=prefix))[-2].name
    previous_refs = get_data_from_store(bucket, previous_source)

    already_downloaded_attachments = {}

    logging.info(f"source {previous_source}, refs: {len(previous_refs)}")

    for ref in previous_refs.get('registrations', []):
        already_downloaded_attachments[(previous_refs['survey'], ref['registration'])] = ref['attachments']

    refs = get_data_from_store(bucket, source)
    if refs:
        for ref in refs['registrations']:
            if already_downloaded_attachments.get((refs['survey'], ref['registration']), False) == ref['attachments']:
                logging.info(
                    f"survey {refs['survey']} registration id: {ref['registration']} already processed")
            else:
                download_photo_if_absent(refs['survey'], ref['registration'], ref['attachments'])
                logging.info(
                    f"survey {refs['survey']} registration id: {ref['registration']}, attachments {ref['attachments']}")
