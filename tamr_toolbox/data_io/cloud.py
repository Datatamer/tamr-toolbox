from google.cloud.client import Client
from boto3 import client
import tamr_toolbox as tbox


# Google examples
google_storage_client = Client()


def google_upload(
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_on_google_bucket",
    cloud_client=google_storage_client,
    bucket_name="my_google_bucket",
    compress=True,
):
    """ Upload data to a google storage bucket"""

    storage_client = cloud_client.Client.from_service_account_json(
        "credentials"
    )  # needs credentials
    bucket = storage_client.get_bucket(bucket_name)
    destination_filepath.upload_from_filename(source_filepath)

    upload_path = bucket.blob(bucket_name)


def google_download(
    source_filepath="path_to_my_file_on_google_bucket",
    destination_filepath="path_to_my_local_file",
    cloud_client=google_storage_client,
    bucket_name="my_google_bucket",
):
    """ Download data to a google storage bucket"""

    storage_client = cloud_client.Client.from_service_account_json(
        "credentials"
    )  # needs credentials
    bucket = cloud_client.get_bucket(bucket_name)
    blob = bucket.blob(source_filepath)
    blob.download_to_filename(destination_filepath)


# Amazon S3 examples
amazon_storage_client = client("s3")


def s3_upload(
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_on_s3_bucket",
    cloud_client=amazon_storage_client,
    bucket_name="my_google_bucket",
):
    cloud_client.upload_file(
        Filename=source_filepath, Bucket=bucket_name, Key=destination_filepath,
    )


def s3_download(
    source_filepath="path_to_my_file_on_s3_bucket",
    destination_filepath="path_to_my_local_file",
    cloud_client=amazon_storage_client,
    bucket_name="my_google_bucket",
    decompress=True,
):
    cloud_client.download_file(
        Bucket=bucket_name, Key=source_filepath, Filename=destination_filepath,
    )


# Todo:
#   un/tar files
#   add de/compression
#   fix credentials
