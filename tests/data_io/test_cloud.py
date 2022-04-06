from tamr_toolbox.data_io import cloud
from google.cloud.client import Client
import pytest
import google
import botocore


def test_google_download():
    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = Client()
        cloud.google_download(cloud_client=cloud_client)


def test_google_upload():
    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = Client()
        cloud.google_upload(cloud_client=cloud_client)


def test_s3_upload():
    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError)):
        cloud.s3_upload(compress=False)


def test_s3_download():
    with pytest.raises(botocore.exceptions.NoCredentialsError):
        cloud.s3_download()


# cloud.file_upload()
# cloud.file_download()
