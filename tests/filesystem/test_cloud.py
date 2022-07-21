import boto3
from tamr_toolbox.filesystem import cloud
import pytest
import google
import botocore
import tempfile
from unittest.mock import patch


def test_gcs_download():
    @patch("google.cloud.client.Client")
    def test_download(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile() as download_tmp:
            cloud.gcs_download(
                cloud_client=client,
                source_filepath="path_in_bucket.txt",
                destination_filepath=download_tmp.name,
                bucket_name="test-bucket",
            )

    test_download()

    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = google.cloud.client.Client()
        cloud.gcs_download(cloud_client=cloud_client)


def test_gcs_upload():
    @patch("google.cloud.client.Client")
    def test_upload(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile(delete=False, mode="w") as upload_tmp:
            upload_tmp.write("test message . . . ")

            cloud.gcs_upload(
                cloud_client=client,
                source_filepath=upload_tmp.name,
                destination_filepath="path_in_bucket.txt",
                bucket_name="test-bucket",
            )

    test_upload()


def test_s3_download():
    @patch("boto3.session.Session")
    def test_download(session):
        s3_client = session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)
        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as download_tmp:
            cloud.s3_download(
                source_filepath="test_file.txt",
                destination_filepath=download_tmp.name,
                cloud_client=s3_client,
                bucket_name=bucket_name,
            )

    test_download()


def test_s3_upload():
    @patch("botocore.session.Session")
    def test_upload(session):

        s3_client = session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            with open(tmp.name, "w") as open_tmp:
                open_tmp.write("ltest message . . . ")

            cloud.s3_upload(
                source_filepath=tmp.name,
                bucket_name=bucket_name,
                cloud_client=s3_client,
                destination_filepath="test_file.txt",
            )

    test_upload()
