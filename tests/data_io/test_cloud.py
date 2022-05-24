import boto3
from tamr_toolbox.data_io import cloud
import pytest
import google
import botocore
import tempfile
import tarfile
from unittest.mock import patch


def test_gcs_download():
    @patch("google.cloud.client.Client")
    def test_download(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message . . . ")

            bucket = client.get_bucket("test-bucket-a")
            blob = bucket.blob("path_in_bucket.txt")
            blob.upload_from_filename(upload_tmp.name)

            cloud.gcs_download(
                cloud_client=client,
                source_filepath="path_in_bucket.txt",
                destination_filepath=download_tmp.name,
                bucket_name="test-bucket-a",
                untar=False,
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

        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message . . . ")

        cloud.gcs_upload(
            cloud_client=client,
            source_filepath=upload_tmp.name,
            destination_filepath="path_in_bucket.txt",
            tar_file=False,
        )

    @patch("google.cloud.client.Client")
    def test_gcs_upload_tarring(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile(
            suffix=".tar.gz"
        ) as upload_tmp, tempfile.NamedTemporaryFile(
            "wb", suffix=".tar.gz", delete=False
        ) as download_tmp:

            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message . . . ")

            cloud.gcs_upload(
                source_filepath=upload_tmp.name,
                bucket_name="bucket-a",
                cloud_client=client,
                destination_filepath="test_file.tar",
                tar_file=True,
                return_uploaded_file=download_tmp.name,
            )

            # with open(download_tmp.name, "r") as samp:
            assert tarfile.is_tarfile(download_tmp.name)

    test_upload()
    test_gcs_upload_tarring()

    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = google.cloud.client.Client()
        cloud.gcs_upload(cloud_client=cloud_client)


def test_s3_download():
    @patch("boto3.session.Session")
    def test_download(session):
        s3_client = session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message . . . ")

            s3_client.upload_file(
                Filename=upload_tmp.name, Bucket=bucket_name, Key="test_file.txt",
            )

            cloud.s3_download(
                source_filepath="test_file.txt",
                destination_filepath=download_tmp.name,
                cloud_client=s3_client,
                bucket_name=bucket_name,
            )

    test_download()

    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError)):
        cloud.s3_upload(cloud_client=boto3.client("s3"), tar_file=False)


def test_s3_upload():
    @patch("botocore.session.Session")
    def test_upload(session):

        s3_client = session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as tmp:
            with open(tmp.name, "w") as open_tmp:
                open_tmp.write("ltest message . . . ")

            cloud.s3_upload(
                source_filepath=tmp.name,
                bucket_name=bucket_name,
                cloud_client=s3_client,
                destination_filepath="test_file.txt",
                tar_file=False,
            )

    @patch("boto3.session.Session")
    def test_tar(session):
        s3_client = session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message . . . ")

            cloud.s3_upload(
                source_filepath=upload_tmp.name,
                bucket_name=bucket_name,
                cloud_client=s3_client,
                destination_filepath="test_file.tar",
                tar_file=True,
                return_uploaded_file=download_tmp.name,
            )

            assert tarfile.is_tarfile(download_tmp.name)

    test_upload()
    test_tar()

    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError)):
        cloud.s3_upload(cloud_client=boto3.client("s3"), tar_file=False)
