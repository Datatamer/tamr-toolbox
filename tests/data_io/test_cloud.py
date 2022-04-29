import boto3
from tamr_toolbox.data_io import cloud
from google.cloud.client import Client
import pytest
import google
import botocore
import tempfile
import botocore.session
from moto import mock_s3
import tarfile
import os


def test_gcs_download():
    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = Client()
        cloud.gcs_download(cloud_client=cloud_client)


def test_gcs_upload():
    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = Client()
        cloud.gcs_upload(cloud_client=cloud_client)


def test_s3_upload():
    # should pass correctly - test
    @mock_s3
    def test_upload():
        s3_session = boto3.session.Session()
        s3_client = s3_session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as tmp:
            with open(tmp.name, "w") as open_tmp:
                open_tmp.write("test message, blah blah . . .")

            cloud.s3_upload(
                source_filepath=tmp.name,
                bucket_name=bucket_name,
                cloud_client=s3_client,
                destination_filepath="test_file.txt",
                tar_file=False,
            )

    @mock_s3
    def test_tar():
        s3_session = boto3.session.Session()
        s3_client = s3_session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message, blah blah . . .")

            cloud.s3_upload(
                source_filepath=upload_tmp.name,
                bucket_name=bucket_name,
                cloud_client=s3_client,
                destination_filepath="test_file.tar",
                tar_file=True,
            )
            # blob = s3_client.get_bucket(bucket_name).blob(tmp.name)
            bucket = s3_client.get_bucket(bucket_name)
            blob = bucket.blob("test_file.tar")
            blob.download_to_filename(download_tmp.name)

            assert tarfile.is_tarfile(download_tmp.name)

    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError)):
        cloud.s3_upload(tar_file=False)


def test_s3_download():
    @mock_s3
    def test_download():
        s3_session = boto3.session.Session()
        s3_client = s3_session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message, blah blah . . .")

            s3_client.upload_file(
                Filename=upload_tmp.name, Bucket=bucket_name, Key="test_file.txt",
            )

            cloud.s3_download(
                source_filepath="test_file.txt",
                destination_filepath=download_tmp.name,
                cloud_client=s3_client,
                bucket_name=bucket_name,
            )
            with open(download_tmp.name, "rb") as open_tmp:
                assert open_tmp.read() == "test message, blah blah . . ."

    @mock_s3
    def test_tar():
        s3_session = boto3.session.Session()
        s3_client = s3_session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp, tempfile.NamedTemporaryFile() as upload_tar_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message, blah blah . . .")
            with tarfile.open(upload_tar_tmp) as tar:
                tar.add(upload_tmp, arcname=os.path.basename(upload_tmp))

            s3_client.upload_file(
                Filename=upload_tmp.name, Bucket=bucket_name, Key="test_file.tar",
            )

            cloud.s3_download(
                source_filepath="test_file.tar",
                destination_filepath=download_tmp.name,
                cloud_client=s3_client,
                bucket_name=bucket_name,
                untar=True,
            )
            with open(download_tmp.name, "rb") as open_tmp:
                assert open_tmp.read() == "test message, blah blah . . ."

    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError)):
        cloud.s3_upload(tar_file=False)
