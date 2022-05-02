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
from unittest.mock import patch


def test_gcs_download():
    @patch("google.cloud.client.Client")
    def test_download(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message, blah blah . . .")

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

    @patch("google.cloud.client.Client")
    def test_untar(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as tarred_file, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message, blah blah . . .")

            bucket = client.get_bucket("test-bucket-a")
            blob = bucket.blob("path_in_bucket.tar")

            with tarfile.open(tarred_file.name, "w") as tar:
                tar.add(upload_tmp.name)

            blob.upload_from_filename(tempfile)

            cloud.gcs_download(
                cloud_client=client,
                source_filepath="path_in_bucket.tar",
                destination_filepath=download_tmp.name,
                bucket_name="test-bucket-a",
                untar=True,
            )
            assert not tarfile.is_tarfile(download_tmp.name)

    test_download()
    test_untar()

    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = Client()
        cloud.gcs_download(cloud_client=cloud_client)


def test_gcs_upload():
    @patch("google.cloud.client.Client")
    def test_upload(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile() as upload_tmp, tempfile.NamedTemporaryFile() as download_tmp:
            with open(upload_tmp.name, "w") as open_tmp:
                open_tmp.write("test message, blah blah . . .")

        cloud.gcs_upload(
            cloud_client=client,
            source_filepath=upload_tmp.name,
            destination_filepath="path_in_bucket.txt",
            tar_file=False,
        )

    @patch("google.cloud.client.Client")
    def test_gcs_upload_tarring(mock_client):
        client = mock_client()

        temp_dir = tempfile.mkdtemp()
        upload_tmp = os.path.join(temp_dir, "tmp_upload")
        download_tmp = os.path.join(temp_dir, "tmp_download")

        with open(upload_tmp, "w") as open_tmp:
            open_tmp.write("test message, blah blah . . .")

        cloud.gcs_upload(
            source_filepath=upload_tmp,
            bucket_name="bucket-a",
            cloud_client=client,
            destination_filepath="test_file.tar",
            tar_file=True,
        )
        bucket = client.get_bucket("bucket-a")
        blob = bucket.blob("test_file.tar")
        blob.download_to_filename(download_tmp)

        assert tarfile.is_tarfile(download_tmp)

    test_upload()
    test_gcs_upload_tarring()

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
            bucket = s3_client.get_bucket(bucket_name)
            blob = bucket.blob("test_file.tar")
            blob.download_to_filename(download_tmp.name)

            assert tarfile.is_tarfile(download_tmp.name)

    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError)):
        cloud.s3_upload(cloud_client=boto3.client("s3"), tar_file=False)


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
        cloud.s3_upload(cloud_client=boto3.client("s3"), tar_file=False)
