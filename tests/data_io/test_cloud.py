import boto3
from tamr_toolbox.data_io import cloud
import pytest
import google
import botocore
import tempfile
import tarfile
from unittest.mock import patch


def test_gcs_download():
    class Mock_blob:
        def __init__(self, bucket_name):
            pass

        def blob(self, bucket_name):
            return Mock_blob(None)

        def download_to_filename(self, file_path):
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:
                tmp.write("test message . . . ")
                with tarfile.open(file_path, "w") as tar:
                    tar.add(tmp.name)

    class Mock_client:
        def get_bucket(self, bucket_name):
            return Mock_blob(bucket_name)

    def test_untarring_false():
        client = Mock_client()
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:

            cloud.gcs_download(
                cloud_client=client,
                source_filepath="path_in_bucket.txt",
                destination_filepath=tmp.name,
                bucket_name="test-bucket-a",
                untar=False,
            )
            assert tarfile.is_tarfile(tmp.name)

    def test_untarring_true():
        client = Mock_client()
        with pytest.raises((NotADirectoryError, FileNotFoundError)):
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:
                cloud.gcs_download(
                    cloud_client=client,
                    source_filepath="path_in_bucket.txt",
                    destination_filepath=tmp.name,
                    bucket_name="test-bucket-a",
                    untar=True,
                )

    @patch("google.cloud.client.Client")
    def test_download(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile() as download_tmp:
            cloud.gcs_download(
                cloud_client=client,
                source_filepath="path_in_bucket.txt",
                destination_filepath=download_tmp.name,
                bucket_name="test-bucket-a",
                untar=False,
            )

    test_untarring_false()
    test_untarring_true()
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
                tar_file=False,
            )

    @patch("google.cloud.client.Client")
    def test_gcs_upload_tarring(mock_client):
        client = mock_client()

        with tempfile.NamedTemporaryFile(
            suffix=".tar.gz", delete=False
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

            assert tarfile.is_tarfile(download_tmp.name)

    test_upload()
    test_gcs_upload_tarring()

    # checks to see google download checks for credentials
    with pytest.raises(google.auth.exceptions.DefaultCredentialsError):
        cloud_client = google.cloud.client.Client()
        cloud.gcs_upload(cloud_client=cloud_client)


def test_s3_download():
    class Mock_client:
        def download_file(self, Filename, Bucket, Key):
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:
                tmp.write("test message . . . ")
                with tarfile.open(Filename, "w") as tar:
                    tar.add(tmp.name)

    def test_untarring_false():
        client = Mock_client()
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:

            cloud.s3_download(
                cloud_client=client,
                source_filepath="test_file.txt",
                destination_filepath=tmp.name,
                bucket_name="bucket",
            )
            assert tarfile.is_tarfile(tmp.name)

    def test_untarring_true():
        client = Mock_client()
        with pytest.raises((NotADirectoryError, FileNotFoundError,)):
            with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:
                cloud.s3_download(
                    cloud_client=client,
                    source_filepath="test_file.txt",
                    destination_filepath=tmp.name,
                    bucket_name="bucket",
                    untar=True,
                )

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
    test_untarring_false()
    test_untarring_true()

    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError,)):
        cloud.s3_upload(
            cloud_client=boto3.client("s3"), tar_file=False,
        )


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
                tar_file=False,
            )

    @patch("boto3.session.Session")
    def test_tar(session):
        s3_client = session.client("s3")
        bucket_name = "my_bucket"

        s3_client.create_bucket(Bucket=bucket_name)

        # create tmp file to upload to mock s3
        with tempfile.NamedTemporaryFile(
            delete=False, mode="w"
        ) as upload_tmp, tempfile.NamedTemporaryFile(delete=False, mode="w") as download_tmp:
            upload_tmp.write("test message . . . ")

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

    with pytest.raises((botocore.exceptions.NoCredentialsError, FileNotFoundError,)):
        cloud.s3_upload(
            cloud_client=boto3.client("s3"), tar_file=False,
        )
