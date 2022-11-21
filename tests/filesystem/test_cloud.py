from tamr_toolbox.filesystem import cloud
import tempfile
from unittest.mock import patch


@patch("google.cloud.client.Client")
def test_gcs_download(mock_client):
    client = mock_client()

    with tempfile.NamedTemporaryFile() as download_tmp:
        cloud.gcs_download(
            cloud_client=client,
            source_filepath="path_in_bucket.txt",
            destination_filepath=download_tmp.name,
            bucket_name="test-bucket",
        )

    bucket = client.get_bucket
    bucket.assert_called_with("test-bucket")
    blob = bucket().blob
    blob.assert_called_with("path_in_bucket.txt")
    download = blob().download_to_filename
    download.assert_called_with(download_tmp.name)


@patch("google.cloud.client.Client")
def test_gcs_upload(mock_client):
    client = mock_client()

    with tempfile.NamedTemporaryFile(delete=False, mode="w") as upload_tmp:
        upload_tmp.write("test message . . . ")

        cloud.gcs_upload(
            cloud_client=client,
            source_filepath=upload_tmp.name,
            destination_filepath="path_in_bucket.txt",
            bucket_name="test-bucket",
        )

        bucket = client.get_bucket
        bucket.assert_called_with("test-bucket")
        blob = bucket().blob
        blob.assert_called_with("path_in_bucket.txt")
        upload = blob().upload_from_filename
        upload.assert_called_with(upload_tmp.name)


@patch("boto3.session.Session")
def test_s3_download(session):
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

        download = s3_client.download_file
        download.assert_called_with(
            Bucket=bucket_name, Key="test_file.txt", Filename=download_tmp.name
        )


@patch("botocore.session.Session")
def test_s3_upload(session):
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
        upload = s3_client.upload_file
        upload.assert_called_with(Filename=tmp.name, Bucket=bucket_name, Key="test_file.txt")
