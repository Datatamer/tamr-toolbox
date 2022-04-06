from boto3 import client
from tarfile import is_tarfile
import tempfile
import os
import tarfile

# Google examples


def google_upload(
    cloud_client,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_on_google_bucket",
    bucket_name="my_google_bucket",
    compress=True,
):
    """Upload data to a google storage bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to google bucket
        cloud_client: google storage client with user credentials
        bucket_name: name of google bucket
        compress: Tar file before upload
    """
    bucket = cloud_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_filepath)

    if compress and not is_tarfile(source_filepath):
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, "temp_file")
        with tarfile.open(temp_path) as tar:
            tar.add(source_filepath, arcname=os.path.basename(source_filepath))

        blob.upload_from_filename(tempfile)

    else:
        blob.upload_from_filename(source_filepath)


def google_download(
    cloud_client,
    source_filepath="path_to_my_file_on_google_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="my_google_bucket",
    decompress=False,
):
    """ Download data to a google storage bucket
    Args:
        source_filepath: Path to file being downloaded on google
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: google storage client with user credentials
        bucket_name: name of google bucket being downloaded from
        decompress: Whether to decompress file being downloaded (Tar)
    """

    bucket = cloud_client.get_bucket(bucket_name)
    blob = bucket.blob(source_filepath)

    if decompress:
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, "temp_file")
        blob.download_to_filename(temp_path)
        with open(temp_path) as file:
            if is_tarfile(file):
                with tarfile.open(temp_path) as tar_file:
                    tar_file.extractall(destination_filepath)
    else:
        blob.download_to_filename(destination_filepath)


# Amazon S3 examples
amazon_storage_client = client("s3")


def s3_upload(
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_on_s3_bucket",
    cloud_client=amazon_storage_client,
    bucket_name="my_google_bucket",
    compress=True,
):
    """Upload data to Amazon AWS bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to AWS bucket
        cloud_client: AWS client with user credentials
        bucket_name: name of AWS bucket
        compress: Tar file before upload
    """
    if compress and not is_tarfile(source_filepath):
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, "temp_file")
        with tarfile.open(temp_path) as tar:
            tar.add(source_filepath, arcname=os.path.basename(source_filepath))

        cloud_client.upload_file(
            Filename=temp_path, Bucket=bucket_name, Key=destination_filepath,
        )
    else:
        cloud_client.upload_file(
            Filename=source_filepath, Bucket=bucket_name, Key=destination_filepath,
        )


def s3_download(
    source_filepath="path_to_my_file_on_s3_bucket",
    destination_filepath="path_to_my_local_file",
    cloud_client=amazon_storage_client,
    bucket_name="my_google_bucket",
    decompress=False,
):
    """ Download data from an Amazon AWS bucket
    Args:
        source_filepath: Path to file being downloaded on AWS
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: AWS client with user credentials
        bucket_name: name of AWS bucket being downloaded from
        decompress: Whether to decompress file being downloaded (Tar)
    """

    if decompress:
        temp_dir = tempfile.gettempdir()
        temp_path = os.path.join(temp_dir, "temp_file")
        cloud_client.download_file(
            Bucket=bucket_name, Key=source_filepath, Filename=temp_path,
        )
        with open(temp_path) as file:
            if is_tarfile(file):
                with tarfile.open(temp_path) as tar_file:
                    tar_file.extractall(destination_filepath)
    else:
        cloud_client.download_file(
            Bucket=bucket_name, Key=source_filepath, Filename=destination_filepath,
        )


def file_upload(
    cloud_client,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_on_google_bucket",
    bucket_name="my_google_bucket",
    compress=True,
):
    """Function run to upload file to Google Bucket or Amazon AWS bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to bucket
        cloud_client: client with user credentials, used to select client type
        bucket_name: name of bucket
        compress: Tar file before upload
    """
    # if cloud_client starts w/ google -> google_upload func >=> else -> S3_upload
    cloud_client_name = str(cloud_client)
    if cloud_client_name.startswith("google_storage_client"):
        google_upload(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            compress=compress,
        )
    if cloud_client_name.startswith("amazon_storage_client"):
        s3_upload(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            compress=compress,
        )


def file_download(
    cloud_client,
    source_filepath="path_to_my_file_on_s3_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="my_google_bucket",
    decompress=True,
):
    """Function selection when downloading file from Google Bucket or Amazon AWS bucket
    Args:
        source_filepath: filepath being downloaded from
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: client with user credentials used for client type
        bucket_name: name of bucket being downloaded from
        decompress: Whether to decompress file being downloaded (Tar)
    """
    # if cloud_client starts w/ google -> google_download func >=> else -> S3_download
    cloud_client_name = str(cloud_client)

    if cloud_client_name.startswith("google_storage_client"):
        google_download(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            cloud_client=cloud_client,
            decompress=decompress,
        )
    if cloud_client_name.startswith("amazon_storage_client"):
        s3_download(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            cloud_client=cloud_client,
            decompress=decompress,
        )
