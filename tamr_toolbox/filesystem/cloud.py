from google.cloud.storage import Client as GcsClient
from mypy_boto3_s3.client import S3Client

def gcs_upload(
    cloud_client: GcsClient,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_in_bucket",
    bucket_name="sample_bucket",
):
    """Upload data to a Google storage bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to google bucket
        cloud_client: google storage client with user credentials
        bucket_name: name of google bucket
    """
    bucket = cloud_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_filepath)
    blob.upload_from_filename(source_filepath)


def gcs_download(
    cloud_client: GcsClient,
    source_filepath="path_to_my_file_in_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="sample_bucket",
):
    """ Download data to a Google storage bucket
    Args:
        source_filepath: Path to file being downloaded on Google
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: google storage client with user credentials
        bucket_name: name of google bucket being downloaded from
    """

    bucket = cloud_client.get_bucket(bucket_name)
    this_blob = bucket.blob(source_filepath)
    this_blob.download_to_filename(destination_filepath)


def s3_upload(
    cloud_client: S3Client,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_in_bucket",
    bucket_name="sample_bucket",
):
    """Upload data to Amazon AWS bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to AWS bucket
        cloud_client: AWS client with user credentials
        bucket_name: name of AWS bucket
        downloads copy of file being uploaded to filepath
    """
    cloud_client.upload_file(
        Filename=source_filepath, Bucket=bucket_name, Key=destination_filepath
    )


def s3_download(
    cloud_client: S3Client,
    source_filepath="path_to_my_file_in_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="sample_bucket",
):
    """ Download data from an Amazon AWS bucket
    Args:
        source_filepath: Path to file being downloaded on AWS
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: AWS client with user credentials
        bucket_name: name of AWS bucket being downloaded from
    """
    cloud_client.download_file(
        Bucket=bucket_name, Key=source_filepath, Filename=destination_filepath,
    )


def file_upload(
    client_type,
    cloud_client,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_in_bucket",
    bucket_name="sample_bucket",
):
    """Function run to upload file to Google Bucket or Amazon AWS bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to bucket
        cloud_client: client with user credentials, used to select client type
        bucket_name: name of bucket
        downloads copy of file being uploaded to filepath
        client_type: enter name of client type ie. 's3' or 'gcs'
    """
    if client_type == "gcs":
        gcs_upload(
            cloud_client=cloud_client,
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
        )
    if client_type == "s3":
        s3_upload(
            cloud_client=cloud_client,
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
        )


def file_download(
    client_type,
    cloud_client,
    source_filepath="path_to_my_file_in_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="sample_bucket",
):
    """Function selection when downloading file from Google Bucket or Amazon AWS bucket
    Args:
        source_filepath: filepath being downloaded from
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: client with user credentials used for client type
        bucket_name: name of bucket being downloaded from
        client_type: enter name of client type ie. 's3' or 'gcs'
    """
    if client_type == "gcs":
        gcs_download(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            cloud_client=cloud_client,
        )
    if client_type == "s3":
        s3_download(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            cloud_client=cloud_client,
        )
