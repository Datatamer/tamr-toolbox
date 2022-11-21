import os

# Building our documentation requires access to all dependencies, including optional ones
# This environments variable is set automatically when `invoke docs` is used
BUILDING_DOCS = os.environ.get("TAMR_TOOLBOX_DOCS") == "1"
if BUILDING_DOCS:
    # Import relevant optional dependencies
    from google.cloud import storage
    from mypy_boto3_s3.client import S3Client


def gcs_upload(
    cloud_client: "storage.Client",
    *,
    source_filepath: str,
    destination_filepath: str,
    bucket_name: str,
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
    cloud_client: "storage.Client",
    *,
    source_filepath: str,
    destination_filepath: str,
    bucket_name: str,
):
    """Download data to a Google storage bucket
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
    cloud_client: "S3Client", *, source_filepath: str, destination_filepath: str, bucket_name: str
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
    cloud_client: "S3Client", *, source_filepath: str, destination_filepath: str, bucket_name: str
):
    """Download data from an Amazon AWS bucket
    Args:
        source_filepath: Path to file being downloaded on AWS
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: AWS client with user credentials
        bucket_name: name of AWS bucket being downloaded from
    """
    cloud_client.download_file(
        Bucket=bucket_name, Key=source_filepath, Filename=destination_filepath
    )
