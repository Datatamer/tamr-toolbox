import tempfile
import tarfile


def gcs_upload(
    cloud_client,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_in_bucket",
    bucket_name="sample_bucket",
    tar_file=True,
    return_uploaded_file=None,
):
    """Upload data to a Google storage bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to google bucket
        cloud_client: google storage client with user credentials
        bucket_name: name of google bucket
        tar_file: Tar file before upload
        return_uploaded_file: If filepath is given,
        downloads copy of file being uploaded to filepath


    """
    bucket = cloud_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_filepath)

    if tar_file:
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:
            with tarfile.open(tmp.name, "w") as tar:
                tar.add(source_filepath)
            blob.upload_from_filename(tmp.name)
            if return_uploaded_file:
                with tarfile.open(return_uploaded_file, "w") as tar:
                    tar.add(source_filepath)

    else:
        blob.upload_from_filename(source_filepath)
        if return_uploaded_file:
            with tarfile.open(return_uploaded_file, "w") as tar:
                tar.add(source_filepath)


def gcs_download(
    cloud_client,
    source_filepath="path_to_my_file_in_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="sample_bucket",
    untar=False,
):
    """ Download data to a Google storage bucket
    Args:
        source_filepath: Path to file being downloaded on Google
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: google storage client with user credentials
        bucket_name: name of google bucket being downloaded from
        untar: Whether to decompress file being downloaded (Tar)
    """

    bucket = cloud_client.get_bucket(bucket_name)
    this_blob = bucket.blob(source_filepath)

    if untar:
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as temp_path:
            this_blob.download_to_filename(temp_path.name)

            if tarfile.is_tarfile(temp_path.name):
                with tarfile.open(temp_path.name) as tar_file:
                    tar_file.extractall(destination_filepath)
            else:
                this_blob.download_to_filename(destination_filepath)
    else:
        this_blob.download_to_filename(destination_filepath)


def s3_upload(
    cloud_client,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_in_bucket",
    bucket_name="sample_bucket",
    tar_file=False,
    return_uploaded_file=None,
):
    """Upload data to Amazon AWS bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to AWS bucket
        cloud_client: AWS client with user credentials
        bucket_name: name of AWS bucket
        tar_file: Tar file before upload
        return_uploaded_file: If filepath is given,
        downloads copy of file being uploaded to filepath

    """
    if tar_file:

        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:
            with tarfile.open(tmp.name, "w") as tar:
                tar.add(source_filepath)

            cloud_client.upload_file(
                Filename=tmp.name, Bucket=bucket_name, Key=destination_filepath,
            )
            if return_uploaded_file:
                with tarfile.open(return_uploaded_file, "w") as tar:
                    tar.add(source_filepath)

    else:
        cloud_client.upload_file(
            Filename=source_filepath, Bucket=bucket_name, Key=destination_filepath,
        )
        if return_uploaded_file:
            with tarfile.open(return_uploaded_file, "w") as tar:
                tar.add(source_filepath)


def s3_download(
    cloud_client,
    source_filepath="path_to_my_file_in_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="sample_bucket",
    untar=False,
):
    """ Download data from an Amazon AWS bucket
    Args:
        source_filepath: Path to file being downloaded on AWS
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: AWS client with user credentials
        bucket_name: name of AWS bucket being downloaded from
        untar: Whether to decompress file being downloaded (Tar)
    """

    if untar:
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp:
            cloud_client.download_file(
                Bucket=bucket_name, Key=source_filepath, Filename=tmp.name,
            )
            if tarfile.is_tarfile(tmp.name):
                with tarfile.open(tmp.name) as tar_file:
                    tar_file.extractall(destination_filepath)
    else:
        cloud_client.download_file(
            Bucket=bucket_name, Key=source_filepath, Filename=destination_filepath,
        )


def file_upload(
    client_type,
    cloud_client,
    source_filepath="path_to_my_local_file",
    destination_filepath="path_to_my_file_in_bucket",
    bucket_name="sample_bucket",
    tar_file=True,
    return_uploaded_file=None,
):
    """Function run to upload file to Google Bucket or Amazon AWS bucket
    Args:
        source_filepath: file path of source file being uploaded
        destination_filepath: path to bucket
        cloud_client: client with user credentials, used to select client type
        bucket_name: name of bucket
        tar_file: Tar file before upload
        return_uploaded_file: If filepath is given,
        downloads copy of file being uploaded to filepath
        client_type: enter name of client type ie. 's3' or 'gcs'

    """
    if client_type == "gcs":
        gcs_upload(
            cloud_client=cloud_client,
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            tar_file=tar_file,
            return_uploaded_file=return_uploaded_file,
        )
    if client_type == "s3":
        s3_upload(
            cloud_client=cloud_client,
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            tar_file=tar_file,
            return_uploaded_file=return_uploaded_file,
        )


def file_download(
    client_type,
    cloud_client,
    source_filepath="path_to_my_file_in_bucket",
    destination_filepath="path_to_my_local_file",
    bucket_name="sample_bucket",
    untar=True,
):
    """Function selection when downloading file from Google Bucket or Amazon AWS bucket
    Args:
        source_filepath: filepath being downloaded from
        destination_filepath: Destination filepath for file being downloaded
        cloud_client: client with user credentials used for client type
        bucket_name: name of bucket being downloaded from
        untar: Whether to decompress file being downloaded (Tar)
        client_type: enter name of client type ie. 's3' or 'gcs'
    """
    if client_type == "gcs":
        gcs_download(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            cloud_client=cloud_client,
            untar=untar,
        )
    if client_type == "s3":
        s3_download(
            source_filepath=source_filepath,
            destination_filepath=destination_filepath,
            bucket_name=bucket_name,
            cloud_client=cloud_client,
            untar=untar,
        )
