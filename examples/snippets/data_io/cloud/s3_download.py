"""
An example script to demonstrate the use of the s3_download function to download file from,
AWS S3 with diferent parameters.
"""

import tamr_toolbox as tbox

# download file from AWS S3
tbox.data_io.cloud.s3_download(
    cloud_client="authenticated_gcs_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    untar=False,
)

# download file from AWS S3 and un-tar content
tbox.data_io.cloud.s3_download(
    cloud_client="authenticated_gcs_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    untar=False,
)
