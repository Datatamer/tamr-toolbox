"""
An example script to demonstrate the use of the file_download function to download file from,
passed client with diferent parameters.
"""

import tamr_toolbox as tbox

# example of download_file from passed client (gcs) with no un-tarring
tbox.data_io.cloud.file_download(
    client_type="gcs",
    cloud_client="authenticated_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    untar=False,
)

# example of download_file from passed client (gcs) un-tarring file content
tbox.data_io.cloud.file_download(
    client_type="gcs",
    cloud_client="authenticated_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    untar=True,
)
