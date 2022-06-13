"""
An example script to demonstrate the use of the file_upload function to upload file to,
passed client with diferent parameters.
"""

import tamr_toolbox as tbox

# uploads file to passed client (s3)
tbox.data_io.cloud.file_upload(
    client_type="s3",
    cloud_client="authenticated_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    tar_file=False,
    return_uploaded_file=False,
)

# uploads file to passed client (s3) and tar's passed file's content before upload
tbox.data_io.cloud.file_upload(
    client_type="s3",
    cloud_client="authenticated_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    tar_file=True,
    return_uploaded_file=False,
)

# uploads file to passed client (s3) and returns a copy of file being uploaded
tbox.data_io.cloud.file_upload(
    client_type="s3",
    cloud_client="authenticated_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    tar_file=False,
    return_uploaded_file=True,
)

# uploads file to passed client (s3) and tar's passed file's content before upload
# returns copy of tarred file
tbox.data_io.cloud.file_upload(
    client_type="s3",
    cloud_client="authenticated_client",
    source_filepath="path_in_bucket.txt",
    destination_filepath="path_where_file_is_downloaded_to",
    bucket_name="test-bucket-a",
    tar_file=True,
    return_uploaded_file=True,
)
