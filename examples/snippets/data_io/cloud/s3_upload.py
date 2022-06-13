"""
An example script to demonstrate the use of the s3 function to upload file,
to an AWS S3 Bucket with multiple diferent parameters
"""

import tamr_toolbox as tbox

# uploads file to AWS S3 without tarring
tbox.data_io.cloud.s3_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=False,
    return_uploaded_file=None,
)

# uploads tarred version of file to AWS S3
tbox.data_io.cloud.s3_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=True,
    return_uploaded_file=None,
)

# uploads file to AWS S3 without tarring saves a copy to passed path
tbox.data_io.cloud.s3_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=False,
    return_uploaded_file="directory_to_save_copy_of_file_being_uploaded",
)

# uploads tarred version of file to AWS S3 and saves copy of tarred version to given path
tbox.data_io.cloud.s3_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=False,
    return_uploaded_file="directory_to_save_copy_of_file_being_uploaded",
)
