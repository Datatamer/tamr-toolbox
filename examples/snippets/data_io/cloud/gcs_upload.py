"""
An example script to demonstrate the use of the gcs_upload function to upload file,
to a Google Bucket with multiple diferent parameters
"""

import tamr_toolbox as tbox

# uploads file to gcs without tarring
tbox.data_io.cloud.gcs_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=False,
    return_uploaded_file=None,
)

# uploads tarred version of file to gcs
tbox.data_io.cloud.gcs_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=True,
    return_uploaded_file=None,
)

# uploads file to gcs without tarring saves a copy to passed path
tbox.data_io.cloud.gcs_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=False,
    return_uploaded_file="directory_to_save_copy_of_file_being_uploaded",
)

# uploads tarred version of file and saves copy of tarred version
tbox.data_io.cloud.gcs_upload(
    cloud_client="authenticated_gcs_client",
    source_filepath="sample_file_to_be_uploaded",
    destination_filepath="path_in_bucket.txt",
    tar_file=False,
    return_uploaded_file="directory_to_save_copy_of_file_being_uploaded",
)
