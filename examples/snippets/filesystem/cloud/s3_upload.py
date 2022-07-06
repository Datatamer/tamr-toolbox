"""
An example script to demonstrate the use of the s3 function to upload file
"""

import tamr_toolbox as tbox
from boto3.session import Session

S3Client = Session("credentials").client("s3")

# uploads file to AWS S3
# upload a local file "my_local_directory/my_file.txt" to "s3://my-bucket/path-to-file"
tbox.filesystem.cloud.s3_upload(
    cloud_client=S3Client,
    source_filepath="my_local_directory/my_file.txt",
    destination_filepath="s3://my-bucket/path-to-file",
    bucket_name="my-bucket",
)
