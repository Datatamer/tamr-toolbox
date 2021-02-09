"""Enum representing types of the filesystems"""
from enum import Enum


class FileSystemType(Enum):
    LOCAL = "serverfs"
    HDFS = "hdfs"
    ADLS = "adls"
    S3 = "s3"
    GS = "gs"
