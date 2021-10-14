"""Tasks related to Tamr Schema Mapping projects"""
from . import jobs
from .._common import schema, transformations

__all__ = ["jobs", "schema", "transformations"]


# define Enum classes for project steps
from enum import Enum


class Steps(Enum):
    """
    Enum class for the steps of a schema mapping project
    """

    UPDATE_UNIFIED_DATASET = "updated_unified_dataset"
