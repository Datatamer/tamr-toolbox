"""
Additional functions to manipulate the profile of the dataset.
"""

from tamr_unify_client.dataset.resource import Dataset, DatasetProfile
from requests import HTTPError
import logging

LOGGER = logging.getLogger(__name__)


def get_profile(dataset: Dataset, allow_create_or_refresh: bool = False) -> DatasetProfile:
    """
    Returns a dataset profile object.  Optionally can refresh or create profile if missing or
    out-of-date.
    Args:
        dataset: Tamr dataset object
        allow_create_or_refresh: optional bool to allow creation/refreshing of profile info
    Returns:
        DatasetProfile object
        Warning if profile information is out of date and allow_create_or_refresh is False
    Raises:
        RuntimeError: if profile has not been created and allow_create_or_refresh is False
    """
    try:
        profile = dataset.profile()
    except HTTPError as e:
        # profile not yet created or in inconsistent state: 409 or 400 return respectively
        if e.response.status_code in [409, 400]:
            if allow_create_or_refresh:
                LOGGER.info(f"creating profile information for dataset: {dataset.name}")
                dataset.create_profile()
                profile = dataset.profile()
            else:
                raise RuntimeError(
                    f"Profile information for {dataset.name} does not exist. If you would like "
                    f"to create one, set allow_create_or_refresh to True."
                )
        # other status codes: re-raise error
        else:
            raise e

    if not profile.is_up_to_date:
        if allow_create_or_refresh:
            LOGGER.info(f"refreshing profile information for dataset: {dataset.name}")
            profile.refresh()
            # retrieve again for up-to-date information
            profile = dataset.profile()
        else:
            # Return profile information with a staleness warning:
            print(
                f"WARNING: Profile information for {dataset.name} is out-of-date "
                f"and allow_create_or_refresh is False. If you would like an up-to-date "
                f"profile, rerun with allow_create_or_refresh set to True."
            )

    return profile
