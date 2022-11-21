import json
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Union

import requests
from tamr_unify_client import Client
from tamr_unify_client.mastering.project import MasteringProject
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.data_type import JsonDict
from tamr_toolbox.utils.operation import from_resource_id

LOGGER = logging.getLogger(__name__)


def update_realtime_match_data(
    *,
    project: MasteringProject,
    do_update_clusters: bool = True,
    do_use_manual_clustering: bool = False,
    **options,
) -> Operation:
    """
    Updates data for RealTime match queries if needed, based on latest published clusters.

    Args:
        project: project to be updated
        do_update_clusters: whether to update clusters, default True
        do_use_manual_clustering: whether to use externally managed clustering, default False
        options: Options passed to underlying :class:`~tamr_unify_client.operation.Operation`

    Returns:
        an operation object describing the update operation

    Raises:
        RuntimeError: if update API call fails
    """

    # Make sure we have the original name of the project to use with the match endpoint
    project_name = _get_internal_project_name(project)

    url = (
        f"projects/{project_name}:updateLLM?updateClusters={do_update_clusters}"
        f"&useManualClustering={do_use_manual_clustering}"
    )

    try:
        response = project.client.post(url).successful()
    except requests.exceptions.HTTPError as e:
        message = f"Match data update for {project_name} failed at submission time: {e}"
        LOGGER.error(message)
        raise RuntimeError(message)
    operation_id = response.content.decode("latin1")
    operation = from_resource_id(project.client, job_id=operation_id)

    return operation.apply_options(**options)


def poll_realtime_match_status(
    *, project: MasteringProject, match_client: Client, num_tries: int = 10, wait_sec: int = 1
) -> bool:
    """
    Check if match service is queryable. Try up to `num_tries` times at 1 sec (or user-specified)
    interval.

    Args:
        project: the mastering project whose status to check
        match_client: a Tamr client set to use the port of the Match API
        num_tries: max number of times to poll endpoint, default 10
        wait_sec: number of seconds to wait between tries, default 1

    Returns:
        bool indicating whether project is queryable
    """

    project_name = _get_internal_project_name(project)
    url = f"/api/v1/projects/{project_name}:isQueryable"
    counter = 0

    # Poll endpoint to until project is queryable or num_tries reached
    while counter < num_tries:

        response = match_client.get(url)
        queryable = response.content == b"true"

        if queryable:
            break

        counter += 1
        time.sleep(wait_sec)  # call api at wait_sec interval if project isn't yet queryable

    return queryable


def match_query(
    *,
    project: MasteringProject,
    match_client: Client,
    records: List[JsonDict],
    type: str,
    primary_key: Optional[str] = None,
    batch_size: Optional[int] = None,
    min_match_prob: Optional[float] = None,
    max_num_matches: Optional[int] = None,
) -> Dict[Union[int, str], List[JsonDict]]:
    """
    Find the best matching clusters or records for each supplied record. Returns a dictionary where
    each key correpsonds to an input record and the value is a list of the RealTime match results
    for that record. An empty result list indicates a null response from matching (or no responses
    above the `min_match_prob`, if that parameter was supplied).

    Args:
        project: the mastering project to query for matches
        match_client: a Tamr client set to use the port of the Match API
        records: list of records to match
        type: one of "records" or  "clusters" -- whether to pull record or cluster matches
        primary_key: a primary key for the data; if supplied, this must be a field in input records
        batch_size: split input into this batch size for match query calls (e.g. to prevent network
            timeouts), default None sends a single query with all records
        min_match_prob: if set, only matches with probability above minimum will be returned,
            default None
        max_num_matches: if set, at most max_num_matches will be returned for each input record in
            records, default None

    Returns:
        Dict keyed by integers (indices of inputs), or by `primary_key` if `primary_key` is
        supplied, with value a list containing matched data

    Raises:
        ValueError: if match `type` is not "records" or "clusters", or if `batch_size` is invalid
        RuntimeError: if query fails
    """

    result_dict = defaultdict(lambda: [])  # dict which defaults to empty list to hold results

    project_name = _get_internal_project_name(project)
    url = f"/api/v1/projects/{project_name}:match?type={type}"

    # Set up keys to read results
    if type == "records":
        record_key = "queryRecordId"
        prob_key = "matchProbability"
    elif type == "clusters":
        record_key = "entityId"
        prob_key = "avgMatchProb"
    else:
        raise ValueError(f"Unsupported match type {type}.")

    # Check batch size and set if not supplied
    if batch_size is None:
        batch_size = len(records)
        if batch_size == 0:
            LOGGER.warn("No input supplied to match_query -- returning empty result.")
            return result_dict
    elif batch_size <= 0:
        raise ValueError(f"Batch size must be non-negative: received {batch_size}")

    # Split into batches and convert to match query format
    for j in range(0, len(records), batch_size):
        json_recs = _prepare_json(records[j : j + batch_size], primary_key=primary_key, offset=j)

        try:
            response = match_client.post(url, json=json_recs).successful()
        except requests.exceptions.HTTPError as e:
            message = f"RealTime match query failed: {e}"
            LOGGER.error(message)
            raise RuntimeError(message)

        # Process responses
        if response.content == b"":  # handle null response
            continue

        # If data was found, decode, identify source record, and add match to corresponding
        # list of results in the result dictionary
        for resp_block in response.content.decode("utf-8").split("\n"):
            if resp_block:
                result = json.loads(resp_block)
                index = int(result[record_key]) if primary_key is None else result[record_key]
                if max_num_matches and len(result_dict[index]) >= max_num_matches:
                    continue

                if min_match_prob and result[prob_key] < min_match_prob:
                    continue

                result_dict[index].append(result)

    return result_dict


def _prepare_json(
    records: List[JsonDict], *, primary_key: Union[str, None], offset: int
) -> List[JsonDict]:
    """
    Put records into JSON format expected by RealTime match endpoint

    Args:
        records: list of records to match
        primary_key: a primary key for the data; if not None, must be a field in the input records
        offset: offset to apply to generated integer `recordId` -- this is necessary for batching

    Returns:
        List of formatted records

    Raises:
        ValueError: if primary_key is supplied but some supplied record(s) do not have the
            primary_key field
    """

    if primary_key is not None:
        try:
            json_records = [{"recordId": rec.pop(primary_key), "record": rec} for rec in records]
        except KeyError:
            raise ValueError(f"Not all input records had a primary key field {primary_key}.")
    else:  # use integers as recordId
        json_records = [
            {"recordId": str(offset + k), "record": rec} for k, rec in enumerate(records)
        ]

    return json_records


def _get_internal_project_name(project: MasteringProject) -> str:
    """Get project's internal name (as opposed to the `displayName`, which can be changed by users)

    Args:
        project: the mastering project of which to retrieve the internal name

    Returns:
        the project's internal name
    """
    # Get usage data for unified dataset of input project
    usage_resp = project.unified_dataset().usage()
    # Get project internal name from first output_from_project_step of unified dataset useage
    name = usage_resp.usage.output_from_project_steps[0].project_name
    return name
