from typing import List, Dict, Optional, Union
import json
from collections import defaultdict
import logging
import time

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.data_type import JsonDict


LOGGER = logging.getLogger(__name__)


def update_llm_data(
    tamr_client: Client,
    *,
    project_name: str,
    do_update_clusters: bool = True,
    do_use_manual_clustering: bool = False,
) -> None:
    """
    Updates data for LLM query if needed, based on latest published clusters.

    Args:
        tamr_client: Tamr client object
        project_name: name of the project to be updated
        do_update_clusters: whether to update clusters, default True
        do_use_manual_clustering: whether to use externally managed clustering, default False
    """

    url = (
        f"projects/{project_name}:updateLLM?updateClusters={do_update_clusters}"
        f"&useManualClustering={do_use_manual_clustering}"
    )
    response = tamr_client.post(url)
    if not response.ok:
        message = (
            f"LLM update for {project_name} failed at submission time: "
            + response.json()["message"]
        )
        LOGGER.error(message)
        raise RuntimeError(message)
    operation_id = response.content.decode("latin1")

    # An operation id of '-1' is returned when LLM is already up to date
    if operation_id != "-1":
        operation = Operation.from_resource_id(tamr_client, operation_id)
        operation.wait()
    return None


def poll_llm_status(
    match_client: Client, *, project_name: str, num_tries: int = 10, wait_sec: int = 1
) -> bool:
    """
    Check if LLM is queryable. Try up to num_tries times at 1s (or user-specified) interval.

    Args:
        match_client: a Tamr client set to use the port of the Match API
        project_name: name of target mastering project
        num_tries: max number of times to poll endpoint, default 10
        wait_sec: number of seconds to wait between tries, default 1
    Returns:
        bool indicating whether project is queryable
    """

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


def llm_query(
    match_client: Client,
    *,
    project_name: str,
    records: Union[JsonDict, List[JsonDict]],
    type: str,
    pKey: Optional[str] = None,
    batch_size: Optional[int] = None,
    min_match_prob: Optional[float] = None,
    max_num_matches: Optional[int] = None,
) -> Dict[int, List[JsonDict]]:
    """
    Find the best matching clusters or records for each supplied record. Returns empty list if
    response is null.

    Args:
        match_client: a Tamr client set to use the port of the Match API
        project_name: name of target mastering project
        records: record or list of records to match
        type: one of "records" or  "clusters" -- whether to pull record or cluster matches
        pKey: a primary key for the data; if supplied, this must be a field in the input records
        batch_size: split input into this batch size for LLM calls (e.g. to prevent network
            timeouts), default None sends a single LLM call with all records
        min_match_prob: if set, only matches with probability above minimum will be returned,
            default None
        max_num_matches: if set, at most max_num_matches will be returned for each input record in
            records, default None
    Returns:
        Dict keyed by integers (indices of inputs), or by pKey if pKey is supplied, with value a 
            list containing matcched data
    Raises:
        ValueError: if match type is not "records" or "clusters", or if batch_size is non-positive
        RuntimeError: if query fails
    """

    result_dict = defaultdict(lambda: [])  # dict which defaults to empty list to hold results

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

    # Convert single record to list for processing
    if isinstance(records, Dict):
        records = [records]

    # Check batch size and set if not supplied
    if batch_size is None:
        batch_size = len(records)
        if batch_size == 0:
            LOGGER.warn("No input supplied to llm_query -- returning empty result.")
            return result_dict
    elif batch_size <= 0:
        raise ValueError(f"Batch size must be non-negative: received {batch_size}")

    # Split into batches and convert to LLM query format
    for j in range(0, len(records), batch_size):
        json_recs = _prepare_json(records[j: j + batch_size], pKey=pKey, offset=j)
        response = match_client.post(url, json=json_recs)

        # Process responses
        if response.ok:
            if response.content == b"":  # handle null response
                continue

            # If data was found, decode, identify source record, and add match to corresponding
            # index in the list of lists of results
            for resp_block in response.content.decode("utf-8").split("\n"):
                if resp_block:
                    result = json.loads(resp_block)
                    index = int(result[record_key]) if pKey is None else result[record_key]

                    if max_num_matches and len(result_dict[index]) >= max_num_matches:
                        continue

                    if min_match_prob:
                        if result[prob_key] < min_match_prob:
                            continue

                    result_dict[index].append(result)

        else:
            message = f"LLM query failed: {response.content}"
            LOGGER.error(message)
            raise RuntimeError(message)

    return result_dict


def _prepare_json(records: List[JsonDict], *, pKey: Optional[str], offset: int) -> List[JsonDict]:
    """
    Put records into JSON format expected by LLM endpoint 

    Args:
        records: list of records to match
        pKey: a primary key for the data; if supplied, this must be a field in the input records
        offset: offset to apply to generated integer `recordId` -- this is necessary for batching
    Returns:
        List of formatted records
    Raises:
        ValueError: if pKey is supplied but is not a field in some record
    """

    if pKey:
        try:
            json_records = [{"recordId": rec.pop(pKey), "record": rec} for rec in records]
        except KeyError:
            raise ValueError(f"Not all input records had a primary key field {pKey}.")
    else:  # use integers as recordId
        json_records = [
            {"recordId": str(offset + k), "record": rec} for k, rec in enumerate(records)
        ]

    return json_records
