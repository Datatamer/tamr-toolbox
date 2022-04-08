from typing import List, Dict, Optional, Literal, Union
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
    Updates a project to be LLM queryable, if needed
    Args:
        tamr_client: Tamr client object
        project_name: name of the project to be updated
        do_update_clusters: whether to update clusters, defaults to True
        do_use_manual_clustering: whether to use externally managed clustering, defaults to False
    """

    url = f"projects/{project_name}/publishedClusters:refresh"
    tamr_client.post(url)

    LOGGER.info("Tamr publish clusters complete")

    url = (
        f"projects/{project_name}:updateLLM?updateClusters={do_update_clusters}"
        f"&useManualClustering={do_use_manual_clustering}"
    )
    response = tamr_client.post(url)
    if not response.ok:
        message = f"Update LLM failed at submission time: {response.content}"
        LOGGER.error(message)
        raise RuntimeError(message)
    operation_id = response.content.decode("latin1")

    # An operation id of '-1' is returned when LLM is already up to date
    if operation_id != "-1":
        operation = Operation.from_resource_id(tamr_client, operation_id=operation_id)
        operation.wait()
    return None


def poll_llm_status(match_client: Client, *, project_name: str, num_tries: int = 10) -> bool:
    """
    Check if LLM is queryable. Try up to num_tries times at 1s intervals.

    Args:
        match_client: a Tamr client set to use the port of the Match API
        project_name: name of target mastering project
        num_tries: optional, max number of times to poll endpoint
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
        time.sleep(1)  # call api at 1s interval if project isn't yet queryable

    return queryable


def llm_query(
    match_client: Client,
    *,
    project_name: str,
    records: Union[JsonDict, List[JsonDict]],
    type: Literal["records", "clusters"],
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
        type: whether to pull record or cluster matches
        batch_size: split input into this batch size for LLM calls (e.g. to prevent network
            timeouts), Default None sends a single LLM call with all records
        min_match_prob: if set, only matches with probability above minimum will be returned,
            Default None
        max_num_matches: if set, at most max_num_matches will be returned for each input record in
            records, Default None
    Returns:
        Dict keyed by integers (the indices of the records), with value a list containing closest
            matched clusters
    Raises:
        ValueError: if match type is not "records" or "clusters", or if batch_size is non-positive
        RuntimeError: if query fails
    """

    result_dict = defaultdict(lambda: [])  # dict which defaults to empty list to hold results

    url = f"/api/v1/projects/{project_name}:match?type={type}"

    # Sett up keys to read results
    if type == "records":
        record_key = "queryRecordId"
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
    for j in range(len(records) // batch_size + 1):
        json_records = [
            {"recordId": str(batch_size * j + k), "record": rec}
            for k, rec in enumerate(records[batch_size * j : batch_size * (j + 1)])
        ]
        response = match_client.post(url, json=json_records)

        # Process responses
        if response.ok:
            if response.content == b"":  # handle null response
                continue

            # If data was found, decode, identify source record, and add match to corresponding
            # index in the list of lists of results
            for resp_block in response.content.decode("utf-8").split("\n"):
                if resp_block:
                    result = json.loads(resp_block)
                    index = int(result[record_key])

                    if max_num_matches and len(result_dict[index]) >= max_num_matches:
                        continue
                    if min_match_prob:
                        prob = result[prob_key] if type == "clusters" else get_record_prob(result)
                        if prob < min_match_prob:
                            continue

                    result_dict[index].append(result)

        else:
            message = f"LLM query failed: {response.content}"
            LOGGER.error(message)
            raise RuntimeError(message)

    return result_dict


def get_record_prob(input: JsonDict) -> float:
    """Parses dictionary returned from a LLM records query. If match, returns confidence of match.
    If non-match, returns 1 - confidence.

    Args:
        input: Dictionary which contains keys 'suggestedLabel' and 'suggestedLabelConfidence'
    Returns:
        confidence that input _is_ a match
    """

    prob = input["suggestedLabelConfidence"]

    if input["suggestedLabel"] == "MATCH":
        return prob
    else:
        return 1 - prob
