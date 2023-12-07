"""Tasks related to interacting with the Tamr Core-connect"""
import json
import logging
from tamr_toolbox.models.data_type import JsonDict
from tamr_unify_client import Client
from tamr_toolbox.utils import version
from airflow.exceptions import AirflowException
import time

LOGGER = logging.getLogger(__name__)


def _run_and_poll_connect_job(tamr_client, api_path, ingest_body):
    """
    Function to run and monitor a core-connect job till completion
    """
    response = tamr_client.post(api_path, json=ingest_body)
    response_dict = json.loads(response.content)
    if not response.ok:
        error_message = f'Ingest failed with message: {response_dict["message"]}'
        raise AirflowException(error_message)
    else:
        job_id = response_dict["jobId"]
        job_poll_url = f"/api/connect/jobs/{job_id}"
        job_response = tamr_client.get(job_poll_url)
        job_response_dict = json.loads(job_response.content)
        job_status = job_response_dict["status"]
        while job_status != "SUCCEEDED":
            # Wait for 10 seconds between polling:
            time.sleep(10)
            job_response = tamr_client.get(job_poll_url)
            job_response_dict = json.loads(job_response.content)
            job_status = job_response_dict["status"]
            if job_status == "FAILED":
                error_message = {
                    f'Ingest failed with message: {job_response_dict["errors"]["error"]}'
                }
                raise AirflowException(error_message)

    return response_dict


def jdbc_ingest(
    *,
    client: Client,
    jdbc_connect: JsonDict,
    dataset_name: str,
    query: str,
    primary_key=None,
    truncate_tamr_dataset=None,
    retrieve_connect_metadata=None,
    retrieve_source_metadata=None,
    tamr_min_version="2022.011.0",
) -> JsonDict:
    """
    Ingest a dataset into Tamr via core_connect given query config, dataset name, query string,
    and optional list of columns for primary key

    Args:
        client: Tamr unify client object.
        jdbc_connect: queryConfig used by the API call.
        dataset_name: Name of Tamr dataset.
        query: jdbc query to execute in the database and results of which will be loaded into Tamr.
        primary_key(optional): list of columns to use as primary key.
        truncate_tamr_dataset(optional): Truncate the Tamr dataset if it exists.
        retrieve_connect_metadata(optional): Core Connect retrieves the metadata for services.
        retrieve_source_metadata(optional): Only applies to Snowflake.
        tamr_min_version(optional): The minimum version of Tamr to use Core Connect.

    Raises:
        HTTPError: if the call to ingest the dataset was unsuccessful
    """

    # Check Tamr version
    if version.is_version_condition_met(
        tamr_version=version.current(client), min_version=tamr_min_version
    ):
        None
    else:
        error_message = "Core-connect is not available in current version of Tamr."
        LOGGER.error(error_message)
        raise Exception(error_message)

    # handle primary key
    if primary_key is None:
        primary_key = []
    else:
        primary_key = primary_key.split(",")

    # ingest data
    api_path = "/api/connect/jdbcIngest/ingest"
    ingest_body = {
        "query": query,
        "datasetName": dataset_name,
        "primaryKey": primary_key,
        "queryConfig": jdbc_connect,
        "truncateTamrDataset": truncate_tamr_dataset,
        "metadataConfig": {
            "retrieveConnectMetadata": retrieve_connect_metadata,
            "retrieveSourceMetadata": retrieve_source_metadata,
        },
    }

    LOGGER.info(f"Streaming data from {jdbc_connect['jdbcUrl']} to {dataset_name}.")

    response_dict = _run_and_poll_connect_job(client, api_path, ingest_body)

    return response_dict
