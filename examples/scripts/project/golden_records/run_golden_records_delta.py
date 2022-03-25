"""Script for running a Mastering Pipeline upto the point of Updating Golden Records"""
# %%
from typing import Dict, Any
from os.path import join
from pathlib import Path
from pyrsistent import freeze
from tamr_toolbox import utils
import tamr_toolbox as tbox

# Establish CONFIG and LOGGER as globals
# Use `freeze` to get an immutable object that cannot be modified

# example config
"""
golden_records_delta_pipeline:
    pre_processing_projects: ["preprocessing_first_datasource", "preprocessing_second_datasource"]
    mastering_project: "My_Mastering_Project"
    GR_project: "MY_GR_Project"
    GR_changes_dataset: "GR_Changes_unified_dataset"
    GR_changes_project: "GR_Changes"
    GR_mapping_delta_dataset: "GR_Mapping_Delta_unified_dataset"
    GR_mapping_delta_project: "GR_Mapping_Delta"
    GR_output_project: "GR_Output"
    GR_output_delta_project: "GR_Output_Delta"
    GR_mapping_project: "GR_Mapping"
"""


CONFIG = freeze(
    utils.config.from_yaml(join(Path(__file__).resolve().parents[1], "conf", "config.yaml"))
)
LOGGER = utils.logger.create("MasteringPipeline", log_directory=CONFIG["logging_dir"])
# Let Tamr Toolbox itself also contribute to the log
utils.logger.enable_toolbox_logging(log_directory=CONFIG["logging_dir"])
LOGGER.info('Logging Enabled')


def main(*, instance_connection_info: Dict[str, str], pipeline_vars: Dict[str, Any]) -> Dict[str, str]:
    """Runs the continuous steps of preprocessing projects, mastering project, and updates a Golden Records project without publishing it
    Args:
        instance_connection_info: Information for connecting to Tamr (host, port, username etc)
        pipeline_vars: The names of target projects and datasets for a mastering pipeline upto updating golden records
    Returns: Dict of jobs run and status output, either 'Failed' or 'Successful'.
    """

    PrjRunStatus = {}
    PrjRunStatus["preprocessing"] = "Failed"
    PrjRunStatus["mastering"] = "Failed"
    PrjRunStatus["profile_GR_Dataset"] = "Failed"
    PrjRunStatus["update_GR_Dataset"] = "Failed"
    PrjRunStatus["pin_ChangesDataset"] = "Failed"
    PrjRunStatus["pin_MapDeltasDataset"] = "Failed"
    PrjRunStatus["update_GR_Changes_project"] = "Failed"
    PrjRunStatus["update_GR_MapDeltas_project"] = "Failed"
    PrjRunStatus["publish_GR"] = "Failed"
    PrjRunStatus["update_GR_output"] = "Failed"
    PrjRunStatus["update_GR_Delta"] = "Failed"
    PrjRunStatus["update_GR_Mapping"] = "Failed"

    # Create the tamr client
    tamr_client = utils.client.create(**instance_connection_info)
    LOGGER.info(
        "retreiving all project names and associated resource ID from Tamr and storing as a lookup dictionary")
    project_id_dict = {}

    for project in tamr_client.projects:
        project_id_dict[project.name] = project.resource_id

    # Retrieve the projects
    pre_processing_projects = pipeline_vars['pre_processing_projects']
    preprocessing_project_ids = [project_id_dict[p_name] for p_name in pre_processing_projects]
    preprocessing_projects = [tamr_client.projects.by_resource_id(
        p_id) for p_id in preprocessing_project_ids]

    LOGGER.info(
        f"About to run pre-processing projects: {[p.name for p in preprocessing_projects]}")

    operations_preprocessing = tbox.workflow.jobs.run(
        preprocessing_projects, run_apply_feedback=False, run_estimate_pair_counts=False
    )

    allSuccess = sum([op.state == "SUCCEEDED" for op in operations_preprocessing])

    if allSuccess == len(operations_preprocessing):
        PrjRunStatus["preprocessing"] = "Successful"
        LOGGER.info("All preprocessing projects ran successfully.")
    else:
        LOGGER.info("FAILURE in a preprocessing project")

    mastering_project_id = project_id_dict[pipeline_vars['mastering_project']]
    mastering_project = tamr_client.projects.by_resource_id(mastering_project_id)

    operations_mastering = tbox.workflow.jobs.run(
        [mastering_project], run_apply_feedback=False, run_estimate_pair_counts=False
    )

    allSuccess = sum([op.state == "SUCCEEDED" for op in operations_mastering])
    if allSuccess == len(operations_mastering):
        PrjRunStatus["mastering"] = "Successful"
        LOGGER.info("Mastering project ran successfully.")
    else:
        LOGGER.info("FAILURE in mastering project")

    GR_project_id = project_id_dict[pipeline_vars['GR_project']]
    LOGGER.info("PROFILE DATASET GOING INTO GR PROJECT")
    prof_gr_sts = tbox.project.golden_records.jobs.update_input_dataset_profiling_information(
        tamr_client.projects.by_resource_id(GR_project_id))

    if prof_gr_sts[0].state == "SUCCEEDED":
        PrjRunStatus["profile_GR_Dataset"] = "Successful"
        LOGGER.info('Golden Records incoming dataset is profiled')
    else:
        LOGGER.info('FAILURE to profile dataset going into Golden Records')

    LOGGER.info("UPDATING GOLDEN RECORDS IN GR PROJECT WITHOUT PUBLISHING")
    upd_gr_sts = tbox.project.golden_records.jobs.update_golden_records(
        tamr_client.projects.by_resource_id(GR_project_id))

    if upd_gr_sts[0].state == "SUCCEEDED":
        PrjRunStatus["update_GR_Dataset"] = "Successful"
        LOGGER.info('Golden Records Updated')
    else:
        LOGGER.info('FAILURE to update Golden Records')

    # Create the tamr client
    tamr_client = tbox.utils.client.create(**instance_connection_info)
    LOGGER.info(
        "retreiving all project names and associated resource ID from Tamr and storing as a lookup dictionary")
    project_id_dict = {}

    for project in tamr_client.projects:
        project_id_dict[project.name] = project.resource_id

    LOGGER.info("Advaning pinned version of CHANGES dataset")
    pin_ChangesDataset = tamr_client.projects.client.post(
        "/api/dataset/datasets/" + pipeline_vars['GR_changes_dataset'] + "/advancePinnedVersion")
    if pin_ChangesDataset.status_code == 200:
        PrjRunStatus["pin_ChangesDataset"] = "Successful"
        LOGGER.info('Changes Dataset Pinned Version Advanced')
    else:
        LOGGER.info('FAILURE to advanced Changes Dataset')

    LOGGER.info("Advaning pinned version of DELTA dataset")

    pin_DeltasDataset = tamr_client.projects.client.post(
        "/api/dataset/datasets/" + pipeline_vars['GR_mapping_delta_dataset'] + "/advancePinnedVersion")
    if pin_DeltasDataset.status_code == 200:
        PrjRunStatus["pin_MapDeltasDataset"] = "Successful"
        LOGGER.info('Delta Dataset Pinned Version Advanced')
    else:
        LOGGER.info('FAILURE to advanced Delta Dataset')

    upd_GR_Changes_sts = tbox.project.schema_mapping.jobs.update_unified_dataset(
        tamr_client.projects.by_resource_id(project_id_dict[pipeline_vars["GR_changes_project"]]))
    if upd_GR_Changes_sts[0].state == "SUCCEEDED":
        PrjRunStatus["update_GR_Changes_project"] = "Successful"
        LOGGER.info('Changes project is updated')
    else:
        LOGGER.info('FAILURE to update changes project')

    upd_GR_MapDeltas_sts = tbox.project.schema_mapping.jobs.update_unified_dataset(
        tamr_client.projects.by_resource_id(project_id_dict[pipeline_vars["GR_mapping_delta_project"]]))
    if upd_GR_MapDeltas_sts[0].state == "SUCCEEDED":
        PrjRunStatus["update_GR_MapDeltas_project"] = "Successful"
        LOGGER.info('Mapping Delta project is updated')
    else:
        LOGGER.info('FAILURE to update Mapping Delta project')

    GR_project_id = project_id_dict[pipeline_vars['GR_project']]
    LOGGER.info("Publishing new golden records")
    pub_gr_sts = tbox.project.golden_records.jobs.publish_golden_records(
        tamr_client.projects.by_resource_id(GR_project_id))
    if pub_gr_sts[0].state == "SUCCEEDED":
        PrjRunStatus["publish_GR"] = "Successful"
        LOGGER.info('New golden records publish')
    else:
        LOGGER.info('FAILURE to publish new golden records')

    LOGGER.info("Updating Golden Record output")
    upd_GR_Out_sts = tbox.project.schema_mapping.jobs.update_unified_dataset(
        tamr_client.projects.by_resource_id(project_id_dict[pipeline_vars["GR_output_project"]]))
    if upd_GR_Out_sts[0].state == "SUCCEEDED":
        PrjRunStatus["update_GR_output"] = "Successful"
        LOGGER.info('Golden Records Output project updated')
    else:
        LOGGER.info('FAILURE to update GR output project')

    LOGGER.info("Updating Golden Record delta")
    upd_GR_Out_sts = tbox.project.schema_mapping.jobs.update_unified_dataset(
        tamr_client.projects.by_resource_id(project_id_dict[pipeline_vars["GR_output_delta_project"]]))
    if upd_GR_Out_sts[0].state == "SUCCEEDED":
        PrjRunStatus["update_GR_Delta"] = "Successful"
        LOGGER.info('Golden Records Output Delta project updated')
    else:
        LOGGER.info('FAILURE to update GR Output Delta project')

    LOGGER.info("Updating Golden Record delta")
    upd_GR_Out_sts = tbox.project.schema_mapping.jobs.update_unified_dataset(
        tamr_client.projects.by_resource_id(project_id_dict[pipeline_vars["GR_mapping_project"]]))
    if upd_GR_Out_sts[0].state == "SUCCEEDED":
        PrjRunStatus["update_GR_Mapping"] = "Successful"
        LOGGER.info('Golden Records Mapping project updated')
    else:
        LOGGER.info('FAILURE to update GR Mapping project')

    for op_name in PrjRunStatus.keys():
        LOGGER.info('Operation ' + op_name + ": " + PrjRunStatus[op_name])

    return PrjRunStatus


if __name__ == "__main__":

    # Run the main function
    main(
        instance_connection_info=CONFIG["my_tamr_instance"],
        pipeline_vars=CONFIG["golden_records_delta_pipeline"],
    )
