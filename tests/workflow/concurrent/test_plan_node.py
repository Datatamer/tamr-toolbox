"""Tests for the PlanNode dataclass and its methods"""

from tamr_toolbox.workflow.concurrent import PlanNode, PlanNodeStatus

from tamr_toolbox import utils

from tamr_toolbox.utils.testing import mock_api
from tests._common import get_toolbox_root_dir

from tamr_toolbox.models.project_type import ProjectType
from tamr_toolbox.models.project_steps import (
    SchemaMappingSteps,
    CategorizationSteps,
    MasteringSteps,
    GoldenRecordsSteps,
)

CONFIG = utils.config.from_yaml(
    get_toolbox_root_dir() / "tests/mocking/resources/toolbox_test.yaml"
)


@mock_api()
def test_post_init_schema_mapping():
    # create a node for a schema mapping project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "minimal_schema_mapping"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
    )

    # make sure the project type and steps get set correct in the post_init function
    assert test_node.project_type == ProjectType.SCHEMA_MAPPING_RECOMMENDATIONS
    # make sure the project steps are correct
    assert test_node.project_steps == [SchemaMappingSteps.UPDATE_UNIFIED_DATASET]


@mock_api()
def test_post_init_categorization():
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "minimal_categorization"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
    )

    # make sure the project type and steps get set correct in the post_init function
    assert test_node.project_type == ProjectType.CATEGORIZATION
    # make sure the steps are correct and don't include training the model
    assert test_node.project_steps == [
        CategorizationSteps.UPDATE_UNIFIED_DATASET,
        CategorizationSteps.UPDATE_RESULTS_ONLY,
    ]


@mock_api()
def test_post_init_categorization_with_training():
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "minimal_categorization"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
        train=True,
    )

    # make sure the project type and steps get set correct in the post_init function
    assert test_node.project_type == ProjectType.CATEGORIZATION
    # make sure the steps are correct and don't include training the model
    assert test_node.project_steps == [
        CategorizationSteps.UPDATE_UNIFIED_DATASET,
        CategorizationSteps.APPLY_FEEDBACK,
        CategorizationSteps.UPDATE_RESULTS_ONLY,
    ]


@mock_api()
def test_post_init_mastering():
    # create a node for a mastering project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # PlanNodes only know about their project so it is ok to test against an un-linked project
    test_project_name = "minimal_mastering"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
    )

    # make sure the project type and steps get set correct in the post_init function
    assert test_node.project_type == ProjectType.DEDUP
    # make sure the steps are correct and don't include training the model
    assert test_node.project_steps == [
        MasteringSteps.UPDATE_UNIFIED_DATASET,
        MasteringSteps.GENERATE_PAIRS,
        MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS,
        MasteringSteps.UPDATE_CLUSTERS,
        MasteringSteps.PUBLISH_CLUSTERS,
    ]


@mock_api()
def test_post_init_mastering_with_training():
    # create a node for a mastering project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # PlanNodes only know about their project so it is ok to test against an un-linked project
    test_project_name = "minimal_mastering"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
        train=True,
    )

    # make sure the project type and steps get set correct in the post_init function
    assert test_node.project_type == ProjectType.DEDUP
    # make sure the steps are correct and don't include training the model
    assert test_node.project_steps == [
        MasteringSteps.UPDATE_UNIFIED_DATASET,
        MasteringSteps.GENERATE_PAIRS,
        MasteringSteps.APPLY_FEEDBACK,
        MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS,
        MasteringSteps.UPDATE_CLUSTERS,
        MasteringSteps.PUBLISH_CLUSTERS,
    ]


@mock_api()
def test_post_init_golden_records():
    # create a node for a golden records project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    # PlanNodes only know about their project so it is ok to test against an un-linked project
    test_project_name = "chained_minimal_golden_records"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
        train=True,
    )

    # make sure the project type and steps get set correct in the post_init function
    assert test_node.project_type == ProjectType.GOLDEN_RECORDS
    # make sure the steps are correct and don't include training the model
    assert test_node.project_steps == [
        GoldenRecordsSteps.PROFILE_GOLDEN_RECORDS,
        GoldenRecordsSteps.UPDATE_GOLDEN_RECORDS,
        GoldenRecordsSteps.PUBLISH_GOLDEN_RECORDS,
    ]


@mock_api(asynchronous=True)
def test_run_categorization():
    """
    Watch a categorization project run to test out iterating through steps and make sure
    the logic updates state correctly at each state. Includes training to test all steps.
    Also polls to take test that method. This is a big test, but thorough.
    """
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "minimal_categorization"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
        train=True,
    )

    # run next step to kick off the process - could do this all in a loop
    # but want to easily check exact state after each iteration
    test_node = PlanNode.run_next_step(test_node)
    # now make sure we have the right objects for update UD step
    # first make sure the current op got filled in
    assert test_node.current_op is not None
    # now check current step and steps to run
    assert test_node.current_step == CategorizationSteps.UPDATE_UNIFIED_DATASET
    assert test_node.steps_to_run == [
        CategorizationSteps.APPLY_FEEDBACK,
        CategorizationSteps.UPDATE_RESULTS_ONLY,
    ]

    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    # wait for the op finish
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # then run apply feedback
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == CategorizationSteps.APPLY_FEEDBACK
    assert test_node.steps_to_run == [CategorizationSteps.UPDATE_RESULTS_ONLY]
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # and now run update results
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == CategorizationSteps.UPDATE_RESULTS_ONLY
    assert test_node.steps_to_run == []
    test_node.current_op.wait(poll_interval_seconds=1)
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED


@mock_api(asynchronous=True)
def test_run_schema_mapping():
    """
    Watch a schema mapping project run to test out iterating through steps and make sure
    the logic updates state correctly at each state.
    Also polls to take test that method. This is a big test, but thorough.
    """
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "minimal_schema_mapping"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
    )

    # make sure we can poll before starting
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PLANNED

    # run next step to kick off the process - could do this all in a loop
    # but want to easily check exact state after each iteration
    test_node = PlanNode.run_next_step(test_node)
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    # now make sure we have the right objects for update UD step
    # first make sure the current op got filled in
    assert test_node.current_op is not None
    # now check current step and steps to run
    assert test_node.current_step == SchemaMappingSteps.UPDATE_UNIFIED_DATASET
    assert test_node.steps_to_run == []
    # wait for the op finish and check for success
    test_node.current_op.wait(poll_interval_seconds=1)
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED


@mock_api(asynchronous=True)
def test_run_mastering():
    """
    Watch a categorization project run to test out iterating through steps and make sure
    the logic updates state correctly at each state. Includes training to test all steps.
    Also polls to take test that method. This is a big test, but thorough.
    """
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "chained_minimal_mastering"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
        train=True,
    )

    # run next step to kick off the process - could do this all in a loop
    # but want to easily check exact state after each iteration
    test_node = PlanNode.run_next_step(test_node)
    # now make sure we have the right objects for update UD step
    # first make sure the current op got filled in
    assert test_node.current_op is not None
    # now check current step and steps to run
    assert test_node.current_step == MasteringSteps.UPDATE_UNIFIED_DATASET
    assert test_node.steps_to_run == [
        MasteringSteps.GENERATE_PAIRS,
        MasteringSteps.APPLY_FEEDBACK,
        MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS,
        MasteringSteps.UPDATE_CLUSTERS,
        MasteringSteps.PUBLISH_CLUSTERS,
    ]
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    # wait for the op finish
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # then run generate pairs
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == MasteringSteps.GENERATE_PAIRS
    assert test_node.steps_to_run == [
        MasteringSteps.APPLY_FEEDBACK,
        MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS,
        MasteringSteps.UPDATE_CLUSTERS,
        MasteringSteps.PUBLISH_CLUSTERS,
    ]
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # run apply feedback
    print("running apply feedback")
    test_node = PlanNode.run_next_step(test_node)
    print(f"status after triggering run next step {test_node.status}")
    assert test_node.current_step == MasteringSteps.APPLY_FEEDBACK
    assert test_node.steps_to_run == [
        MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS,
        MasteringSteps.UPDATE_CLUSTERS,
        MasteringSteps.PUBLISH_CLUSTERS,
    ]
    # check status
    print(f"current status before polling: {test_node.status}")
    test_node = PlanNode.poll(test_node)
    print(f"status after polling: {test_node.status}")
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # run update high impact pairs
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == MasteringSteps.UPDATE_HIGH_IMPACT_PAIRS
    assert test_node.steps_to_run == [
        MasteringSteps.UPDATE_CLUSTERS,
        MasteringSteps.PUBLISH_CLUSTERS,
    ]
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # run update clusters
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == MasteringSteps.UPDATE_CLUSTERS
    assert test_node.steps_to_run == [MasteringSteps.PUBLISH_CLUSTERS]
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # run publish clusters
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == MasteringSteps.PUBLISH_CLUSTERS
    assert test_node.steps_to_run == []
    test_node.current_op.wait(poll_interval_seconds=1)
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED


@mock_api(asynchronous=True)
def test_run_golden_records():
    """
    Watch a schema mapping project run to test out iterating through steps and make sure
    the logic updates state correctly at each state.
    Also polls to take test that method. This is a big test, but thorough.
    """
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "chained_minimal_golden_records"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
    )

    # run next step to kick off the process - could do this all in a loop
    # but want to easily check exact state after each iteration
    test_node = PlanNode.run_next_step(test_node)
    # now make sure we have the right objects for update UD step
    # first make sure the current op got filled in
    assert test_node.current_op is not None
    # now check current step and steps to run
    assert test_node.current_step == GoldenRecordsSteps.PROFILE_GOLDEN_RECORDS
    assert test_node.steps_to_run == [
        GoldenRecordsSteps.UPDATE_GOLDEN_RECORDS,
        GoldenRecordsSteps.PUBLISH_GOLDEN_RECORDS,
    ]
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    # wait for the op finish
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # now run update golden records
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == GoldenRecordsSteps.UPDATE_GOLDEN_RECORDS
    assert test_node.steps_to_run == [GoldenRecordsSteps.PUBLISH_GOLDEN_RECORDS]
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    # wait for the op finish
    test_node.current_op.wait(poll_interval_seconds=1)
    # check that the node went back to pending
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # now run publish golden records
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == GoldenRecordsSteps.PUBLISH_GOLDEN_RECORDS
    assert test_node.steps_to_run == []
    # check status
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    test_node.current_op.wait(poll_interval_seconds=1)
    test_node = PlanNode.poll(test_node)
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED


@mock_api(asynchronous=True)
def test_run_schema_mapping_with_monitor():
    """
    Watch a schema mapping project run to test out iterating through steps and make sure
    the logic updates state correctly at each state.
    Also polls to take test that method. This is a big test, but thorough.
    """
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "minimal_schema_mapping"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
    )
    # run the next step and monitor it
    test_node = PlanNode.run_next_step(test_node)
    test_node = PlanNode.monitor([test_node], polling_interval=1)
    test_node = test_node[0]
    # there is only one step for schema mapping
    # so make sure after the state change it was successful
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED


@mock_api(asynchronous=True)
def test_run_categorization_with_monitor():
    """
    Watch a categorization project run using the monitor function.
    """
    # create a node for a categorization project
    tamr = utils.client.create(**CONFIG["toolbox_test_instance"])
    test_project_name = "minimal_categorization"
    test_node = PlanNode.PlanNode(
        name=test_project_name,
        project=tamr.projects.by_name(test_project_name),
        current_op=None,
        operations=None,
        priority=0,
    )
    # run the next step and monitor it
    test_node = PlanNode.run_next_step(test_node)
    test_node = PlanNode.monitor([test_node], polling_interval=1)
    test_node = test_node[0]
    # make sure that after monitoring the job it goes back to pending state
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.PENDING_NEXT_STEP

    # run the next step and monitor it
    test_node = PlanNode.run_next_step(test_node)
    assert test_node.current_step == CategorizationSteps.UPDATE_RESULTS_ONLY
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.RUNNING
    test_node = PlanNode.monitor([test_node], polling_interval=1)
    test_node = test_node[0]
    # make sure that after monitoring the job it goes back to pending state
    assert test_node.status == PlanNodeStatus.PlanNodeStatus.SUCCEEDED
