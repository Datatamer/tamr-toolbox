Utilities
======================================================

Logging
-----------------------------------
How to log
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/utils/logger/how_to_log.py
  :language: python


Operation
-----------------------------------
How to use operation functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/utils/operation/job_operations.py
  :language: python


Testing
-----------------------------------
How to use the mock_api decorator for testing your code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml

.. literalinclude:: ../../examples/snippets/utils/testing/mock_api_usage.py
  :language: python


*The test script above the following response log files when located at in my_project/tests/test_function_with_api_calls.py*

**response_logs_dir:** my_project/tests/response_logs/test_function_with_api_calls

**response_log_file:** test_get_latest_operation.ndjson

.. literalinclude:: ../../examples/snippets/utils/testing/response_logs/mock_api_usage/test_get_latest_operation.ndjson
  :language: json

**response_logs_dir:** my_project/tests/response_logs/test_function_with_api_calls

**response_log_file:** test_operation_by_id_details__operation_id=110_expected_description=Update Pairs_expected_state=CANCELED.ndjson

.. literalinclude:: ../../examples/snippets/utils/testing/response_logs/mock_api_usage/test_operation_by_id_details__operation_id=110_expected_description=Update Pairs_expected_state=CANCELED.ndjson
  :language: json

**response_logs_dir:** my_project/tests/response_logs/test_function_with_api_calls

**response_log_file:** test_operation_by_id_details__operation_id=117_expected_description=Predict Pairs_expected_state=SUCCEEDED.ndjson

.. literalinclude:: ../../examples/snippets/utils/testing/response_logs/mock_api_usage/test_operation_by_id_details__operation_id=117_expected_description=Predict Pairs_expected_state=SUCCEEDED.ndjson
  :language: json

**response_logs_dir:** my_project/tests/my_custom_dir

**response_log_file:** test_get_latest_operation.ndjson

.. literalinclude:: ../../examples/snippets/utils/testing/my_custom_dir/test_operation_by_id.ndjson
  :language: json


Upstream
---------------------------------------------------------------
How to get a list of projects upstream from a specified project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/utils/upstream/get_upstream_projects.py
  :language: python


How to get a list of datasets upstream from a specified dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/resources/conf/dataset.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/utils/upstream/get_upstream_datasets.py
  :language: python


Downstream
-------------------------------------------------------------------------------
How to get a list of datasets and projects downstream from a specified dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/utils/downstream/cleanup_downstream_resources.py
  :language: python