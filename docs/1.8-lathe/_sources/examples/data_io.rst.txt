Data Input/Output
======================================================

DF-Connect
------------------------------------------------------------------------
Connecting to Tamr's Auxiliary Service Df-Connect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Connecting to Tamr's df-connect auxiliary service is straightforward using the tamr-toolbox.
The module that does this is `tamr_toolbox/data_io/df_connect` and the main object is an instance of type `ConnectInfo`.
The easiest way to generate such an object is via reading in a configuration file like so:

.. literalinclude:: ../../examples/resources/conf/connect.config.yaml
  :language: yaml


Ingesting a table as a Tamr dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/data_io/df_connect/ingest_simple.py
  :language: python

Ingesting multiple tables from multiple sources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/connect_multi_ingest.config.yaml
  :language: yaml

.. literalinclude:: ../../examples/snippets/data_io/df_connect/ingest_complex.py
  :language: python


Profiling tables and writing results to a Tamr dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/data_io/df_connect/profile_simple.py
  :language: python


Exporting a Tamr dataset to a single target
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/data_io/df_connect/export_simple.py
  :language: python


Exporting a Tamr dataset to multiple targets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/connect_multi_export.config.yaml
  :language: yaml

.. literalinclude:: ../../examples/snippets/data_io/df_connect/export_complex.py
  :language: python

Exporting a Tamr dataset to Hive
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/data_io/df_connect/export_to_hive.py
  :language: python

Check if Tamr dataset is streamable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/data_io/df_connect/streamable_check.py
  :language: python

Dataframe
------------------------------------------------------------------------

tamr-toolbox provides functions to create a pandas Dataframe from a Tamr dataset and optionally flatten it.
It can also perform basic profiling and validation of pandas Dataframes,
intended to be used for data quality checks before upserting records into Tamr.


Create Dataframe from Tamr Dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/data_io/dataframe/create_dataframe.py
 :language: python


Validate a Dataframe before upserting records to a Tamr Dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/data_io/dataframe/validate_dataframe.py
 :language: python

CSV
------------------------------------------------------------------------

Export CSV from Tamr Dataset to a designated filepath
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/dataset.config.yaml
  :language: yaml
.. literalinclude :: ../../examples/scripts/data_io/csv/export_from_dataset.py
  :language: python


Export CSV from taxonomy to a designated filepath
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/data_io/csv/export_from_taxonomy.py
  :language: python