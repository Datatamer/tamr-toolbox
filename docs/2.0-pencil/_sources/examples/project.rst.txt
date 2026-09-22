Projects
======================================================

General
------------------------------------------------------------------------
Add dataset to project and perform schema mapping
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/mastering/add_dataset_and_map.py
  :language: python

Unmap datasets and remove from project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/project/mastering/unmap_dataset_and_remove.py
  :language: python


Categorization
------------------------------------------------------------------------
Run Categorization Simple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/categorization/run_categorization_simple.py
  :language: python

Run Categorization Step-By-Step
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/categorization/run_categorization_verbose.py
  :language: python

Bootstrap a Categorization Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/categorization/bootstrap_taxonomy.py
  :language: python

Obtain Average Confidence for a Specific Tier
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/project/categorization/get_tier_confidence.py
  :language: python

Make changes to taxonomy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/categorization/manage_taxonomy.py
  :language: python
  
Mastering
------------------------------------------------------------------------
Run Mastering Simple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/mastering/run_mastering_simple.py
  :language: python

Run Mastering Step-By-Step
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/mastering/run_mastering_verbose.py
  :language: python

Golden Records
------------------------------------------------------------------------
Run Golden Records Simple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/golden_records/run_golden_records_simple.py
  :language: python

Run Golden Records Step-By-Step
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/golden_records/run_golden_records_verbose.py
  :language: python

Schema Mapping
------------------------------------------------------------------------
Run Schema Mapping Simple
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/schema_mapping/run_schema_mapping_simple.py
  :language: python

Run Schema Mapping Step-By-Step
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/schema_mapping/run_schema_mapping_verbose.py
  :language: python


Transformations
------------------------------------------------------------------------

Edit Unified Transformations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/_common/edit_unified_transformations.py
  :language: python

Edit Unified and Input Transformations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/resources/conf/project.config.yaml
  :language: yaml
.. literalinclude:: ../../examples/scripts/project/_common/edit_transformations.py
  :language: python

Project Movement
------------------------------------------------------------------------


Import Artifacts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/project/_common/import_artifacts.py
  :language: python

Export Artifacts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/snippets/project/_common/export_artifacts.py
  :language: python

Fork Project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/project/_common/fork_project.py
  :language: python

Related Examples
------------------------------------------------------------------------
* :ref:`Run a list of projects <workflow-run-projects>`
