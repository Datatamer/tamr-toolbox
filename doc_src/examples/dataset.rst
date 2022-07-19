Dataset
======================================================

Manage Datasets
------------------------------------------------------------------------

Create a dataset
~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude :: ../../examples/resources/conf/create_dataset.config.yaml
 :language: yaml

.. literalinclude :: ../../examples/scripts/dataset/manage/create_dataset.py
 :language: python

Migrate dataset definition changes from a source to target instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude :: ../../examples/resources/conf/migrate_dataset.config.yaml
 :language: yaml
 
.. literalinclude :: ../../examples/snippets/dataset/manage/migrate_dataset_changes.py
 :language: python

Add attributes to a dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude :: ../../examples/snippets/dataset/manage/add_complex_attribute.py
 :language: python

Remove an attribute from a dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude :: ../../examples/snippets/dataset/manage/delete_attribute.py
 :language: python
 
Dataset Profiles
------------------------------------------------------------------------
 
Create and retrieve a profile for a dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude :: ../../examples/snippets/dataset/_profile/create_retrieve_profile.py
 :language: python
