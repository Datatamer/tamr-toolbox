Enrichment
======================================================

Translation
------------------------------------------------------------------------

The tamr-toolbox provides functions to translate standardized data and store it in dictionaries
making sure that data is not translated twice.
These translation capabilities can be applied to a Tamr dataset or a Pandas DataFrame.


Translate data within a pandas DataFrame and save dictionaries on disk
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/enrichment/translate_from_file.py
  :language: python


Translate data from Tamr and update dictionary saved as a source dataset on Tamr
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/enrichment/translate_from_tamr.py
  :language: python

Initiate a translation dictionary on Tamr
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/enrichment/initiate_dictionary_on_tamr.py
  :language: python