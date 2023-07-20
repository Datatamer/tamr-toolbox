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

======================================================

Address Validation
------------------------------------------------------------------------

The tamr-toolbox provides functions to validate address data and store it (up to the cache time
limit) to ensure addresses are not repeatedly validated.
These validation capabilities can be applied to a Tamr dataset or local data.


Validate data within a local CSV and save results on disk
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/enrichment/validate_addresses_from_file.py
  :language: python


Validate data from Tamr and update validation data mapping saved as a source dataset on Tamr
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/enrichment/validate_addresses_from_tamr.py
  :language: python

Initiate an address validation mapping dataset on Tamr
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. literalinclude:: ../../examples/scripts/enrichment/initiate_address_validation_on_tamr.py
  :language: python