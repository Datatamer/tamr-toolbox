Models
========

Operation State
------------------------
.. automodule:: tamr_toolbox.models.operation_state

Project Type
------------------------
.. automodule:: tamr_toolbox.models.project_type

Validation Check
------------------------
.. automodule:: tamr_toolbox.models.validation_check

Project Artifacts
------------------------
.. automodule:: tamr_toolbox.models.project_artifacts

Project Steps
------------------------
.. automodule:: tamr_toolbox.models.project_steps

Attribute Types
------------------------

See https://docs.tamr.com/reference#attribute-types

.. autodata:: tamr_toolbox.models.attribute_type.BOOLEAN
.. autodata:: tamr_toolbox.models.attribute_type.DOUBLE
.. autodata:: tamr_toolbox.models.attribute_type.INT
.. autodata:: tamr_toolbox.models.attribute_type.LONG
.. autodata:: tamr_toolbox.models.attribute_type.STRING
.. autodata:: tamr_toolbox.models.attribute_type.DEFAULT
.. autodata:: tamr_toolbox.models.attribute_type.GEOSPATIAL


.. NOTE:
   `Array` has a recursive dependency on `AttributeType`.
   `sphinx_autodoc_typehint` cannot handle recursive dependencies,
   so reference docs are written manually

.. class:: tamr_toolbox.models.attribute_type.Array(inner_type)

   :param inner_type:
   :type inner_type: :class:`~tamr_toolbox.models.attribute_type.AttrType`

.. NOTE:
   `Map` has a recursive dependency on `AttributeType`.
   `sphinx_autodoc_typehint` cannot handle recursive dependencies,
   so reference docs are written manually

.. class:: tamr_toolbox.models.attribute_type.Map(inner_type)

   :param inner_type:
   :type inner_type: :class:`~tamr_toolbox.models.attribute_type.AttrType`


.. autoclass:: tamr_toolbox.models.attribute_type.Record

.. autofunction:: tamr_toolbox.models.attribute_type.from_json
.. autofunction:: tamr_toolbox.models.attribute_type.to_json