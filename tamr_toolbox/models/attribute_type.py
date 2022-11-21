"""Enum and dataclasses representing attribute types in Tamr"""
from enum import Enum
from dataclasses import dataclass
from typing import ClassVar, Tuple, Union
from tamr_toolbox.models.data_type import JsonDict
from copy import deepcopy

PrimitiveType = Enum("PrimitiveType", ["BOOLEAN", "DOUBLE", "INT", "LONG", "STRING"])

# primitive type aliases
DOUBLE = PrimitiveType.DOUBLE
BOOLEAN = PrimitiveType.BOOLEAN
INT = PrimitiveType.INT
LONG = PrimitiveType.LONG
STRING = PrimitiveType.STRING


@dataclass(frozen=True)
class Array:
    """See https://docs.tamr.com/reference#attribute-types
    NOTE:
        `sphinx_autodoc_typehints` cannot handle forward reference to `AttributeType`,
        so reference docs are written manually for this type
    Args:
        inner_type
    """

    _tag: ClassVar[str] = "ARRAY"
    inner_type: "AttributeType"


@dataclass(frozen=True)
class Map:
    """See https://docs.tamr.com/reference#attribute-types
    NOTE:
        `sphinx_autodoc_typehints` cannot handle forward reference to `AttributeType`,
        so reference docs are written manually for this type
    Args:
        inner_type
    """

    _tag: ClassVar[str] = "MAP"
    inner_type: "AttributeType"


@dataclass(frozen=True)
class SubAttribute:
    """An attribute which is itself a property of another attribute.
    See https://docs.tamr.com/reference#attribute-types
    NOTE:
        `sphinx_autodoc_typehints` cannot handle forward reference to `AttributeType`,
        so reference docs are written manually for this type
    Args:
        name: Name of sub-attribute
        type: See https://docs.tamr.com/reference#attribute-types
        is_nullable: If this sub-attribute can be null
    """

    name: str
    type: "AttributeType"
    is_nullable: bool


@dataclass(frozen=True)
class Record:
    """See https://docs.tamr.com/reference#attribute-types
    Args:
        attributes
    """

    _tag: ClassVar[str] = "RECORD"
    attributes: Tuple[SubAttribute, ...]


ComplexType = Union[Array, Map, Record]

AttributeType = Union[PrimitiveType, ComplexType]

# complex type aliases
DEFAULT: AttributeType = Array(STRING)
GEOSPATIAL: AttributeType = Record(
    attributes=(
        SubAttribute(name="point", is_nullable=True, type=Array(DOUBLE)),
        SubAttribute(name="multiPoint", is_nullable=True, type=Array(Array(DOUBLE))),
        SubAttribute(name="lineString", is_nullable=True, type=Array(Array(DOUBLE))),
        SubAttribute(name="multiLineString", is_nullable=True, type=Array(Array(Array(DOUBLE)))),
        SubAttribute(name="polygon", is_nullable=True, type=Array(Array(Array(DOUBLE)))),
        SubAttribute(
            name="multiPolygon", is_nullable=True, type=Array(Array(Array(Array(DOUBLE))))
        ),
    )
)


def from_json(data: JsonDict) -> AttributeType:
    """Make an attribute type from JSON data (deserialize)
    Args:
        data: JSON data from Tamr server
    """
    base_type = data.get("baseType")
    if base_type is None:
        raise ValueError("Missing required field 'baseType'.")

    for primitive in PrimitiveType:
        if base_type == primitive.name:
            return primitive

    if base_type == Array._tag:
        inner_type = data.get("innerType")
        if inner_type is None:
            raise ValueError("Missing required field 'innerType' for Array type.")
        return Array(inner_type=from_json(inner_type))
    elif base_type == Map._tag:
        inner_type = data.get("innerType")
        if inner_type is None:
            raise ValueError("Missing required field 'innerType' for Map type.")
        return Map(inner_type=from_json(inner_type))
    elif base_type == Record._tag:
        attributes = data.get("attributes")
        if attributes is None:
            raise ValueError("Missing required field 'attributes' for Record type.")
        return Record(attributes=tuple([_subattribute_from_json(attr) for attr in attributes]))
    else:
        raise ValueError(f"Unrecognized 'baseType': {base_type}")


def _subattribute_from_json(data: JsonDict) -> SubAttribute:
    """Make a SubAttribute from JSON data (deserialize)
    Args:
        data: JSON data received from Tamr server.
    """

    cp = deepcopy(data)
    d = {}
    d["name"] = cp["name"]
    d["is_nullable"] = cp["isNullable"]
    d["type"] = from_json(cp["type"])
    return SubAttribute(**d)


def to_json(attr_type: AttributeType) -> JsonDict:
    """Serialize attribute type to JSON
    Args:
        attr_type: Attribute type to serialize
    """
    if isinstance(attr_type, PrimitiveType):
        return {"baseType": attr_type.name}
    elif isinstance(attr_type, (Array, Map)):
        return {"baseType": type(attr_type)._tag, "innerType": to_json(attr_type.inner_type)}
    elif isinstance(attr_type, Record):

        return {
            "baseType": type(attr_type)._tag,
            "attributes": [
                {
                    "name": subattr.name,
                    "type": to_json(subattr.type),
                    "isNullable": subattr.is_nullable,
                }
                for subattr in attr_type.attributes
            ],
        }
    else:
        raise TypeError(attr_type)
