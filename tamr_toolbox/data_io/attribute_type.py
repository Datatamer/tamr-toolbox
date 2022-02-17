"""Enum representing attribute types in Tamr https://docs.tamr.com/new/reference/attribute-types#examples"""
from enum import Enum

class AttributeType(Enum):
    """A parent class representing Tamr attribute types"""
    pass

class PrimitiveType(AttributeType, Enum):
    """A dataclass representing primitive attribute types
    Args:
        STRING: Primitive String Type
        DOUBLE: Primitive Double Type
        INT: Primitive Int Type
        LONG: Primitive Long Type
        NULL: Primitive Null Type 
    """

    STRING = {"baseType": "STRING"}
    DOUBLE = {"baseType": "DOUBLE"}
    INT = {"baseType": "INT"}
    LONG = {"baseType": "LONG"}
    NULL = {"baseType": "NULL"}

class ArrayType(AttributeType, Enum):
    """A dataclass representing array attribute types
    Args:
        STRING: Array String Type
        DOUBLE: Array Double Type
        INT: Array Int Type
        LONG: Array Long Type
        NULL: Array Null Type 
    """

    STRING = {"baseType": "ARRAY", "innerType": {"baseType": "STRING"}}
    DOUBLE = {"baseType": "ARRAY", "innerType": {"baseType": "DOUBLE"}}
    INT = {"baseType": "ARRAY", "innerType": {"baseType": "INT"}}
    LONG = {"baseType": "ARRAY", "innerType": {"baseType": "LONG"}}
    NULL = {"baseType": "ARRAY", "innerType": {"baseType": "NULL"}}
    
