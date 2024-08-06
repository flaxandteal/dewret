from unittest.mock import patch, MagicMock
from dewret.renderers.cwl import cwl_type_from_value, to_cwl_type, set_configuration, configuration
from dewret.tasks import factory
from dewret.workflow import Unset
from queue import Queue
from collections import OrderedDict
from types import UnionType
from collections.abc import Iterable
from typing import Tuple, Union, List

############################################
#
# Tests for `to_cwl_type` method in dewret.renderers.cwl
#
############################################

def return_union_type(a) -> UnionType:
    return a

def test_cwl_basic_types():
    integer = type(12)
    boolean = type(False)
    dictionary = type({})
    fl = type(12.2)
    string = type("12")
    byt = type(bytes([104, 101, 108, 108, 111]))
    
    assert "int" == to_cwl_type(integer)
    assert "boolean" == to_cwl_type(boolean)
    assert "record" == to_cwl_type(dictionary)
    assert "float" == to_cwl_type(fl)
    assert "string" == to_cwl_type(string)
    assert "bytes" == to_cwl_type(byt)

def test_allow_complex_types():
    custom_config = {
        "allow_complex_types": True,
        "factories_as_params": False
    }
    set_configuration(custom_config)

    queue = Queue(1)
    queue.put(4)

    od = OrderedDict()
    od['one'] = 1
    assert "Queue" == to_cwl_type(type(queue))
    assert "OrderedDict" == to_cwl_type(type(od))

def test_union_type():

    uti = return_union_type(4)
    uts = return_union_type("4")

    assert "int" == to_cwl_type(type(uti))
    assert "string" == to_cwl_type(type(uts))

def test_list_with_multiple_basic_types():
    # Set default configurations
    set_configuration({})
    typ = list[int]
    result = to_cwl_type(typ)
    assert result == {"type": "array", "items": "int"}

def test_list_with_single_basic_type():
    # Set default configurations
    set_configuration({})
    result = to_cwl_type(List[int])
    assert result == {"type": "array", "items": "int"}

def test_tuple_with_multiple_basic_types():
    # Set default configurations
    set_configuration({})
    result = to_cwl_type(Tuple[int, str])
    expected = {
        "type": "array",
        "items": [{"type": "int"}, {"type": "string"}],
    }
    assert result == expected, f"Expected {expected}, but got {result}"

def test_tuple_with_single_basic_type():
    # Set default configurations
    set_configuration({})
    result = to_cwl_type(Tuple[int])
    expected = {"type": "array", "items": "int"}
    assert result == expected, f"Expected {expected}, but got {result}"


############################################
#
# Tests for `cwl_type_from_value` method in dewret.renderers.cwl
#
############################################


def test_cwl_type_from_int():
    val = 42
    result = cwl_type_from_value(val)
    expected = "int"
    assert result == expected

def test_cwl_type_from_float():
    val = 3.14
    result = cwl_type_from_value(val)
    expected = "float"
    assert result == expected

def test_cwl_type_from_str():
    val = "hello"
    result = cwl_type_from_value(val)
    expected = "string"
    assert result == expected

def test_cwl_type_from_bytes():
    val = b"binary data"
    result = cwl_type_from_value(val)
    expected = "bytes"
    assert result == expected

def test_cwl_type_from_dict():
    val = {"key": "value"}
    result = cwl_type_from_value(val)
    expected = "record"
    assert result == expected


# Question: Why doesn't it work with complex types

# def test_cwl_type_from_list_of_ints(): 
#     # Set default configurations
#     set_configuration({})
#     val = [1,2,3]
#     result = cwl_type_from_value(val)
#     expected = {"type": "array", "items": "int"}
#     assert result == expected

# def test_cwl_type_from_tuple_of_ints_and_strs():
#     val = (1, "text")
#     result = cwl_type_from_value(val)
#     expected = {
#         "type": "array",
#         "items": ["int", "string"],
#     }
#     assert result == expected

# def test_cwl_type_from_unset():
#     val = Unset 
#     result = cwl_type_from_value(val)
#     expected = "unset"
#     print("**********************",result)
#     assert result == expected

# def test_cwl_type_from_object_with_type_attribute():
#     class CustomType:
#         __type__ = "custom_type"

#     val = CustomType()
#     result = cwl_type_from_value(val)
#     expected = "custom_type"  # Assuming to_cwl_type maps this correctly
#     assert result == expected

# def test_cwl_type_from_empty_list():
#     val = []
#     result = cwl_type_from_value(val)
#     expected = {"type": "array", "items": "unset"}  # Adjust as needed
#     assert result == expected

# def test_cwl_type_from_empty_tuple():
#     val = ()
#     result = cwl_type_from_value(val)
#     expected = {"type": "array", "items": "unset"}  # Adjust as needed
#     assert result == expected
