from dewret.renderers.cwl import (
    raw_to_command_input_schema, 
    to_output_schema,
    _raw_to_command_input_schema_internal,
    set_configuration
)

# def test_raw_to_command_input_schema_dict():
#     set_configuration({})
#     result = raw_to_command_input_schema("test_dict", {"a": 1, "b": [2, 3]})
#     expected = {
#         "type": "record",
#         "label": "test_dict",
#         "fields": {
#             "a": {"type": "int", "label": "a"},
#             "b": {
#                 "type": "array",
#                 "items": "int",
#                 "label": "b",
#             },
#         },
#     }
#     assert result == expected

# class TestCommandInputSchema(unittest.TestCase):
#     def test_raw_to_command_input_schema_dict(self):
#         result = raw_to_command_input_schema("test_dict", {"a": 1, "b": [2, 3]})
#         expected = {
#             "type": "record",
#             "label": "test_dict",
#             "fields": {
#                 "a": {"type": "int", "label": "a"},
#                 "b": {
#                     "type": "array",
#                     "items": "int",
#                     "label": "b",
#                 },
#             },
#         }
#         self.assertEqual(result, expected)

#     def test_raw_to_command_input_schema_list(self):
#         result = raw_to_command_input_schema("test_list", [1, 2, 3])
#         expected = {
#             "type": "array",
#             "items": "int",
#             "label": "test_list",
#         }
#         self.assertEqual(result, expected)

#     def test_raw_to_command_input_schema_basic_type(self):
#         result = raw_to_command_input_schema("test_int", 42)
#         expected = {
#             "type": "int",
#             "label": "test_int",
#         }
#         self.assertEqual(result, expected)

#     def test_to_output_schema_record(self):
#         @dataclass
#         class TestRecord:
#             a: int
#             b: str

#         result = to_output_schema("test_record", TestRecord)
#         expected = {
#             "type": "record",
#             "label": "test_record",
#             "fields": {
#                 "a": {"type": "int", "label": "a"},
#                 "b": {"type": "string", "label": "b"},
#             },
#         }
#         self.assertEqual(result, expected)

#     def test_to_output_schema_basic_type(self):
#         result = to_output_schema("test_string", str)
#         expected = {
#             "type": "string",
#             "label": "test_string",
#         }
#         self.assertEqual(result, expected)

#     def test_to_output_schema_with_output_source(self):
#         @dataclass
#         class TestRecord:
#             a: int

#         result = to_output_schema("test_record", TestRecord, "step_result")
#         expected = {
#             "type": "record",
#             "label": "test_record",
#             "fields": {
#                 "a": {"type": "int", "label": "a"},
#             },
#             "outputSource": "step_result",
#         }
#         self.assertEqual(result, expected)

#     def test_raw_to_command_input_schema_internal_dict(self):
#         result = _raw_to_command_input_schema_internal("test_dict", {"key": "value"})
#         expected = {
#             "type": "record",
#             "label": "test_dict",
#             "fields": {
#                 "key": {"type": "string", "label": "key"}
#             },
#         }
#         self.assertEqual(result, expected)

#     def test_raw_to_command_input_schema_internal_list(self):
#         result = _raw_to_command_input_schema_internal("test_list", ["value1", "value2"])
#         expected = {
#             "type": "array",
#             "items": "string",
#             "label": "test_list",
#         }
#         self.assertEqual(result, expected)

#     def test_raw_to_command_input_schema_internal_unset(self):
#         result = _raw_to_command_input_schema_internal("test_unset", Unset)
#         expected = {
#             "type": "string",  # Adjust based on your actual Unset handling
#             "label": "test_unset",
#             "default": None,
#         }
#         self.assertEqual(result, expected)

# if __name__ == "__main__":
#     unittest.main()
