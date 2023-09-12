import jsonschema
from jsonschema import validate

def validate_json(schema, json_dict):
    try:
        validate(instance=json_dict, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        # todo: add logger
        print(err)
        return False
    return True
