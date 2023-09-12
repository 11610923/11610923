import jsonschema
from jsonschema import validate

def validate_json(schema, json_dict):

    """
        This method will validate validate an instance under a given schema.

        :param str schema: The schema to validate with.
        
        :param dict json_dict: The instance to validate.

        :returns: The Validation boolean.

        :rtype: boolean
    """
    
    try:
        validate(instance=json_dict, schema=schema)
    except jsonschema.exceptions.ValidationError as err:
        # todo: add logger
        print(err)
        return False
    return True
