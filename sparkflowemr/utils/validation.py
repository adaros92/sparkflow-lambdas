

def validate_event_inputs(event: dict, required_inputs: list, valid_values_per_input: dict) -> None:
    """Validates that the input provided to the Lambda is correct

    :param event the event dictionary passed in as Lambda input
    :param required_inputs a list of required attributes the event should have
    :param valid_values_per_input a dictionary of required attribute to valid values
    """
    for required_input in required_inputs:
        if required_input not in event:
            raise ValueError("{0} must be part of the event input passed in".format(required_input))
        valid_values = valid_values_per_input.get(required_input, [])
        if len(valid_values) > 0:
            input_passed_in = event[required_input]
            if input_passed_in not in valid_values:
                raise ValueError("input must be one of {0} but found {1}".format(valid_values, input_passed_in))
