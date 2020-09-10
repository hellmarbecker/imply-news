class DataGeneratorError(Exception):
    "Base exception for this project, all exceptions that can be raised inherit from this class."


class InvalidStateValue(DataGeneratorError):
    "The current model state value is not mapped to a state definition."
