class DataGeneratorError(Exception):
    "Base exception for this project, all exceptions that can be raised inherit from this class."

class InvalidStateException(DataGeneratorError):
    "The current model state value is not mapped to a state definition."

class InvalidTransitionException(DataGeneratorError):
    "No legal transition."
