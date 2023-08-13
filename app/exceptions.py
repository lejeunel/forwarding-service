class TransferError(Exception):
    def __init__(self, message, operation):

        self.message = message
        self.operation = operation


class AuthenticationError(Exception):
    def __init__(self, message, operation):

        self.message = message
        self.operation = operation
