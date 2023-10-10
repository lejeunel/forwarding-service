class TransferError(Exception):
    def __init__(self, message, operation):

        self.message = message
        self.operation = operation

class AuthenticationError(Exception):
    def __init__(self, message, operation):

        self.message = message
        self.operation = operation

class InitError(Exception):
    def __init__(self, message):

        self.message = message
