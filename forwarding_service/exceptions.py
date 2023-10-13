class RemoteException(Exception):
    def __init__(self, message, operation):

        self.message = message
        self.operation = operation

class TransferError(RemoteException):
    pass

class AuthenticationError(RemoteException):
    pass

class CheckSumError(RemoteException):
    pass

class InitSrcError(Exception):
    def __init__(self, message):

        self.message = message

class InitDuplicateJobError(Exception):
    def __init__(self, message):

        self.message = message
