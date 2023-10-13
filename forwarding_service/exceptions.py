class RemoteException(Exception):
    def __init__(self, message, operation):

        self.message = message
        self.operation = operation

class TransferException(RemoteException):
    pass

class AuthenticationError(RemoteException):
    pass

class CheckSumException(RemoteException):
    pass

class InitSrcException(Exception):
    def __init__(self, message):

        self.message = message

class InitDuplicateJobException(Exception):
    def __init__(self, message):

        self.message = message
