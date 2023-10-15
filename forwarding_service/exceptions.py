class RemoteException(Exception):
    def __init__(self, error, operation):

        self.error = error
        self.operation = operation

class TransferException(RemoteException):
    pass

class AuthenticationError(RemoteException):
    pass

class CheckSumException(RemoteException):
    pass

class InitException(Exception):
    def __init__(self, error):

        self.error = error


class InitSrcException(Exception):
    def __init__(self, error):

        self.error = error

class InitDuplicateJobException(Exception):
    def __init__(self, error):

        self.error = error
