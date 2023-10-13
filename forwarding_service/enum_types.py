import enum


class ItemStatus(enum.IntEnum):

    PENDING = 0
    TRANSFERRED = 1


class JobStatus(enum.IntEnum):

    INIT = 0
    PARSED = 1
    DONE = 2


class JobError(enum.IntEnum):

    NONE = 0
    TRANSFER_ERROR = 1
    CHECKSUM_ERROR = 2
    AUTH_ERROR = 3
