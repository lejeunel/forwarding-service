import enum


class ItemStatus(enum.IntEnum):
    """Allowed status options.

    =============== ====== =======
    Status          Action Result
    =============== ====== =======
    ``PENDING``         Upload N/A
    ``TRANSFERRED`` None   Success
    ``ERROR``       None   Failure
    =============== ====== =======
    """

    PENDING = 0
    TRANSFERRED = 1


class JobStatus(enum.IntEnum):
    """Allowed job status options."""

    INITIATED = 0
    PARSING = 1
    PARSED = 2
    TRANSFERRING = 3
    DONE = 4


class JobError(enum.IntEnum):

    NONE = 0
    INIT_ERROR = 1
    TRANSFER_ERROR = 2
    CHECKSUM_ERROR = 3
    AUTH_ERROR = 4
