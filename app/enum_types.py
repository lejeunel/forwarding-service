import enum


class ItemStatus(enum.Enum):
    """Allowed status options.

    =============== ====== =======
    Status          Action Result
    =============== ====== =======
    ``PENDING``         Upload N/A
    ``ONHOLD``      Ignore Ignored
    ``TRANSFERRED`` None   Success
    ``ERROR``       None   Failure
    =============== ====== =======
    """

    PENDING = "PENDING"
    ONHOLD = "ONHOLD"
    TRANSFERRED = "TRANSFERRED"
    ERROR = "ERROR"


class JobStatus(enum.Enum):
    """Allowed job status options."""

    INITIATED = "INITIATED"
    PARSING = "PARSING"
    PARSED = "PARSED"
    TRANSFERRING = "TRANSFERRING"
    DONE = "DONE"


class JobError(enum.IntEnum):

    NONE = 0
    INIT_ERROR = 1
    S3_ERROR = 2
    CHECKSUM_ERROR = 3
    VAULT_ERROR = 4
