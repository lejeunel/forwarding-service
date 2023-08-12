import enum


class FileStatus(enum.Enum):
    """Allowed file status options.

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


class JobError(enum.Enum):

    INIT_ERROR = "INIT_ERROR"
    S3_ERROR = "S3_ERROR"
    CHECKSUM_ERROR = "CHECKSUM_ERROR"
    VAULT_ERROR = "VAULT_ERROR"
    NONE = "NONE"
