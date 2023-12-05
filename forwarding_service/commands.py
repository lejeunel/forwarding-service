from abc import ABC, abstractmethod
from datetime import datetime

from .enum_types import ItemStatus, JobError
from .exceptions import CheckSumException, TransferException
from .models import Item, Transaction


class Command(ABC):
    @abstractmethod
    def execute(self) -> None:
        pass


class CommandWithSession(Command):
    def __init__(self, session, threaded=False):
        self.session = session
        self.threaded = threaded


class UpdateItemStatusCommand(CommandWithSession):
    def execute(self, payload: Transaction | list[Transaction]):
        if self.threaded:
            return

        if isinstance(payload, Transaction):
            payload = [payload]

        for t in payload:
            item = self.session.query(Item).get(t.item_id)
            if t.success:
                item.status = ItemStatus.TRANSFERRED
                item.transferred_at = datetime.now()
                self.session.commit()


class UpdateJobErrorCommand(CommandWithSession):
    """Set error fields of job record according to exception"""

    def execute(self, payload: Transaction | list[Transaction]):
        if self.threaded:
            return

        if isinstance(payload, Transaction):
            payload = [payload]

        exceptions = [t.exception for t in payload if t.exception]
        if len(payload) == 0:
            return
        item = self.session.query(Item).get(payload[0].item_id)
        job = item.job

        for e in exceptions:
            if type(e) == CheckSumException:
                job.error = max(job.error, JobError.CHECKSUM_ERROR)
            elif type(e) == TransferException:
                job.error = max(job.error, JobError.TRANSFER_ERROR)
            job.info["message"] = e.error
            job.info["operation"] = e.operation
            self.session.commit()


class RaiseExceptionCommand(Command):
    def __init__(self, threaded=False):
        self.threaded = threaded

    def execute(self, results: list[Transaction] | Transaction):
        if self.threaded:
            return

        if isinstance(results, Transaction):
            results = [results]

        exceptions = [r.exception for r in results if r.exception]
        if exceptions:
            raise exceptions[0]
