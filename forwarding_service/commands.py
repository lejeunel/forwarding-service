#!/usr/bin/env python3
from abc import ABC, abstractmethod
from datetime import datetime

from .models import TransferItemResult
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import CheckSumException, RemoteException, TransferException
from .models import Job


class Command(ABC):
    @abstractmethod
    def execute(self) -> None:
        pass


class UpdateItemStatusCommand(Command):
    def execute(self, result: TransferItemResult):
        item = result.item
        if result.success:
            item.status = ItemStatus.TRANSFERRED
            item.transferred_at = datetime.now()


class HandleExceptionCommand(Command):
    def execute(self, result: TransferItemResult):
        if result.exception:
            job = result.item.job
            exception = result.exception

            if type(exception) == CheckSumException:
                job.error = max(job.error, JobError.CHECKSUM_ERROR)
            elif type(exception) == TransferException:
                job.error = max(job.error, JobError.TRANSFER_ERROR)
            job.info["message"] = exception.error
            job.info["operation"] = exception.operation


class CommitCommand(Command):
    def __init__(self, session, threaded=False):
        self.session = session
        self.threaded = threaded

    def execute(self, *args, **kwargs):
        if not self.threaded:
            self.session.commit()


class UpdateJobDoneCommand(Command):
    def __init__(self, job: Job):
        self.job = job

    def execute(self, *args, **kwargs):
        if self.job.num_done_items() == len(self.job.items):
            self.job.status = JobStatus.DONE


class RaiseFirstExceptionCommand(Command):
    def __init__(self, threaded=False):
        self.threaded = threaded

    def execute(self, results: list[TransferItemResult] | TransferItemResult):
        if self.threaded:
            return

        if isinstance(results, TransferItemResult):
            results = [results]

        exceptions = [r.exception for r in results if r.exception]
        if exceptions:
            raise exceptions[0]
