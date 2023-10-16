#!/usr/bin/env python3
from abc import ABC, abstractmethod
from datetime import datetime

from .batch_reader_writer import TransferItemResult
from .enum_types import ItemStatus, JobError, JobStatus
from .exceptions import CheckSumException, RemoteException
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

class CommitChangesCommand(Command):
    def __init__(self, session):
        self.session = session

    def execute(self, *args, **kwargs):
        self.session.commit()


class UpdateJobDoneCommand(Command):

    def execute(self, job: Job):
        if job.num_done_items() == len(job.items):
            job.status = JobStatus.DONE

class RaiseJobExceptionCommand(Command):
    def execute(self, job: Job):
        if job.error == JobError.TRANSFER_ERROR:
            raise RemoteException(
                error=job.info["message"], operation=job.info["operation"]
            )

        elif job.error == JobError.CHECKSUM_ERROR:
            raise CheckSumException(
                error=job.info['message'], operation=job.info['operation']
            )
