#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from .enum_types import ItemStatus, JobError
from .exceptions import CheckSumException, RemoteException, TransferException
from .models import Job
from .reader_writer import BaseReaderWriter


class BatchReaderWriter:
    def __init__(self,
                 reader_writer: BaseReaderWriter = BaseReaderWriter(),
                 n_threads: int = 10):
        self.reader_writer = reader_writer
        self.n_threads = n_threads

    def _post_transfer_update(self, job: Job, result: dict):
        item = result["item"]

        if result["success"]:
            item.status = ItemStatus.TRANSFERRED
            item.transferred_at = datetime.now()
        else:
            job.error = result['error']
            job.info = result["message"]
            raise RemoteException(
                message=result["message"], operation=result["operation"]
            )

    def run(self, job: Job):
        if self.n_threads > 1:
            self.n_threads = min(self.n_threads, len(job.items))
            self._run_parallel(job)
        else:
            self._run_sequential(job)

        return job


    def _run_parallel(self, job: Job):
        items = [item for item in job.items if item.status != ItemStatus.TRANSFERRED]
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            results = [executor.submit(self._transfer_one_item, item) for item in items]

        results = [r._result for r in results]
        for result in results:
            self._post_transfer_update(job, result)

    def _run_sequential(self, job: Job):
        items = [item for item in job.items if item.status != ItemStatus.TRANSFERRED]
        for item in items:
            result = self._transfer_one_item(item)
            self._post_transfer_update(job, result)

    def _transfer_one_item(self, item):
        try:
            self.reader_writer.send(item.in_uri, item.out_uri)
        except RemoteException as e:
            result =  {
                "item": item,
                "success": False,
                "message": e.message,
                "operation": e.operation
            }
            if type(e) == CheckSumException:
                result['error'] = JobError.CHECKSUM_ERROR
            elif type(e) == TransferException:
                result['error'] = JobError.TRANSFER_ERROR
            return result

        return {"item": item, "success": True, "message": "", "operation": "", 'error': JobError.NONE}
