#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor

from .enum_types import ItemStatus, JobError
from .exceptions import CheckSumException, RemoteException, TransferException
from .models import Job, Item
from .reader_writer import BaseReaderWriter
from dataclasses import dataclass

@dataclass
class TransferItemResult:
    item: Item
    success: bool = True
    job_error: JobError = JobError.NONE
    message: str = ''
    operation: str = ''


class BatchReaderWriter:
    def __init__(self,
                 reader_writer: BaseReaderWriter = BaseReaderWriter(),
                 post_item_callbacks : list = [],
                 post_batch_callbacks : list = [],
                 n_threads: int = 10):
        self.reader_writer = reader_writer
        self.post_item_callbacks = post_item_callbacks
        self.post_batch_callbacks = post_batch_callbacks
        self.n_threads = n_threads

    def run(self, job: Job):
        n_threads = min(self.n_threads, len(job.items))
        if n_threads > 1:
            results = self._run_parallel(job)
        else:
            results = self._run_sequential(job)

        for clbk in self.post_batch_callbacks:
            clbk(results)

        return job


    def _run_parallel(self, job: Job):
        items = [item for item in job.items if item.status != ItemStatus.TRANSFERRED]
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            results = [executor.submit(self._transfer_one_item, item) for item in items]

        results = [r._result for r in results]

        return results

    def _run_sequential(self, job: Job):
        items = [item for item in job.items if item.status != ItemStatus.TRANSFERRED]
        results = []
        for item in items:
            result = self._transfer_one_item(item)
            results.append(result)
            for clbk in self.post_item_callbacks:
                clbk(result, self.n_threads)
        return results


    def _transfer_one_item(self, item):
        result = TransferItemResult(item=item)

        job = item.job
        try:
            self.reader_writer.send(item.in_uri, item.out_uri)
        except RemoteException as e:
            if type(e) == CheckSumException:
                job.error = max(job.error, JobError.CHECKSUM_ERROR)
            elif type(e) == TransferException:
                job.error = max(job.error, JobError.TRANSFER_ERROR)
            job.info['message'] = e.error
            job.info['operation'] = e.operation
            result =  TransferItemResult(
                item= item,
                success= False,
            )

        for clbk in self.post_item_callbacks:
            clbk(result, self.n_threads)

        return result
