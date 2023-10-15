#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from .enum_types import ItemStatus, JobError
from .exceptions import CheckSumException, RemoteException, TransferException
from .models import Item
from .reader_writer import BaseReader, BaseWriter, ReaderWriter
from .utils import chunks, get_todo_items


@dataclass
class TransferItemResult:
    item: Item
    success: bool = True
    job_error: JobError = JobError.NONE
    message: str = ""
    operation: str = ""


class BatchReaderWriter(ReaderWriter):
    def __init__(
        self,
        reader: BaseReader,
        writer: BaseWriter,
        post_item_callbacks: list = [],
        post_batch_callbacks: list = [],
        n_threads: int = 30,
        split_ratio: float = 0.1,
    ):
        super().__init__(reader=reader, writer=writer, do_checksum=True)
        self.post_item_callbacks = post_item_callbacks
        self.post_batch_callbacks = post_batch_callbacks
        self.n_threads = n_threads

        assert (
            split_ratio <= 1
        ), f"got split_ratio = {split_ratio}. Should be <= 1"
        self.split_ratio = split_ratio

    def run(self, items: list[Item]):
        n_threads = min(self.n_threads, len(items))
        if n_threads > 1:
            for b in self._split_to_batches(items):
                self._run_threaded(b)
        else:
            self._run_sequential(items)

    def _split_to_batches(self, items: list[Item]):
        batch_size = round(self.split_ratio * len(items))
        n_batches = round(len(items) / batch_size)

        batches = chunks(items, n_batches)
        return batches


    def _run_threaded(self, items: list[Item]):
        items = get_todo_items(items)
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            results = [
                executor.submit(self._transfer_one_item, item) for item in items
            ]

        results = [r._result for r in results]

        for clbk in self.post_batch_callbacks:
            clbk(results)

    def _run_sequential(self, items: list[Item]):
        items = get_todo_items(items)
        results = []
        for item in items:
            result = self._transfer_one_item(item)
            results.append(result)
            for clbk in self.post_item_callbacks:
                clbk(result, self.n_threads)

        for clbk in self.post_batch_callbacks:
            clbk(results)

    def _transfer_one_item(self, item: Item):
        result = TransferItemResult(item=item)

        job = item.job
        try:
            self.send(item.in_uri, item.out_uri)
        except RemoteException as e:
            if type(e) == CheckSumException:
                job.error = max(job.error, JobError.CHECKSUM_ERROR)
            elif type(e) == TransferException:
                job.error = max(job.error, JobError.TRANSFER_ERROR)
            job.info["message"] = e.error
            job.info["operation"] = e.operation
            result = TransferItemResult(
                item=item,
                success=False,
            )

        for clbk in self.post_item_callbacks:
            clbk(result, self.n_threads)

        return result
