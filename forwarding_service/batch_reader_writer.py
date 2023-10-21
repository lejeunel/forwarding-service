from concurrent.futures import ThreadPoolExecutor

from .commands import Command
from .exceptions import RemoteException
from .models import Item, TransferItemResult
from .reader_writer import BaseReader, BaseWriter, ReaderWriter
from .utils import chunks


class BatchReaderWriter(ReaderWriter):
    def __init__(
        self,
        reader: BaseReader,
        writer: BaseWriter,
        post_item_commands: list[Command] = [],
        post_batch_commands: list[Command] = [],
        n_threads: int = 30,
        split_ratio: float = 0.1,
    ):
        super().__init__(reader=reader, writer=writer, do_checksum=True)
        self.post_item_commands = post_item_commands
        self.post_batch_commands = post_batch_commands
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
        job = items[0].job
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            results = [
                executor.submit(self._transfer_one_item, item) for item in items
            ]

        results = [r._result for r in results]

        for cmd in self.post_batch_commands:
            cmd.execute(results)

    def _run_sequential(self, items: list[Item]):
        results = []
        for item in items:
            result = self._transfer_one_item(item)
            results.append(result)

        for cmd in self.post_batch_commands:
            cmd.execute(results)

    def _transfer_one_item(self, item: Item):
        result = TransferItemResult(item=item, success=True)

        try:
            self.send(item.in_uri, item.out_uri)
        except RemoteException as e:
            result.exception = e
            result.success = False

        for cmd in self.post_item_commands:
            cmd.execute(result)

        return result
