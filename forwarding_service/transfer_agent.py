from concurrent.futures import ThreadPoolExecutor

from .commands import Command
from .exceptions import RemoteException
from .models import Transaction
from .reader_writer import BaseReader, BaseWriter, ReaderWriter
from .utils import chunks


class TransferAgent(ReaderWriter):
    def __init__(
        self,
        reader: BaseReader,
        writer: BaseWriter,
        post_transaction_commands: list[Command] = [],
        post_batch_commands: list[Command] = [],
        n_threads: int = 30,
        split_ratio: float = 0.1,
    ):
        super().__init__(reader=reader, writer=writer, do_checksum=True)
        self.post_transaction_commands = post_transaction_commands
        self.post_batch_commands = post_batch_commands
        self.n_threads = n_threads

        assert (
            split_ratio <= 1
        ), f"got split_ratio = {split_ratio}. Should be <= 1"
        self.split_ratio = split_ratio

    def run(self, transactions: list[Transaction]):
        n_threads = min(self.n_threads, len(transactions))
        if n_threads > 1:
            for b in self._split_to_batches(transactions):
                self._run_threaded(b)
        else:
            self._run_sequential(transactions)

    def _split_to_batches(self, transactions: list[Transaction]):
        batch_size = round(self.split_ratio * len(transactions))
        n_batches = round(len(transactions) / batch_size)

        batches = chunks(transactions, n_batches)
        return batches

    def _run_threaded(self, transactions: list[Transaction]):
        with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
            _ = [executor.submit(self._transfer_one, t) for t in transactions]

        for cmd in self.post_batch_commands:
            cmd.execute(transactions)
        return transactions

    def _run_sequential(self, transactions: list[Transaction]):
        for t in transactions:
            self._transfer_one(t)

        for cmd in self.post_batch_commands:
            cmd.execute(transactions)

    def _transfer_one(self, transaction: Transaction):
        try:
            self.send(transaction.input, transaction.output)
            transaction.success = True
        except RemoteException as e:
            transaction.exception = e

        for cmd in self.post_transaction_commands:
            cmd.execute(transaction)

        return transaction
