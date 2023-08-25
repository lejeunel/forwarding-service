#!/usr/bin/env python3
import hashlib
from base64 import b64encode
from multiprocessing import current_process
from datetime import datetime

from .base import BaseReader, BaseWriter
from .enum_types import ItemStatus, JobError
from .exceptions import TransferError


class ItemUploader:
    def __init__(
        self,
        reader: BaseReader,
        writer: BaseWriter,
        do_checksum=True,
    ):
        self.reader = reader
        self.writer = writer
        self.do_checksum = do_checksum

    @staticmethod
    def compute_sha256_checksum(bytes_):
        checksum = hashlib.sha256(bytes_.getbuffer())
        checksum = b64encode(checksum.digest()).decode()
        return checksum

    def upload(self, in_uri: str, out_uri: str):
        """
        Upload a single item.

        NOTE: As this function must work across parallel processes, we return
        deserialized objects and update the database from main process. This is because
        SQLite does not allow write-concurrency, i.e. several processes attempting to
        write simultaneously to the same DB.
        """
        try:
            print(f"[{current_process().pid}] {in_uri} -> {out_uri}")
            bytes_, type_ = self.reader(in_uri)

            checksum = None
            if self.do_checksum:
                checksum = self.compute_sha256_checksum(bytes_)

            self.writer(bytes_, out_uri, type_, checksum)

        except TransferError as e:
            return {
                "item": {"status": ItemStatus.PENDING},
                "job": {
                    "error": JobError.TRANSFER_ERROR,
                    "info": {"message": e.message, "operation": e.operation},
                },
            }

        return {
            "item": {"status": ItemStatus.TRANSFERRED,
                     'transferred': datetime.now()},
            "job": {
                "error": JobError.NONE,
                "info": None,
            },
        }

    def refresh_credentials(self):
        self.reader.refresh_credentials()
        self.writer.refresh_credentials()
