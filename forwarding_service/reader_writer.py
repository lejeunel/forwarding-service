#!/usr/bin/env python3
import hashlib
from base64 import b64encode
from multiprocessing import current_process

from .base import BaseReader, BaseWriter


class BaseReaderWriter:
    def __init__(self, *args, **kwargs):
        pass


class ReaderWriter(BaseReaderWriter):
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

    def send(self, in_uri: str, out_uri: str):
        print(f"[{current_process().pid}] {in_uri} -> {out_uri}")
        bytes_, type_ = self.reader(in_uri)

        checksum = None
        if self.do_checksum:
            checksum = self.compute_sha256_checksum(bytes_)

        self.writer(bytes_, out_uri, type_, checksum)

    def refresh_credentials(self):
        self.reader.refresh_credentials()
        self.writer.refresh_credentials()
