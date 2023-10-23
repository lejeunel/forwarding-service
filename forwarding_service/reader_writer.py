import hashlib
from base64 import b64encode

from .base import BaseReader, BaseWriter


class ReaderWriter:
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
    def compute_sha256_checksum(bytes_) -> str:
        checksum = hashlib.sha256(bytes_.getbuffer())
        checksum = b64encode(checksum.digest()).decode()
        return checksum

    def send(self, in_uri: str, out_uri: str) -> None:
        print(f"{in_uri} -> {out_uri}")
        bytes_, type_ = self.reader(in_uri)

        checksum = None
        if self.do_checksum:
            checksum = self.compute_sha256_checksum(bytes_)

        self.writer(bytes_, out_uri, type_, checksum)

    def refresh_credentials(self) -> None:
        self.reader.refresh_credentials()
        self.writer.refresh_credentials()
