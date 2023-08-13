from abc import ABC, abstractmethod
import mimetypes


class BaseReader(ABC):
    @abstractmethod
    def read(self, *args, **kwargs):
        pass

    @abstractmethod
    def exists(self, *args, **kwargs):
        pass

    @abstractmethod
    def list(self, *args, **kwargs):
        pass

    @staticmethod
    def guess_mime_type(uri):
        return mimetypes.guess_type(uri)[0]

    def __call__(self, uri):
        bytes_ = self.read(uri)
        type_ = self.guess_mime_type(uri)

        return bytes_, type_

    def refresh_credentials(self):
        pass


class BaseWriter(ABC):
    @abstractmethod
    def __call__(self, *args, **kwargs):
        pass

    def refresh_credentials(self):
        pass
