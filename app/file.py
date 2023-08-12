#!/usr/bin/env python3
from urllib.parse import urlparse
from .base import BaseReader
import os
import io


class FileSystemReader(BaseReader):
    def read(self, uri: str):
        uri = urlparse(uri)
        with open(uri.path, "rb") as f:
            fileobj = io.BytesIO(f.read())

        return fileobj

    def exists(self, uri):
        return os.path.exists(urlparse(uri).path)

    def list(self, uri, files_only=True):
        """
        Return URIs of all items present at path
        """
        path = urlparse(uri).path
        if os.path.isfile(path):
            return [path]

        list_ = [path + f for f in os.listdir(path)]
        list_ = ['file://' +
                 f for f in list_ if(os.path.isfile(f) or not files_only)]

        return list_
