import glob
from os import path
from typing import Union
import logging

from watchdog import events

logger = logging.getLogger("ImdbWatcher")


class ImdbEventHandler(events.FileSystemEventHandler):
    def __init__(self) -> None:
        super().__init__()


    def s3_upload(self, file_path: str):
        pass

    def on_any_event(self, event: events.FileSystemEvent):
        logger.debug(event)


    def on_created(self, event: Union[events.FileCreatedEvent, events.DirCreatedEvent]):
        if event.is_directory:
            for file in glob.glob(path.join(event.src_path, '**'), recursive=True):
                if path.isfile(file):
                    self.s3_upload(file)
        else:
            self.s3_upload(event.src_path)


    def on_modified(self, event: Union[events.FileModifiedEvent, events.DirModifiedEvent]):
        if not event.is_directory:
            self.s3_upload(event.src_path)