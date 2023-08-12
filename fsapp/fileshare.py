import fnmatch, re
import uuid
import os
from functools import partial


def path_is_parent(parent_path, child_path):
    """Check if child_path is contained in parent_path

    Args:
        parent_path (str): parent path 
        child_path (str): child path

    Returns:
        boot: True or False
    """    
    # Smooth out relative path names, note: if you are concerned about symbolic links, you should use os.path.realpath too
    parent_path = os.path.abspath(parent_path)
    child_path = os.path.abspath(child_path)

    # Compare the common path of the parent and child path with the common path of just the parent path. Using the commonpath method on just the parent path will regularise the path name in the same way as the comparison that deals with both paths, removing any trailing path separator
    return os.path.commonpath([parent_path]) == os.path.commonpath(
        [parent_path, child_path]
    )


def _match_file_extension(filename: str, pattern: str, is_regex=False):
    """
    Function that return boolean, tell if the filename match the the given pattern

    filename -- (str) Name of file
    pattern -- (str) Pattern to filter file, can be '*.txt' if not regex, else '.*\txt'
    is_regex -- Bool If pattern is a regex or not
    """
    if not is_regex:
        pattern = fnmatch.translate(pattern)

    reobj = re.compile(pattern)
    match = reobj.match(filename)

    if match is None:
        return False
    return True


class FileShare:
    """
    This Flask extension wraps a couple of
    os module functions

    Allows to set "allowed" directories for restricting access
    """

    def __init__(self, app=None):

        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        self.allowed_root_dirs = app.config["FS_ALLOWED_ROOT_DIRS"]

    def path_exists(self, path):
        return os.path.exists(path)

    def is_allowed(self, path):
        for root_path in self.allowed_root_dirs:
            if path_is_parent(root_path, path):
                return True
        return False

    def listdir(self, path, files_only=False, pattern_filter="*.*", is_regex=False):
        """
        Return list of files at path
        """
        if os.path.isfile(path):
            return [path]
        
        # TODO: scandir is not overloaded by mock-file-tree...
        # list_ = os.scandir(path)
        list_ = os.listdir(path)
        if files_only:
            list_ = [
                l
                for l in list_
                if os.path.isfile(os.path.join(path, l))
                and _match_file_extension(l, pattern_filter, is_regex)
            ]
        return list_
