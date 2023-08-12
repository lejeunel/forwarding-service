from app.worker import _upload
import uuid
from mock_file_tree import FileTree, MockFileTree
import os

user = "test-user"
bucket = "mybucket"


# dummy file tree for unit test
# this overrides function of the os module
TEST_DIRS = [
    "/root/path/project/",
    "/root/path/otherproject/",
]
TEST_FILES = [
    s + "/" + f
    for f in ["file_{}.ext".format(uuid.uuid4()) for _ in range(10)]
    for s in TEST_DIRS
]

TEST_TREE = FileTree.from_paths(*TEST_FILES)
mock_tree = MockFileTree(os, TEST_TREE)
