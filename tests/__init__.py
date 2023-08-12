from fsapp.worker import _upload
import uuid
from mock_file_tree import FileTree, MockFileTree
import os

user = "test-user"
bucket = "mybucket"


# dummy file tree for unit test
# this overrides function of the os module
TEST_DIRS = [
    "/fileshares/hcs-research10/project/experiment/plate",
    "/fileshares/hcs-research11/otherproject/exp/plate",
]
TEST_FILES = [
    s + "/" + f
    for f in ["file_{}.ext".format(uuid.uuid4()) for _ in range(10)]
    for s in TEST_DIRS
]

TEST_TREE = FileTree.from_paths(*TEST_FILES)
mock_tree = MockFileTree(os, TEST_TREE)


def upload(
    bucket=bucket,
    prefix="prefix",
    regexp=".*",
    src="/fileshares/hcs-research10/project/experiment/plate",
    user=user,
):
    _upload("", bucket, prefix, testing=True,
            regexp=regexp, src=src, user=user)
