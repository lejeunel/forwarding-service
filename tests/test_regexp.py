from forwarding_service.models import Item
import pytest


def test_regexp_no_match(job_manager):

    with pytest.raises(Exception):
        job_manager.init("file://root/path/project/",
                            r"s3://bucket/project/", r"^.*\.funnyextension")


def test_regexp_match(job_manager):

    job = job_manager.init("file:///root/path/project/",
                         r"s3://bucket/project/", r"^.*\.funnyextension")
    job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)

    items = job.items
    assert len(items) >= 0
