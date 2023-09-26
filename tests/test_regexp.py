from src.models import Item


def test_regexp_no_match(job_manager):

    job = job_manager.init("file://root/path/project/",
                         r"s3://bucket/project/", r"^.*\.funnyextension")
    items = job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)

    items = job_manager.session.query(Item).all()
    assert len(items) == 0


def test_regexp_that_match(job_manager):

    job = job_manager.init("file://root/path/project/",
                         r"s3://bucket/project/", r"^.*\.funnyextension")
    items = job_manager.parse_and_commit_items(job.id)
    job_manager.run(job.id)

    items = job_manager.session.query(Item).all()
    assert len(items) >= 0
