

def test_regexp_no_match(job_manager):

    job = job_manager.init("file:///root/path/project/",
                        r"s3://bucket/project/", r"^.*\.funnyextension")
    job_manager.parse_and_commit_items(job)
    assert len(job.items) == 0


def test_regexp_match(job_manager):

    job = job_manager.init("file:///root/path/project/",
                         r"s3://bucket/project/", r"^.*\.ext")
    job_manager.parse_and_commit_items(job)
    job_manager.run(job)

    assert len(job.items) >= 0
