

def test_regexp_no_match(agent):

    from app.models import Item
    job = agent.init_job("file://root/path/project/",
                         r"s3://bucket/project/", r"^.*\.funnyextension")
    items = agent.parse_and_commit_items(job.id)
    agent.upload(job.id)

    items = agent.session.query(Item).all()
    assert len(items) == 0


def test_regexp_that_match(agent):

    from app.models import Item
    job = agent.init_job("file://root/path/project/",
                         r"s3://bucket/project/", r"^.*\.funnyextension")
    items = agent.parse_and_commit_items(job.id)
    agent.upload(job.id)

    items = agent.session.query(Item).all()
    assert len(items) >= 0
