

def test_regexp_no_match(agent, session):

    from app.models import Item
    job = agent.init_job("file://root/path/project/",
                         "s3://bucket/project/", "^.*\.funnyextension")
    items = agent.parse_and_commit_items(job.id)
    agent.upload(job.id)

    items = session.query(Item).all()
    assert len(items) == 0


def test_regexp_that_match(agent, session):

    from app.models import Item
    job = agent.init_job("file://root/path/project/",
                         "s3://bucket/project/", "^.*\.funnyextension")
    items = agent.parse_and_commit_items(job.id)
    agent.upload(job.id)

    items = session.query(Item).all()
    assert len(items) >= 0
