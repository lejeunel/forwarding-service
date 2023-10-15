from forwarding_service.enum_types import JobStatus
from forwarding_service.models import Job
from forwarding_service.query import JobQueryArgs, Query


def test_get_items(session, completed_job):

    query = Query(session, Job)

    assert len(query.get(JobQueryArgs(id=completed_job.id))) > 0


def test_get_jobs(session, completed_job):
    query = Query(session, Job)
    result = query.get(JobQueryArgs(status=JobStatus.DONE))
    assert result[0].id == completed_job.id
