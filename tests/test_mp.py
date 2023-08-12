from app.worker import _init_and_upload
from app.enum_types import JobError, JobStatus


def test_mp_job(app, mock_file_tree):
    from app import fs, db
    from app.models import Item
    from app.enum_types import ItemStatus

    with app.app_context():
        fs.n_procs = 4
        job = _init_and_upload("file:///root/path/project/",
                               "s3://bucket/project/")
        items = db.session.query(Item).where(Item.job_id == job.id)
        assert job.last_state == JobStatus.DONE
        assert job.error == JobError.NONE
        assert all(item.status == ItemStatus.TRANSFERRED for item in items)
