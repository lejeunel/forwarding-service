from .utils import filter_table

def get_item_by_query(
    source=None,
    destination=None,
    status=None,
    job_id=None,
    limit=50,
    sort_on="upload_date",
):
    """Return Item representation, with filtering capabilities

    Args:
        filename (str, optional): filename filtering. Defaults to None.
        source_path (str, optional): source path filtering. Defaults to None.
        bucket (str, optional): bucket filtering. Defaults to None.
        prefix (str, optional): prefix filtering. Defaults to None.
        status (str, optional): status filtering. Defaults to None.
        user (str, optional): user filtering. Defaults to None.
        job_id (str, optional): job id filtering. Defaults to None.
        limit (int, optional): Limit number of file. Defaults to 50.
        sort_on (str, optional): Sort file by. Defaults to "upload_date".

    Returns:
        [ItemSchema]: return files representation in a list
    """
    from .models import Item
    from .schemas import ItemSchema

    query = filter_table(Item, **locals())
    if sort_on is not None:
        field = getattr(Item, sort_on)
        query = query.order_by(field)

    query = query.limit(limit)
    files = query.all()

    return ItemSchema(many=True).dump(files)


def get_job_by_query(id=None, status=None, limit=50, sort_on="created"):
    """Return Job representation, with filtering capabilities

    Args:
        id (str, optional): Job id filtering. Defaults to None.
        status (str, optional): status filtering. Defaults to None.
        limit (int, optional): limit number of Job. Defaults to 50.
        sort_on (str, optional): sort job by. Defaults to "created".

    Returns:
        [JobSchema]: return Job representation in a list
    """
    from .models import Job
    from .schemas import JobSchema

    query = filter_table(Job, **locals())

    if sort_on is not None:
        field = getattr(Job, sort_on)
        query = query.order_by(field.desc())

    query = query.limit(limit)

    jobs = query.all()[::-1]

    return JobSchema(many=True).dump(jobs)


