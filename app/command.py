import fnmatch

from sqlalchemy.exc import IntegrityError

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


def get_user_permission(user=None, bucket=None, limit=50):
    """_summary_

    Args:
        user (str, optional): user filtering. Defaults to None.
        bucket (str, optional): bucket filtering. Defaults to None.
        limit (int, optional): limint number of User. Defaults to 50.

    Returns:
        [UserSchema]: return User representation in a list
    """
    from .models import UserBucket
    from .schemas import UserSchema

    query = filter_table(UserBucket, **locals())
    users = query.all()

    return UserSchema(many=True).dump(users)


def _match_file_extension(filename: str, extension: str):
    if fnmatch.fnmatch(filename, extension):
        return True
    else:
        return False


def add_user(user, bucket, allowed_root_dirs):
    """Add user with permission on bucket and allowed_root_dirs
    If user with bucket/allowed_root_dir already exist, do nothing

    Args:
        user (str): User name to add
        bucket (str): bucket name
        allowed_root_dirs (str): directory path
    """

    from . import db
    from .models import UserBucket

    # Search first if user & bucket exist, if true, make an update, else insert
    is_user = UserBucket.query.filter_by(
        user=user, bucket=bucket, allowed_root_dirs=allowed_root_dirs
    ).first()

    # Update part
    if is_user is not None:
        return
    else:
        # Create user instead of update
        user = UserBucket(user=user, bucket=bucket, allowed_root_dirs=allowed_root_dirs)
        db.session.add(user)

        # Save change
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()


def remove_user(user, bucket, allowed_root_dirs=None):
    """Delete user according user and bucket

    Args:
        user (_type_): user name
        bucket (_type_): bucket name
        allowed_root_dirs (_type_, optional): directory path. Defaults to None.
    """
    from sqlalchemy import delete

    from . import db
    from .models import UserBucket

    stmt = delete(UserBucket).where(
        UserBucket.user == user, UserBucket.bucket == bucket
    )

    if allowed_root_dirs is not None:
        stmt = stmt.where(UserBucket.allowed_root_dirs == allowed_root_dirs)

    try:
        db.session.execute(stmt)
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
