#!/usr/bin/env python3

from flask_sqlalchemy.model import DefaultMeta

from fsapp.fileshare import path_is_parent


class MD5CheckSumException(Exception):
    pass


class UserBucketException(Exception):
    pass


class BucketAccessException(Exception):
    pass


def user_bucket_allowed(user, bucket):
    """Checks that user is allowed to push to bucket

    Args:
        user (str): user name
        bucket (str): bucket name

    Returns:
        bool: _description_
    """
    from . import db
    from .models import UserBucket

    permissions = db.session.execute(
        db.select(UserBucket.id).filter_by(user=user, bucket=bucket)
    ).first()

    if permissions is None:
        return False
    return True


def user_bucket_dirs_allowed(user, bucket, dir):
    """Cheks if that user is allowed to push to bucket from given directory

    Args:
        user (str): user name
        bucket (str): bucket name
        dir (str): directory

    Returns:
        bool: _description_
    """
    from . import db
    from .models import UserBucket

    granted = False

    permissions = UserBucket.query.filter_by(user=user,
                                             bucket=bucket).all()
    for parent in permissions:
        if path_is_parent(parent.allowed_root_dirs, dir):
            granted = True

    return granted


def filter_table(model: DefaultMeta, **kwargs):
    """
    Applies filters defined in kwargs on sqlalchemy model.
    Non-matching fields are ignored.

    Returns a query
    """
    query = model.query

    # get field names
    fields = model.__table__.columns.keys()

    for k, v in kwargs.items():
        if (k in fields) and (v is not None):
            query = query.filter(getattr(model, k) == v)

    return query
