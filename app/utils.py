#!/usr/bin/env python3

from flask_sqlalchemy.model import DefaultMeta


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
