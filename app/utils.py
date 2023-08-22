import fnmatch
import re

from flask_sqlalchemy.model import DefaultMeta


def filter_table(session, model: DefaultMeta, **kwargs):
    """
    Applies filters defined in kwargs on sqlalchemy model.
    Non-matching fields are ignored.

    Returns a query
    """

    # get field names
    fields = model.__table__.columns.keys()
    query = session.query(model)

    for k, v in kwargs.items():
        if (k in fields) and (v is not None):
            query = query.filter(getattr(model, k) == v)

    return query


def chunks(l, n):
    """Yield n number of striped chunks from l."""
    for i in range(0, n):
        yield l[i::n]


def _match_file_extension(filename: str, pattern: str, is_regex=False):
    """
    Function that return boolean, tell if the filename match the the given pattern

    filename -- (str) Name of file
    pattern -- (str) Pattern to filter file, can be '*.txt' if not regex, else '.*\txt'
    is_regex -- Bool If pattern is a regex or not
    """
    if not is_regex:
        pattern = fnmatch.translate(pattern)

    reobj = re.compile(pattern)
    match = reobj.match(filename)

    if match is None:
        return False
    return True


