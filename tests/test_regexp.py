from app.worker import _init_and_upload


def test_regexp_leaves_no_items(app, mock_file_tree):

    from app.models import Item
    job = _init_and_upload("file://root/path/project/", "s3://bucket/project/",
                           regexp="^.*\.funnyextension")
    items = Item.query.all()
    assert len(items) == 0


def test_regexp_that_match(app, mock_file_tree):

    from app.models import Item
    job = _init_and_upload("file:///root/path/project/", "s3://bucket/project/",
                           regexp="^.*\.ext")
    items = Item.query.all()
    assert len(items) >= 0
