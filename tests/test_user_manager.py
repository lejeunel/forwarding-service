from moto.core import set_initial_no_auth_action_count
from fsapp.command import add_user, remove_user
from sqlalchemy.exc import IntegrityError
from fsapp.models import UserBucket

USERNAME = "UNITTEST"
BUCKETNAME = "BUCKET_UNIT_TEST"


@set_initial_no_auth_action_count(4)
def test_add_user(app, bucket, mock_file_tree, mock_refresh_credentials):

    add_user(USERNAME, BUCKETNAME, "/TEST/UNIT")

    user = UserBucket.query.filter(UserBucket.user == USERNAME)
    assert user.count() == 1


@set_initial_no_auth_action_count(4)
def test_delete_all_permissions(app, bucket, mock_file_tree, mock_refresh_credentials):

    add_user(USERNAME, BUCKETNAME, "/test/src")
    add_user(USERNAME, BUCKETNAME, "/src/test")

    remove_user(USERNAME, BUCKETNAME)

    users = UserBucket.query.filter(UserBucket.user == USERNAME).filter(
        UserBucket.bucket == BUCKETNAME
    )
    assert users.count() == 0


@set_initial_no_auth_action_count(4)
def test_delete_one_permission(app, bucket, mock_file_tree, mock_refresh_credentials):
    from fsapp.models import UserBucket
    from fsapp import db

    add_user(USERNAME, BUCKETNAME, "/test/src")
    add_user(USERNAME, BUCKETNAME, "/src/test")

    remove_user(USERNAME, BUCKETNAME, "/test/src")

    records = (
        UserBucket.query.filter(UserBucket.user == USERNAME)
        .filter(UserBucket.bucket == BUCKETNAME)
        .all()
    )

    assert len(records) == 1
