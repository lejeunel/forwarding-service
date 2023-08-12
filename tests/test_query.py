from moto.core import set_initial_no_auth_action_count
from . import upload
from fsapp.command import get_file_by_query


@set_initial_no_auth_action_count(4)
def test_query_files_of_one_job(app, bucket, mock_file_tree, mock_refresh_credentials):

    upload()
    src = "/fileshares/hcs-research11/otherproject/exp/plate"
    upload(src=src)
    res = get_file_by_query(source_path=src)
    assert all([r["source_path"] == src for r in res])

