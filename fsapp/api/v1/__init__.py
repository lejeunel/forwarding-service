#!/usr/bin/env python3

from flask.views import MethodView
from flask_smorest import Blueprint

from ...schemas import UserSchema

bp = Blueprint(
    "Identity",
    "Identity",
    url_prefix="/api/v1",
    description="Obtain information on current user",
)


@bp.route("/whoami")
class UserAPI(MethodView):
    @bp.response(200, UserSchema)
    def get(self):
        """Get my identity"""

        return {"id": 1, "name": "Test User", "email": "test-user@mail.com"}
