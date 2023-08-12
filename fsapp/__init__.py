#!/usr/bin/env python3

from flask import Blueprint, Flask
from flask_marshmallow import Marshmallow
from flask_rq2 import RQ
from flask_smorest import Api
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

from fsapp.sender import S3SenderExtension

from .fileshare import FileShare

bp = Blueprint("index", __name__)
db = SQLAlchemy()
restapi = Api()
rq = RQ()
fs = FileShare()
s3 = S3SenderExtension()
ma = Marshmallow()
migrate = Migrate()


def create_app(mode="dev", docker_mode=False):
    """
    Application factory

    docker_mode variable sets Redis URL according to
    service name defined in docker-compose.yml
    """

    from .api.v1 import bp as api_blp
    from .cli import (
        show_file,
        show_job,
        show_user_bucket,
        upload,
        resume_job,
        manage_user,
    )

    app = Flask(__name__)

    assert mode in ["dev", "test", "prod"]
    if mode == "dev":
        app.config.from_object("fsapp.config.dev")
    if mode == "test":
        app.config.from_object("fsapp.config.test")
    if mode == "prod":
        app.config.from_object("fsapp.config.prod")
    if mode in ["dev", "prod"]:
        rq.init_app(app)

    if docker_mode:
        print("docker mode")
        app.config["RQ_REDIS_URL"] = "redis://redis:6379/0"

    # register blueprints
    app.register_blueprint(bp, url_prefix="/")
    restapi.init_app(app)
    restapi.register_blueprint(api_blp)

    db.init_app(app)
    fs.init_app(app)
    s3.init_app(app)
    ma.init_app(app)
    migrate.init_app(app, db)

    app.cli.add_command(upload)
    app.cli.add_command(show_job)
    app.cli.add_command(resume_job)
    app.cli.add_command(show_user_bucket)
    app.cli.add_command(show_file)
    app.cli.add_command(manage_user)

    with app.app_context():
        db.create_all()

    return app
