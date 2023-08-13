#!/usr/bin/env python3

from flask import Flask
from flask_marshmallow import Marshmallow
from flask_sqlalchemy import SQLAlchemy

from app.uploader import UploaderExtension

db = SQLAlchemy()
fs = UploaderExtension()
ma = Marshmallow()


def create_app(mode="notest"):
    """
    Application factory

    docker_mode variable sets Redis URL according to
    service name defined in docker-compose.yml
    """

    from .cli import resume, list_item, list_job, upload
    from .models import Job, Item

    app = Flask(__name__)
    if mode == "test":
        app.config.from_object("app.config.test")
    else:
        app.config.from_object("app.config.notest")

    fs.init_app(app)

    db.init_app(app)
    ma.init_app(app)

    app.cli.add_command(upload)
    app.cli.add_command(list_job)
    app.cli.add_command(resume)
    app.cli.add_command(list_item)

    with app.app_context():
        db.create_all()

    return app
