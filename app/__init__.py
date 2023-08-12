#!/usr/bin/env python3

from flask import Flask
from flask_marshmallow import Marshmallow
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy

from app.forwarder import ForwarderExtension, BaseReader, BaseWriter

db = SQLAlchemy()
fs = ForwarderExtension()
ma = Marshmallow()
migrate = Migrate()


def create_app(mode='notest'):
    """
    Application factory

    docker_mode variable sets Redis URL according to
    service name defined in docker-compose.yml
    """

    from .cli import (resume_job, show_item, show_job, upload)
    from .models import Job, Item

    app = Flask(__name__)
    if mode == "test":
        app.config.from_object("app.config.test")
    else:
        app.config.from_object("app.config.notest")
        fs.init_app(app)

    db.init_app(app)
    ma.init_app(app)
    migrate.init_app(app, db)

    app.cli.add_command(upload)
    app.cli.add_command(show_job)
    app.cli.add_command(resume_job)
    app.cli.add_command(show_item)

    with app.app_context():
        db.create_all()

    return app
