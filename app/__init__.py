from decouple import config
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from sqlalchemy import create_engine

from app.auth import S3StaticCredentials
from app.file import FileSystemReader
from app.models import Base
from app.s3 import S3Writer
from app.transfer_agent import TransferAgent


def setup_db():
    db_path = Path(
        config("FORW_SERV_DB_PATH", "~/.cache/forwarding_service.db")
    ).expanduser()
    assert db_path.parent.exists(), f"{db_path.parent} not found."

    engine = create_engine(f"sqlite:///{db_path}")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    return session


def make_agent():
    session = setup_db()

    auth = S3StaticCredentials(
        aws_access_key_id=config("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key=config("AWS_SECRET_ACCESS_KEY", None),
    )
    writer = S3Writer(auth)
    agent = TransferAgent(session, reader=FileSystemReader(), writer=writer)

    return agent, session
