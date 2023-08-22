from decouple import config
from flask_sqlalchemy.model import DefaultMeta
from sqlalchemy.orm import sessionmaker

from app.auth import S3StaticCredentials
from app.file import FileSystemReader
from app.models import Base
from app.s3 import S3Writer
from app.transfer_agent import TransferAgent

def setup_db():
    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///uploads.db")
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    return session

def make_agent():

    session = setup_db()

    auth = S3StaticCredentials(aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
                               aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'))
    writer = S3Writer(auth)
    agent = TransferAgent(session, reader=FileSystemReader(), writer=writer)

    return agent, session
