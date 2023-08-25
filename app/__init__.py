from pathlib import Path

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import BaseModel


def make_session(db_url: str = None):
    if db_url is None:
        db_url = Path(
            config("FORW_SERV_DB_PATH", "~/.cache/forwarding_service.db")
        ).expanduser()
        assert db_url.parent.exists(), f"{db_url.parent} not found."

    engine = create_engine(f"sqlite:///{db_url}")
    Session = sessionmaker(bind=engine)
    session = Session()
    BaseModel.metadata.create_all(session.bind.engine)

    return session
