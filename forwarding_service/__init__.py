from pathlib import Path

from decouple import config
from sqlmodel import create_engine, Session, SQLModel


def make_session(db_url: str = None):
    if db_url is None:
        db_path = Path(
            config("FORW_SERV_DB_PATH", "~/.cache/forwarding_service.db")
        ).expanduser()
        db_url = f'sqlite:///{db_path}'

    engine = create_engine(f"{db_url}")
    SQLModel.metadata.create_all(engine)
    session = Session(engine)

    return session
