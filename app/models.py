import uuid

import sqlalchemy as sa
from sqlalchemy import JSON, DateTime, Enum, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy_mixins.repr import ReprMixin
from sqlalchemy_mixins.serialize import SerializeMixin
from sqlalchemy_utils.types.uuid import UUIDType

from app.enum_types import ItemStatus, JobError, JobStatus


class Base(DeclarativeBase, SerializeMixin, ReprMixin):
    pass

class Item(Base):
    """
    Each record is an item to transfer
    """
    __tablename__ = "item"

    id = mapped_column(UUIDType, primary_key=True, default=uuid.uuid4)
    in_uri = mapped_column(String)
    out_uri = mapped_column(String)
    status = mapped_column(Enum(ItemStatus))
    job_id = mapped_column(UUIDType, sa.ForeignKey("job.id"))
    created = mapped_column(DateTime(timezone=True), server_default=func.now())
    transferred = mapped_column(DateTime(timezone=True), nullable=True)
    job: Mapped["Job"] = relationship(back_populates="items")



class Job(Base):
    """
    Each record is a set of items with flags that are
    helpful to monitor status
    """
    __tablename__ = "job"

    id = mapped_column(UUIDType, primary_key=True, default=uuid.uuid4)
    last_state = mapped_column(Enum(JobStatus), default=JobStatus.INITIATED)
    error = mapped_column(Enum(JobError), default=JobError.NONE)
    info = mapped_column(JSON)
    source = mapped_column(String)
    destination = mapped_column(String)
    regexp = mapped_column(String)
    created = mapped_column(DateTime(timezone=True), server_default=func.now())

    items: Mapped[list["Item"]] = relationship(back_populates="job")

    def to_detailed_dict(self, *args, **kwargs):
        ret = super().to_dict()
        ret['n_items'] = len(self.items)
        ret['done_item'] = len([i for i in self.items if i.status == ItemStatus.TRANSFERRED])
        if ret['n_items'] > 0:
            ret['done_perc'] = '{:.2f}%'.format(100 * ret['done_item'] / ret['n_items'])
        else:
            ret['done_perc'] = 'none'

        return ret
