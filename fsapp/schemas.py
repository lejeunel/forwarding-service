#!/usr/bin/env python3
from . import ma, db
from .models import Job, File, UserBucket
from .enum_types import FileStatus
from marshmallow import post_dump
import enum

class BaseSchema(ma.SQLAlchemyAutoSchema):
    @post_dump
    def enum_to_str(self, in_data, **kwargs):
        for k, v in in_data.items():
            if isinstance(v, enum.Enum):
                in_data[k] = v.name
        return in_data

class UserSchema(BaseSchema):
    class Meta:
        model = UserBucket



class FileSchema(BaseSchema):
    class Meta:
        model = File
        include_fk = True


class JobSchema(BaseSchema):
    class Meta:
        model = Job
        include_fk = True


    @post_dump
    def add_file_info(self, in_data, **kwargs):
        """
        Add information regarding pending transfers
        """
        cur_job = db.session.get(Job, in_data["id"])
        files = db.session.query(File).filter(File.job_id == str(cur_job.id)).all()

        total = len(files)
        done = len([f for f in files if f.status == FileStatus.TRANSFERRED])
        if total != 0:
            perc = (done / total) * 100
        else:
            perc = 0
        in_data["total_files"] = total
        in_data["done_files"] = done
        in_data["progress"] = "{:.2f}%".format(perc)

        return in_data
