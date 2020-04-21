import logging
import datetime
from sqlalchemy import (SmallInteger, Integer, Boolean, Text,
                        String, BigInteger, and_, or_,
                        Column, JSON, DateTime)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.attributes import flag_modified

from init_db import db

logger = logging.getLogger(__name__)

class BaseMixin:
    def save(self, field=''):
        session = db.Session
        if self.id == None:
            session.add(self)
        elif field:
            flag_modified(self, field)
        return self.commit(session)

    def commit(self, session):
        session.commit()
        return self


class Collections(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    name = Column(String(64))
    version = Column(JSON, default={})

    __tablename__ = 'Collections'


class CollectionFields(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    name = Column(String(64))
    num = Column(SmallInteger)
    ftype = Column(Integer)
    collection_id = Column(BigInteger)
    params = Column(JSON, default={})

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(CollectionFields.collection_id) == Collections.id)',
            backref=backref('fields', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'CollectionFields'


class CollectionFieldIndice(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    name = Column(String(64))
    ftype = Column(Integer)
    field_id = Column(BigInteger)
    version = Column(JSON, default={})
    params = Column(JSON, default={})

    field = relationship(
            'CollectionFields',
            primaryjoin='and_(foreign(CollectionFieldIndice.field_id) == CollectionFields.id)',
            backref=backref('indice', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'CollectionFieldIndice'


class Snapshots(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    version = Column(JSON, default={})
    params = Column(JSON, default={})

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(Snapshots.collection_id) == Collections.id)',
            backref=backref('snapshots', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'Snapshots'


class Segments(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    version = Column(JSON, default={})

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(Segments.collection_id) == Collections.id)',
            backref=backref('segments', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'Segments'


class CommitFileMapping(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)

    file_id = Column(BigInteger)
    commit_id = Column(BigInteger)

    file = relationship(
            'SegmentFiles',
            primaryjoin='and_(foreign(CommitFileMapping.file_id) == SegmentFiles.id)',
    )

    commit = relationship(
            'SegmentCommits',
            primaryjoin='and_(foreign(CommitFileMapping.commit_id) == SegmentCommits.id)',
    )

    __tablename__ = 'CommitFileMapping'


class SegmentFiles(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    version = Column(JSON, default={})
    attributes = Column(JSON, default={})

    segment_id = Column(BigInteger)
    ftype = Column(Integer)
    entity_cnt = Column(BigInteger, default=0)
    lsn = Column(BigInteger, default=0)
    size = Column(BigInteger, default=0)

    segment = relationship(
            'Segments',
            primaryjoin='and_(foreign(SegmentFiles.segment_id) == Segments.id)',
            backref=backref('files', uselist=True, lazy='dynamic')
    )

    commit = relationship(
        'SegmentCommits',
        secondary=CommitFileMapping.__table__,
        # secondary='CommitFileMapping',
        primaryjoin='and_(foreign(CommitFileMapping.file_id) == SegmentFiles.id)',
        secondaryjoin='and_(foreign(CommitFileMapping.commit_id) == SegmentCommits.id)',
        backref=backref('files', uselist=True)
    )

    __tablename__ = 'SegmentFiles'


class SegmentCommits(db.Model, BaseMixin):
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    version = Column(JSON, default={})
    attributes = Column(JSON, default={})

    segment_id = Column(BigInteger)
    snapshot_id = Column(BigInteger)

    segment = relationship(
            'Segments',
            primaryjoin='and_(foreign(SegmentCommits.segment_id) == Segments.id)',
            backref=backref('commits', uselist=True, lazy='dynamic')
    )

    snapshot = relationship(
            'Snapshots',
            primaryjoin='and_(foreign(SegmentCommits.snapshot_id) == Snapshots.id)',
            backref=backref('commits', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'SegmentCommits'


class TableFiles(db.Model):
    FILE_TYPE_NEW = 0
    FILE_TYPE_RAW = 1
    FILE_TYPE_TO_INDEX = 2
    FILE_TYPE_INDEX = 3
    FILE_TYPE_TO_DELETE = 4
    FILE_TYPE_NEW_MERGE = 5
    FILE_TYPE_NEW_INDEX = 6
    FILE_TYPE_BACKUP = 7

    __tablename__ = 'TableFiles'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_id = Column(String(50))
    segment_id = Column(String(50))
    engine_type = Column(Integer)
    file_id = Column(String(50))
    file_type = Column(Integer)
    file_size = Column(Integer, default=0)
    row_count = Column(Integer, default=0)
    updated_time = Column(BigInteger)
    created_on = Column(BigInteger)
    date = Column(Integer)
    flush_lsn = Column(Integer)

    table = relationship(
        'Tables',
        primaryjoin='and_(foreign(TableFiles.table_id) == Tables.table_id)',
        backref=backref('files', uselist=True, lazy='dynamic')
    )


class Tables(db.Model):
    TO_DELETE = 1
    NORMAL = 0

    __tablename__ = 'Tables'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    table_id = Column(String(50), unique=True)
    owner_table = Column(String(50))
    partition_tag = Column(String(50))
    version = Column(String(50))
    state = Column(Integer)
    dimension = Column(Integer)
    created_on = Column(Integer)
    flag = Column(Integer, default=0)
    index_file_size = Column(Integer)
    index_params = Column(String(50))
    engine_type = Column(Integer)
    metric_type = Column(Integer)
    flush_lsn = Column(Integer)

    def files_to_search(self, date_range=None):
        cond = or_(
            TableFiles.file_type == TableFiles.FILE_TYPE_RAW,
            TableFiles.file_type == TableFiles.FILE_TYPE_TO_INDEX,
            TableFiles.file_type == TableFiles.FILE_TYPE_INDEX,
        )
        if date_range:
            cond = and_(
                cond,
                or_(
                    and_(TableFiles.date >= d[0], TableFiles.date < d[1]) for d in date_range
                )
            )

        files = self.files.filter(cond)

        return files
