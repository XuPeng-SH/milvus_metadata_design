import logging
import datetime
from sqlalchemy import (SmallInteger, Integer, Boolean, Text,
                        String, BigInteger, and_, or_,
                        Column, JSON, DateTime)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.attributes import flag_modified

from database import db

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
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    name = Column(String(64))
    version = Column(JSON, default={})

    __tablename__ = 'Collections'

    def create_segment(self, version={}):
        s = Segments(collection=self, version=version)
        return s

    def create_snapshot(self):
        s = CollectionSnapshots(collection=self)
        return s

    def create_field(self, *args, **kwargs):
        s = CollectionFields(*args, collection=self, **kwargs)
        return s


class CollectionFields(db.Model, BaseMixin):
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
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

    def add_index(self, name, ftype, params={}):
        idx = FieldElements(field=self, name=name, ftype=ftype, params=params,
                collection=self.collection)
        return idx


class FieldElements(db.Model, BaseMixin):
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    name = Column(String(64))
    ftype = Column(Integer)
    field_id = Column(BigInteger)
    version = Column(JSON, default={})
    params = Column(JSON, default={})
    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(FieldElements.collection_id) == Collections.id)',
            backref=backref('field_elements', uselist=True, lazy='dynamic')
    )


    field = relationship(
            'CollectionFields',
            primaryjoin='and_(foreign(FieldElements.field_id) == CollectionFields.id)',
            backref=backref('elements', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'FieldElements'


class CollectionSnapshots(db.Model, BaseMixin):
    bound = set()
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    version = Column(JSON, default={})
    params = Column(JSON, default={})
    mappings = Column(JSON, default={})

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(CollectionSnapshots.collection_id) == Collections.id)',
            backref=backref('snapshots', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'CollectionSnapshots'

    @property
    def commits(self):
        return db.Session.query(SegmentCommits).filter(SegmentCommits.id.in_(self.mappings))

    def append_mappings(self, *targets):
        for target in targets:
            if isinstance(target, int):
                self.bound.add(target)
                continue
            assert isinstance(target, SegmentCommits)
            self.bound.add(target.id)

    def apply(self):
        if len(self.bound) == 0:
            return
        self.mappings = list(self.bound)
        self.bound.clear()


class Segments(db.Model, BaseMixin):
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
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

    def create_file(self, ftype=None, lsn=None, size=None, version=None, attributes=None):
        f = SegmentFiles(ftype=ftype, lsn=lsn, size=size, version=version, attributes=attributes,
                segment=self, collection_id=self.collection.id)
        return f

    def commit_files(self, *files, **kwargs):
        target = kwargs.pop('target', None)
        target = target if target else SegmentCommits(segment=self, collection_id=self.collection_id, **kwargs)
        target.append_mappings(*files)
        target.apply()
        return target


class SegmentCommits(db.Model, BaseMixin):
    bound = set()
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    version = Column(JSON, default={})

    collection_id = Column(BigInteger)
    segment_id = Column(BigInteger, nullable=False)
    mappings = Column(JSON, default={})

    segment = relationship(
            'Segments',
            primaryjoin='and_(foreign(SegmentCommits.segment_id) == Segments.id)',
            backref=backref('commits', uselist=True, lazy='dynamic')
    )

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(SegmentCommits.collection_id) == Collections.id)',
            backref=backref('commits', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'SegmentCommits'

    @property
    def files(self):
        return db.Session.query(SegmentFiles).filter(SegmentFiles.id.in_(self.mappings))

    def append_mappings(self, *targets):
        for target in targets:
            if isinstance(target, int):
                self.bound.add(target)
                continue
            self.bound.add(target.id)

    def apply(self):
        if len(self.bound) == 0:
            return
        self.mappings = list(self.bound)
        self.bound.clear()

    def commit_snapshot(self, target=None, **kwargs):
        target = kwargs.pop('target', None)
        apply = kwargs.pop('apply', False)
        target = target if target else self.segment.collection.create_snapshot()
        target.append_mappings(self)
        apply and target.apply()
        return target


class SegmentFiles(db.Model, BaseMixin):
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(SmallInteger, default=0)
    version = Column(JSON, default={})
    attributes = Column(JSON, default={})

    collection_id = Column(BigInteger, nullable=False)
    segment_id = Column(BigInteger, nullable=False)
    ftype = Column(Integer)
    entity_cnt = Column(BigInteger, default=0)
    lsn = Column(BigInteger, default=0)
    size = Column(BigInteger, default=0)

    segment = relationship(
            'Segments',
            primaryjoin='and_(foreign(SegmentFiles.segment_id) == Segments.id)',
            backref=backref('files', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'SegmentFiles'

    # snapshots = relationship(
    #     'CollectionSnapshots',
    #     secondary=SnapshotFileMapping.__table__,
    #     primaryjoin='and_(foreign(SnapshotFileMapping.file_id) == SegmentFiles.id)',
    #     secondaryjoin='and_(foreign(SnapshotFileMapping.snapshot_id) == CollectionSnapshots.id)',
    #     backref=backref('files', uselist=True)
    # )


# class Snapshots(db.Model, BaseMixin):
#     id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
#     created_on = Column(DateTime, default=datetime.datetime.utcnow)
#     status = Column(SmallInteger, default=0)
#     version = Column(JSON, default={})
#     params = Column(JSON, default={})

#     collection_id = Column(BigInteger)

#     collection = relationship(
#             'Collections',
#             primaryjoin='and_(foreign(Snapshots.collection_id) == Collections.id)',
#             backref=backref('snapshots', uselist=True, lazy='dynamic')
#     )

#     __tablename__ = 'Snapshots'


#     def get_resources(self):
#         files = self.files
#         return files

#     def submit(self, another):
#         resources = []
#         files = self.get_resources()
#         for f in files:
#             m = f.submit(another)
#             resources.append(f)
#             resources.append(m)
#         return resources


# class SnapshotFileMapping(db.Model, BaseMixin):
#     id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
#     created_on = Column(DateTime, default=datetime.datetime.utcnow)

#     file_id = Column(BigInteger)
#     snapshot_id = Column(BigInteger)

#     file = relationship(
#             'SegmentFiles',
#             primaryjoin='and_(foreign(SnapshotFileMapping.file_id) == SegmentFiles.id)',
#     )

#     snapshot = relationship(
#             'CollectionSnapshots',
#             primaryjoin='and_(foreign(SnapshotFileMapping.snapshot_id) == CollectionSnapshots.id)',
#     )

#     __tablename__ = 'SnapshotFileMapping'
