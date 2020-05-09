import logging
import datetime
import enum
from sqlalchemy import (SmallInteger, Integer, Boolean, Text,
                        String, BigInteger, and_, or_,
                        Column, JSON, DateTime, UniqueConstraint, Enum)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.attributes import flag_modified

from database import db
from utils import get_lsn

logger = logging.getLogger(__name__)


class ModeEnum(enum.Enum):
    @classmethod
    def members(cls):
        return cls._member_names_


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


class StatusType(ModeEnum):
    PENDING = 0
    ACTIVE = 1
    DEACTIVE = 2


class StatusMixin:
    def activate(self, commit=False):
        if self.status == StatusType.DEACTIVE:
            raise RuntimeError(f'Could not activate deactive {self.id}')

        if self.status == StatusType.ACTIVE:
            return self

        self.status = StatusType.ACTIVE

        if commit == True:
            session = db.Session
            session.add(self)
            session.commit()
        return self

    def deactivate(self, commit=False):
        if self.status == StatusType.DEACTIVE:
            return

        self.status = StatusType.DEACTIVE

        if commit == True:
            session = db.Session
            session.add(self)
            session.commit()
        return self


class BaseModel(db.Model, BaseMixin, StatusMixin):
    __abstract__ = True

    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(Enum(StatusType), default=StatusType.ACTIVE)


class PendingBaseModel(db.Model, BaseMixin, StatusMixin):
    __abstract__ = True

    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(Enum(StatusType), default=StatusType.PENDING)


class MappingModel(BaseModel):
    __abstract__ = True

    bound = set()
    mappings = Column(JSON, default={})

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

    @property
    def subnodes(self):
        return db.Session.query(self.subnode_model).filter(
                self.subnode_model.id.in_(self.mappings))


class LogSequenceNumbers(db.Model, BaseMixin):
    id = Column(BigInteger().with_variant(Integer, 'sqlite'), primary_key=True, autoincrement=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    lsn = Column(BigInteger, default=0, unique=True)

    __tablename__ = 'LogSequenceNumbers'


class SegmentFiles(BaseModel):
    version = Column(JSON, default={})
    attributes = Column(JSON, default={})

    field_element_id = Column(BigInteger, default=None)

    partition_id = Column(BigInteger)
    segment_id = Column(BigInteger)
    ftype = Column(Integer)
    entity_cnt = Column(BigInteger, default=0)
    size = Column(BigInteger, default=0)

    partition = relationship(
            'Partitions',
            primaryjoin='and_(foreign(SegmentFiles.partition_id) == Partitions.id)',
            backref=backref('files', uselist=True, lazy='dynamic')
    )

    segment = relationship(
            'Segments',
            primaryjoin='and_(foreign(SegmentFiles.segment_id) == Segments.id)',
            backref=backref('files', uselist=True, lazy='dynamic')
    )

    field_element = relationship(
            'FieldElements',
            primaryjoin='and_(foreign(SegmentFiles.field_element_id) == FieldElements.id)',
            backref=backref('files', uselist=True, lazy='dynamic')
    )

    @property
    def collection_id(self):
        return self.partition.collection.id

    __tablename__ = 'SegmentFiles'


class SegmentCommits(MappingModel):
    subnode_model = SegmentFiles

    version = Column(JSON, default={})

    schema_id = Column(BigInteger)

    partition_id = Column(BigInteger)
    segment_id = Column(BigInteger)

    segment = relationship(
            'Segments',
            primaryjoin='and_(foreign(SegmentCommits.segment_id) == Segments.id)',
            backref=backref('commits', uselist=True, lazy='dynamic')
    )

    schema = relationship(
            'SchemaCommits',
            primaryjoin='and_(foreign(SegmentCommits.schema_id) == SchemaCommits.id)',
            backref=backref('segment_commits', uselist=True, lazy='dynamic')
    )

    partition = relationship(
            'Partitions',
            primaryjoin='and_(foreign(SegmentCommits.partition_id) == Partitions.id)',
            backref=backref('commits', uselist=True, lazy='dynamic')
    )

    @property
    def collection_id(self):
        return self.partition.collection.id

    __tablename__ = 'SegmentCommits'

    @property
    def files(self):
        return self.subnodes

    def commit_snapshot(self, target=None, **kwargs):
        target = kwargs.pop('target', None)
        apply = kwargs.pop('apply', False)
        target = target if target else self.segment.partition.create_partition_commit()
        target.append_mappings(self)
        apply and target.apply()
        return target


class Collections(PendingBaseModel):
    name = Column(String(64), nullable=False)
    version = Column(JSON, default={})

    __tablename__ = 'Collections'

    __table_args__ = (
        UniqueConstraint('name', 'status', name='_name_status_uc'),
    )


class Partitions(PendingBaseModel):
    name = Column(String(64), nullable=False)
    version = Column(JSON, default={})

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(Partitions.collection_id) == Collections.id)',
            backref=backref('partitions', uselist=True, lazy='dynamic')
    )

    def create_segment(self, version={}):
        s = Segments(partition=self, version=version)
        return s

    def create_partition_commit(self):
        s = PartitionCommits(partition=self, collection=self.collection)
        return s

    __tablename__ = 'Partitions'


class PartitionCommits(MappingModel):
    subnode_model = SegmentCommits

    version = Column(JSON, default={})
    params = Column(JSON, default={})

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(PartitionCommits.collection_id) == Collections.id)',
            backref=backref('partition_commits', uselist=True, lazy='dynamic')
    )

    partition_id = Column(BigInteger)

    partition = relationship(
            'Partitions',
            primaryjoin='and_(foreign(PartitionCommits.partition_id) == Partitions.id)',
            backref=backref('partition_commits', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'PartitionCommits'

    @property
    def segment_commits(self):
        return self.subnodes


class CollectionCommits(MappingModel):
    subnode_model = PartitionCommits

    version = Column(JSON, default={})

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(CollectionCommits.collection_id) == Collections.id)',
            backref=backref('snapshots', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'CollectionCommits'


class Fields(BaseModel):
    name = Column(String(64), nullable=False)
    num = Column(SmallInteger, default=0)
    ftype = Column(Integer)
    params = Column(JSON, default={})

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(Fields.collection_id) == Collections.id)',
            backref=backref('fields', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'Fields'

    __table_args__ = (
        UniqueConstraint('collection_id', 'name', name='_collection_name_uc'),
        UniqueConstraint('collection_id', 'num', name='_collection_num_uc'),
    )

    def add_element(self, **kwargs):
        idx = FieldElements(field=self, collection=self.collection, **kwargs)
        return idx


class FieldElements(BaseModel):
    name = Column(String(64), nullable=False)
    ftype = Column(Integer)
    field_id = Column(BigInteger)
    version = Column(JSON, default={})
    params = Column(JSON, default={})

    field = relationship(
            'Fields',
            primaryjoin='and_(foreign(FieldElements.field_id) == Fields.id)',
            backref=backref('elements', uselist=True, lazy='dynamic')
    )

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(FieldElements.collection_id) == Collections.id)',
            backref=backref('field_elements', uselist=True, lazy='dynamic')
            )

    __tablename__ = 'FieldElements'

    __table_args__ = (
        UniqueConstraint('field_id', 'name', name='_field_name_uc'),
    )


class FieldCommits(MappingModel):
    subnode_model = FieldElements

    version = Column(JSON, default={})

    field_id = Column(BigInteger)

    field = relationship(
            'Fields',
            primaryjoin='and_(foreign(FieldCommits.field_id) == Fields.id)',
            backref=backref('commits', uselist=True, lazy='dynamic')
    )

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(FieldCommits.collection_id) == Collections.id)',
            backref=backref('field_commits', uselist=True, lazy='dynamic')
    )


    __tablename__ = 'FieldCommits'

    @property
    def elements(self):
        return self.subnodes


class SchemaCommits(MappingModel):
    subnode_model = FieldCommits

    __tablename__ = 'SchemaCommits'

    collection_id = Column(BigInteger)

    collection = relationship(
            'Collections',
            primaryjoin='and_(foreign(SchemaCommits.collection_id) == Collections.id)',
            backref=backref('schema_commits', uselist=True, lazy='dynamic')
    )

    @property
    def field_commits(self):
        return self.subnodes


class Segments(BaseModel):
    version = Column(JSON, default={})

    partition_id = Column(BigInteger)

    partition = relationship(
            'Partitions',
            primaryjoin='and_(foreign(Segments.partition_id) == Partitions.id)',
            backref=backref('segments', uselist=True, lazy='dynamic')
    )

    __tablename__ = 'Segments'

    def create_file(self, ftype=None, lsn=None, size=None, version=None, attributes=None):
        f = SegmentFiles(ftype=ftype, lsn=lsn, size=size, version=version, attributes=attributes,
                segment=self, partition_id=self.partition.id)
        return f

    def commit_files(self, *files, **kwargs):
        target = kwargs.pop('target', None)
        target = target if target else SegmentCommits(segment=self, partition_id=self.partition_id, **kwargs)
        target.append_mappings(*files)
        target.apply()
        return target
