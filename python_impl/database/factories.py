import time
from collections import defaultdict
import factory
import random
from collections.abc import Iterable
from factory.alchemy import SQLAlchemyModelFactory
from faker import Faker
from faker.providers import BaseProvider
from database.models import db
from database.models import (Collections, Fields, FieldElements, FieldCommits, CollectionCommits,
        Segments, SegmentFiles, PartitionCommits, SegmentCommits, SchemaCommits, LogSequenceNumbers)
from database.utils import Commit
from utils import get_lsn


class FakerProvider(BaseProvider):
    def this_date(self):
        t = datetime.datetime.today()
        return (t.year - 1900) * 10000 + (t.month - 1) * 100 + t.day

factory.Faker.add_provider(FakerProvider)


class CollectionsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Collections
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    name = factory.Faker('word')


class FieldsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Fields
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    name = factory.Faker('word')
    ftype = factory.Faker('random_element', elements=(0,1,2,3,5))
    collection = factory.SubFactory(CollectionsFactory)


class FieldElementsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = FieldElements
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    name = factory.Faker('word')
    ftype = factory.Faker('random_element', elements=(0,1,2,3,5))
    field = factory.SubFactory(FieldsFactory)


class FieldCommitsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = FieldCommits
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    field = factory.SubFactory(FieldsFactory)


class SchemaCommitsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = SchemaCommits
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    collection = factory.SubFactory(CollectionsFactory)


class CollectionCommitsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = PartitionCommits
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    collection = factory.SubFactory(CollectionsFactory)


class SegmentsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Segments
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    collection = factory.SubFactory(CollectionsFactory)


class SegmentCommitsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = SegmentCommits
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    segment = factory.SubFactory(SegmentsFactory)


class SegmentFilesFactory(SQLAlchemyModelFactory):
    class Meta:
        model = SegmentFiles
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    segment = factory.SubFactory(SegmentsFactory)
    ftype = factory.Faker('random_element', elements=(0,1,2,3,5))


BINARY_FILE = 1
STRING_FILE = 2
IVFSQ8_FILE = 3
DEL_FILE = 4
FILE_TYPES = [BINARY_FILE, STRING_FILE, IVFSQ8_FILE, DEL_FILE]

def create_collection_commit(data_manager, collection, lsn, schema=None, new_files=None, segment=None):
    post_cb = []
    if not schema:
        schema = data_manager.get_schema(collection.id)
        post_cb.append(lambda : data_manager.release_schema(schema))
    prev = data_manager.get_snapshot(collection.id)
    post_cb.append(lambda : data_manager.release_snapshot(prev))
    partition_commit_ids = prev.mappings
    prev_pc = data_manager.partition_commits_mgr.get(collection.id, partition_commit_ids[-1])
    resources = []
    partition = prev_pc.partition
    segment = segment if segment else partition.create_segment()
    Commit(segment)

    field_commits = schema.field_commits.all()

    if not new_files:
        new_files = []
        for field_commit in field_commits:
            field_elements = field_commit.elements.all()
            for field_element in field_elements:
                seg_file = SegmentFiles(ftype=random.choice(FILE_TYPES), segment=segment, field_element=field_element,
                        partition=partition)
                new_files.append(seg_file)

    for new_f in new_files:
        resources.append(new_f)

    Commit(*resources)

    segment_commit = segment.commit_files(*resources, schema=schema)

    # import pdb;pdb.set_trace()
    Commit(segment_commit)
    partition_commit = segment_commit.commit_snapshot()

    partition_commit.append_mappings(*prev_pc.mappings)
    partition_commit.apply()
    Commit(partition_commit)

    lsn_entry = LogSequenceNumbers(lsn=lsn)
    c_c = CollectionCommits(collection=collection, mappings=[partition_commit.id])
    Commit(c_c, lsn_entry)
    data_manager.partition_commits_mgr.release(prev_pc)

    data_manager.collection_commit_mgr.append(c_c)
    for cb in post_cb:
        cb()
    Commit(partition_commit)

    return partition_commit
