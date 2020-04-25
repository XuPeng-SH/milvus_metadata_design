import time
from collections import defaultdict
import factory
import random
from factory.alchemy import SQLAlchemyModelFactory
from faker import Faker
from faker.providers import BaseProvider
from database.models import db
from database.models import (Collections, Fields, FieldElements,
        Segments, SegmentFiles, CollectionSnapshots, SegmentCommits)
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


class CollectionSnapshotsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = CollectionSnapshots
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


# class SnapshotFileMappingFactory(SQLAlchemyModelFactory):
#     class Meta:
#         model = SnapshotFileMapping
#         sqlalchemy_session = db.session_factory
#         sqlalchemy_session_persistence = 'commit'

#     file = factory.SubFactory(SegmentFilesFactory)
#     snapshot = factory.SubFactory(CollectionSnapshotsFactory)

BINARY_FILE = 1
STRING_FILE = 2
IVFSQ8_FILE = 3
FILE_TYPES = [BINARY_FILE, STRING_FILE, IVFSQ8_FILE]

def create_snapshot(collection, new_files, segment=None, prev=None):
    resources = []
    segment = segment if segment else collection.create_segment()
    resources.append(segment)
    for i in range(new_files):
        f = segment.create_file(ftype=random.choice(FILE_TYPES), lsn=get_lsn())
        resources.append(f)

    Commit(*resources)

    segment_commit = segment.commit_files(*resources[1:])

    Commit(segment_commit)
    snapshot = segment_commit.commit_snapshot()

    if prev:
        snapshot.append_mappings(*prev.mappings)
    snapshot.apply()

    return snapshot
