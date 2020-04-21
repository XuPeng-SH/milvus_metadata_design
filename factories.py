import time
from collections import defaultdict
import factory
from factory.alchemy import SQLAlchemyModelFactory
from faker import Faker
from faker.providers import BaseProvider
from models import (Tables, TableFiles, Collections, CollectionFields, CollectionFieldIndice,
        Snapshots, Segments, SegmentFiles, SegmentCommits, CommitFileMapping)
from models import db


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

    id = factory.Faker('random_number', digits=16, fix_len=True)
    name = factory.Faker('word')


class CollectionFieldsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = CollectionFields
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    name = factory.Faker('word')
    ftype = factory.Faker('random_element', elements=(0,1,2,3,5))
    collection = factory.SubFactory(CollectionsFactory)


class CollectionFieldIndiceFactory(SQLAlchemyModelFactory):
    class Meta:
        model = CollectionFieldIndice
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    name = factory.Faker('word')
    ftype = factory.Faker('random_element', elements=(0,1,2,3,5))
    field = factory.SubFactory(CollectionFieldsFactory)


class SnapshotsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Snapshots
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    collection = factory.SubFactory(CollectionsFactory)


class SegmentsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Segments
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    collection = factory.SubFactory(CollectionsFactory)


class SegmentFilesFactory(SQLAlchemyModelFactory):
    class Meta:
        model = SegmentFiles
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    segment = factory.SubFactory(SegmentsFactory)
    ftype = factory.Faker('random_element', elements=(0,1,2,3,5))


class SegmentCommitsFactory(SQLAlchemyModelFactory):
    class Meta:
        model = SegmentCommits
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    segment = factory.SubFactory(SegmentsFactory)
    snapshot = factory.SubFactory(SnapshotsFactory)


class CommitFileMappingFactory(SQLAlchemyModelFactory):
    class Meta:
        model = CommitFileMapping
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    file = factory.SubFactory(SegmentFilesFactory)
    commit = factory.SubFactory(SegmentCommits)


class TablesFactory(SQLAlchemyModelFactory):
    class Meta:
        model = Tables
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    table_id = factory.Faker('uuid4')
    state = factory.Faker('random_element', elements=(0, 1))
    dimension = factory.Faker('random_element', elements=(256, 512))
    created_on = int(time.time())
    index_file_size = 0
    engine_type = factory.Faker('random_element', elements=(0, 1, 2, 3))
    metric_type = factory.Faker('random_element', elements=(0, 1))
    # nlist = 16384


class TableFilesFactory(SQLAlchemyModelFactory):
    class Meta:
        model = TableFiles
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    table = factory.SubFactory(TablesFactory)
    engine_type = factory.Faker('random_element', elements=(0, 1, 2, 3))
    file_id = factory.Faker('uuid4')
    file_type = factory.Faker('random_element', elements=(0, 1, 2, 3, 4))
    file_size = factory.Faker('random_number')
    updated_time = int(time.time())
    created_on = int(time.time())
    date = factory.Faker('this_date')
