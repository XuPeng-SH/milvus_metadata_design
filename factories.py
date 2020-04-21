import time
from collections import defaultdict
import factory
from factory.alchemy import SQLAlchemyModelFactory
from faker import Faker
from faker.providers import BaseProvider
from models import (Collections, CollectionFields, CollectionFieldIndice,
        Snapshots, Segments, SegmentFiles, SnapshotFileMapping)
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


# class SegmentCommitsFactory(SQLAlchemyModelFactory):
#     class Meta:
#         model = SegmentCommits
#         sqlalchemy_session = db.session_factory
#         sqlalchemy_session_persistence = 'commit'

#     id = factory.Faker('random_number', digits=16, fix_len=True)
#     segment = factory.SubFactory(SegmentsFactory)
#     snapshot = factory.SubFactory(SnapshotsFactory)


class SnapshotFileMappingFactory(SQLAlchemyModelFactory):
    class Meta:
        model = SnapshotFileMapping
        sqlalchemy_session = db.session_factory
        sqlalchemy_session_persistence = 'commit'

    id = factory.Faker('random_number', digits=16, fix_len=True)
    file = factory.SubFactory(SegmentFilesFactory)
    snapshot = factory.SubFactory(Snapshots)
