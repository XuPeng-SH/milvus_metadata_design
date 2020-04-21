import random
import time
from init_db import db

SQLALCHEMY_DATABASE_URI='sqlite:////tmp/meta_lab/meta.sqlite?check_same_thread=False'
db.init_db(uri=SQLALCHEMY_DATABASE_URI)

import models

db.drop_all()
db.create_all()

from factories import (CollectionsFactory, CollectionFieldsFactory, CollectionFieldIndiceFactory,
        SnapshotsFactory, SegmentsFactory, SegmentFilesFactory, SegmentCommitsFactory,
        CommitFileMappingFactory, SegmentFiles, SegmentCommits, CommitFileMapping,
        Snapshots, Segments)

UID=0
def get_uid():
    global UID
    UID += 1
    return UID

collection = CollectionsFactory()
collection.version = {'one': '1'}
collection.save()

field0 = CollectionFieldsFactory(num=0, collection=collection)
field1 = CollectionFieldsFactory(num=1, collection=collection)

fields = collection.fields.all()

for field in fields:
    CollectionFieldIndiceFactory.create_batch(random.randint(1,4) ,field=field)
    print(field.num, field.name, field.ftype)
    indice = field.indice.all()
    for index in indice:
        print(f'\t\t{index.name} {index.ftype}')

# SegmentsFactory.create_batch(random.randint(100, 200), collection=collection)
start = time.time()
for _ in range(random.randint(1000, 2000)):
    Segments(collection=collection, id=get_uid())
end = time.time()
print(f'Create segments takes {end-start}')
segments = collection.segments.all()
for segment in segments:
    for _ in range(random.randint(3, 6)):
        SegmentFiles(id=get_uid(), segment=segment)
        # SegmentFilesFactory(segment=segment)
    # SegmentFilesFactory.create_batch(random.randint(3, 6), segment=segment)
    # print(f'Segment {segment.id} {segment.collection.name}')
    # files = segment.files.all()
    # for f in files:
    #     print(f'\t\t{f.id} {f.lsn} {f.entity_cnt} {f.size} {f.segment.id}')


start = time.time()
session = db.Session
snapshot = Snapshots(collection=collection, id=get_uid())
for segment in segments:
    c = SegmentCommits(segment=segment, id=get_uid(), snapshot=snapshot)
    session.add(c)
    for _ in range(random.randint(1, 5)):
        f = SegmentFiles(segment=segment, id=get_uid())
        mapping = CommitFileMapping(file=f, commit=c, id=get_uid())
        session.add(f)
        session.add(mapping)
session.commit()
end = time.time()
print(f'Create Snapshots Takes {end-start}')

# for segment in segments:
#     commits = segment.commits.all()
#     print(f'Segment {segment.id} Commits')
#     for commit in commits:
#         print(f'\t{commit.id} {commit.files}')

# for segment in segments:
#     files = segment.files.all()
#     print(f'Segment {segment.id}')
#     for f in files:
#         print(f'\tfile {f.id} {f.commit[0].id if f.commit else ""}')

# SnapshotsFactory.create_batch(random.randint(2,6), collection=collection)

snapshots = collection.snapshots.all()
for snapshot in snapshots:
    print(f'Snapshot {snapshot.id} Collection {snapshot.collection.name}')
    commits = snapshot.commits.all()
    # for c in commits:
    #     print(f'\tCommit {c.id} Files {[f.id for f in c.files]}')

# for segment in segments:
#     SegmentCommitsFactory.create_batch(random.randint(2, 4), segment=segment)
#     commits = segment.commits.all()
#     print(f'Segment {segment.id}')
#     for c in commits:
#         print(f'\tcommit {c.id}')
