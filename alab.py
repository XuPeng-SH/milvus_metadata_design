import sys
import random
import time
from collections import defaultdict
from init_db import db

SQLALCHEMY_DATABASE_URI='sqlite:////tmp/meta_lab/meta.sqlite?check_same_thread=False'
db.init_db(uri=SQLALCHEMY_DATABASE_URI)

import models

db.drop_all()
db.create_all()

from factories import (CollectionsFactory, CollectionFieldsFactory, CollectionFieldIndiceFactory,
        CollectionSnapshotsFactory, SegmentsFactory, SegmentFilesFactory, CollectionFieldIndice,
        SegmentFiles, SegmentCommits,
        CollectionSnapshots, Segments, Collections, CollectionFields)

LSN=0
def get_lsn():
    global LSN
    LSN += 1
    return LSN

def Commit(*instances, **kwargs):
    # print(f'Instances {instances}')
    session = db.Session
    for instance in instances:
        session.add(instance)
    to_add = kwargs.get('to_add', [])
    for i in to_add:
        session.add(i)
    to_delete = kwargs.get('to_delete', [])
    for i in to_delete:
        i and session.delete(i)

    session.commit()

BINARY_FILE = 1
STRING_FILE = 2
IVFSQ8_FILE = 3
FILE_TYPES = [BINARY_FILE, STRING_FILE, IVFSQ8_FILE]

def create_snapshot(new_files, prev=None):
    resources = []
    segment = collection.create_segment()
    resources.append(segment)
    for i in range(new_files):
        f = segment.create_file(ftype=random.choice(FILE_TYPES), lsn=get_lsn())
        resources.append(f)

    Commit(*resources)
    segment_commit = SegmentCommits(segment=segment)
    segment_commit.append_mappings(*resources[1:])
    segment_commit.apply()
    Commit(segment_commit)
    snapshot = collection.create_ss()
    snapshot.append_mappings(segment_commit)

    if prev:
        snapshot.append_mappings(*prev.mappings)
    snapshot.apply()

    return snapshot


VECTOR_FIELD = 1
INT_FIELD = 2
STRING_FIELD = 3

IVFSQ8 = 1
IVFFLAT = 2

collection = Collections()
vf = collection.create_field(name='vector', ftype=VECTOR_FIELD, params={'dimension': 512})
vfi = vf.add_index(name='sq8', ftype=IVFSQ8, params={'metric_type': 'L2'})
idf = collection.create_field(name='id', ftype=STRING_FIELD)
Commit(collection, vf, vfi, idf)
print(f'fields: {[ (f.name, f.params) for f in collection.fields.all()]}')

prev = None
for _ in range(10):
    start = time.time()
    snapshot = create_snapshot(2, prev=prev)
    Commit(snapshot, to_delete=[prev])
    end = time.time()
    print(f'Takes {end-start}')
    prev = snapshot

# print(f'Snapshots {collection.snapshots.all()}')
snapshots = collection.snapshots.all()
for ss in snapshots:
    print(f'{ss} {[f.id for f in ss.commits.all()]}')

segments = collection.segments.all()
for segment in segments:
#     f = segment.create_file(lsn=get_lsn())
#     Commit(f)
#     c = segment.commit_files(f)
#     Commit(c)
    print(f'{segment} {segment.commits.all()}')

sys.exit(0)
