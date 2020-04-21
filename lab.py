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
        SnapshotsFactory, SegmentsFactory, SegmentFilesFactory,
        SnapshotFileMappingFactory, SegmentFiles, SnapshotFileMapping,
        Snapshots, Segments, Collections)

LSN=0
def get_lsn():
    global LSN
    LSN += 1
    return LSN

def Commit(*instances):
    # print(f'Instances {instances}')
    session = db.Session
    for instance in instances:
        session.add(instance)
    session.commit()

BINARY_FILE = 1
STRING_FILE = 2
IVFSQ8_FILE = 3
FILE_TYPES = [BINARY_FILE, STRING_FILE, IVFSQ8_FILE]

def create_snapshot(new_files, prev=None):
    resources = []
    to_delete = []
    snapshot = collection.create_snapshot()
    resources.append(snapshot)
    segment = collection.create_segment()
    resources.append(segment)
    for i in range(new_files):
        f = segment.create_file(ftype=random.choice(FILE_TYPES), lsn=get_lsn())
        m = f.submit(snapshot)
        resources.append(f)
        resources.append(m)

    if prev:
        r = prev.submit(snapshot)
        # print(f'PREV {r}')
        resources.extend(r)
        return resources

    return resources

collection = Collections().save()
print(f'Collection {collection.id}')
start = time.time()
resources = create_snapshot(2000)
Commit(*resources)
end = time.time()
print(f'Takes {end-start}')

start = time.time()
prev_ss = resources[0]
resources = create_snapshot(2000, prev_ss)
Commit(*resources)
end = time.time()
print(f'Takes {end-start}')

start = time.time()
prev_ss = resources[0]
resources = create_snapshot(2000, prev_ss)
Commit(*resources)
end = time.time()
print(f'Takes {end-start}')
# print([commit.files for commit in this_ss.commits.all()])

# this_ss.get_resources()
# Commit(snapshot)
# print(f'Segment {segment.id} SegmentFile {seg_file1.id} lsn {seg_file1.lsn} {seg_file1.segment.id}')
# print(f'Segment {segment.id} SegmentFile {seg_file2.id} lsn {seg_file2.lsn} {seg_file2.segment.id}')

# ss = collection.snapshots.first()
# print(f'{ss.commits.first().files}')


sys.exit(0)
