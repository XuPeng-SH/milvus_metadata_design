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
        CollectionSnapshotsFactory, SegmentsFactory, SegmentFilesFactory,
        SnapshotFileMappingFactory, SegmentFiles, SnapshotFileMapping,
        CollectionSnapshots, Segments, Collections)

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
    # to_delete = []
    snapshot = collection.create_ss()
    segment = collection.create_segment()
    resources.append(segment)
    for i in range(new_files):
        f = segment.create_file(ftype=random.choice(FILE_TYPES), lsn=get_lsn())
        resources.append(f)

    Commit(*resources)
    snapshot.append_mappings(*resources[1:])

    if prev:
        snapshot.append_mappings(*prev.mappings)
    snapshot.apply()

    return snapshot

collection = Collections().save()
print(f'Collection {collection.id}')

prev = None
for _ in range(10):
    start = time.time()
    snapshot = create_snapshot(5, prev=prev)
    Commit(snapshot)
    end = time.time()
    print(f'Takes {end-start}')
    prev = snapshot

sys.exit(0)
