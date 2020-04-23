import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

import random
import time
from collections import defaultdict
import threading
from database import db

# SQLALCHEMY_DATABASE_URI='sqlite:////tmp/meta_lab/meta.sqlite?check_same_thread=False'
# db.init_db(uri=SQLALCHEMY_DATABASE_URI)

import database.models

db.drop_all()
db.create_all()

from database.factories import (CollectionsFactory, CollectionFieldsFactory, CollectionFieldIndiceFactory,
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

class Head: pass

class SnapshotsProxy:
    def __init__(self, node, prev=None):
        self.node = node
        self._prev = prev

    @property
    def prev(self):
        if isinstance(self._prev, Head):
            return None
        if self._prev is not None:
            return self._prev

        self._prev = db.Session.query(CollectionSnapshots).filter(
                CollectionSnapshots.id < self.node.id).order_by(CollectionSnapshots.id.desc()).first()
        if self._prev is None:
            self._prev = Head()
            return None

        return self._prev

class SnapshotsList:
    def __init__(self, from_db=True):
        self.nodes = []
        if from_db:
            self.nodes = db.Session.query(CollectionSnapshots).order_by(CollectionSnapshots.id
                    ).all()

    @property
    def current(self):
        return self.nodes[-1]


def create_snapshot(new_files, segment=None, prev=None):
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


import queue
class Woker(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.q = queue.Queue()
        self.stale_snapshot = set()

    def handle_event(self, event):
        print(f'Handling event {event}')
        self.stale_snapshot.add(event)

        print(f'{event.stale_commit_ids}')
        # print(f'NEW {event.new_commits}')
        # Commit(to_delete=[event])

    def run(self):
        while True:
            event = self.q.get()
            print(f'event  {event}')
            if not event:
                break
            self.handle_event(event)

    def stop(self):
        self.q.put(False)
        self.join()

    def submit(self, event):
        self.q.put(event)

class WorkerContext:
    def __init__(self, current, prev=None):
        self.curr_id = current.id
        self.curr_mappings = set(current.mappings)
        self.prev_id = prev.id if prev else None
        self.prev_mappings = set(prev.mappings) if prev else set()
        self.stale_commit_ids = self.prev_mappings - self.curr_mappings
        self.new_commit_ids = self.curr_mappings - self.prev_mappings
        self._stale_files = None
        self._stale_commits = None
        self._new_commits = None

    @property
    def new_commits(self):
        if self._new_commits is not None:
            return self._new_commits
        if not self.new_commit_ids:
            self._new_commits = []
            return self._new_commits

        self._new_commits = db.Session.query(SegmentCommits).filter(
                SegmentCommits.id.in_(self.new_commit_ids)
                ).order_by(SegmentCommits.segment_id).all()
        return self._new_commits

    @property
    def stale_commits(self):
        if self._stale_commits is not None:
            return self._stale_commits
        if not self.stale_commit_ids:
            self._stale_commits = []
            return self._stale_commits

        self._stale_commits = db.Session.query(SegmentCommits).filter(
                SegmentCommits.id.in_(self.stale_commit_ids)
                ).order_by(SegmentCommits.segment_id).all()
        return self._stale_commits

    @property
    def stale_files(self):
        if self._stale_files is not None:
            return self._stale_files

        if not self.stale_commits:
            self._stale_files = []
            return self._stale_files
        stale_related_segment_ids = set([commit.segment_id for commit in self.stale_commits])
        new_related_segment_ids = set([commit.segment_id for commit in self.new_commits])
        stale_segments = stale_related_segment_ids - new_related_segment_ids
        common_segments = stale_related_segment_ids & new_related_segment_ids

        if len(stale_segments) == 0 and len(common_segments) == 0:
            self._stale_files = []
            return self._stale_files

        cursor = 0
        prev_common_commits = []
        for commit in self.stale_commits:
            for segment_id in common_segments[cursor:]:
                cursor = segment_id
                if commit.segment_id == segment_id:
                    prev_common_commits.append(commit)
                    break

        curr_comm_commits = []
        for commit in self.new_commits:
            for segment_id in common_segments[cursor:]:
                cursor = segment_id
                if commit.segment_id == segment_id:
                    curr_comm_commits.append(commit)
                    break


        prev_possible_stale_files = []
        for commit in prev_common_commits:
            prev_common_commits.extend(commit.files.all())

        curr_files = []
        for commit in curr_comm_commits:
            curr_files.extend(commit.files.all())

        diff_stale_files = set(prev_common_commits) - set(curr_files)


        # db.Session.query(SegmentCommits)..filter().filter(SegmentCommits.segment_id.in_(stale_related_segment_ids))

        return self._stale_files


VECTOR_FIELD = 1
INT_FIELD = 2
STRING_FIELD = 3

IVFSQ8 = 1
IVFFLAT = 2

worker = Woker()
worker.start()

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
    Commit(snapshot)
    context = WorkerContext(snapshot, prev)
    prev and worker.submit(context)

    end = time.time()
    print(f'Takes {end-start}')
    prev = snapshot

commit = prev.commits.first()
ss = create_snapshot(3, segment=commit.segment, prev=prev)
Commit(ss)
context = WorkerContext(ss, prev)
print(f'XXXXXX {context.stale_files}')
prev and worker.submit(context)

# print(f'Snapshots {collection.snapshots.all()}')
time.sleep(0.1)
snapshots = collection.snapshots.all()
for ss in snapshots:
    print(f'{ss} {[f.id for f in ss.commits.all()]}')
    p = SnapshotsProxy(ss)
    prev = p.prev
    print(f'Snapshots {ss.id} prev={prev.id if prev else None}')

smgr = SnapshotsList()
print(f'Current {smgr.current.id}')

# segments = collection.segments.all()
# for segment in segments:
#     f = segment.create_file(lsn=get_lsn())
#     Commit(f)
#     c = segment.commit_files(f)
#     Commit(c)
    # print(f'{segment} {segment.commits.all()}')

# time.sleep(0.1)
worker.stop()
sys.exit(0)
