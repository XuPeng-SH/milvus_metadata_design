import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

from apps.managers import (CollectionsMgr, PartitionCommitsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db,
        Collections, FieldsMgr, FieldElementsMgr, SchemaCommitsMgr)

import logging
import faker
from database.models import Fields, FieldCommits, SchemaCommits
from database.factories import create_collection_commit
from database.utils import Commit

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')

# Define some const variables
VECTOR_FIELD = 1
INT_FIELD = 2
STRING_FIELD = 3

RAW = 1
IVFSQ8_INDEX = 100
IVFFLAT_INDEX = 101
PQ_INDEX = 102

FAKER = faker.Faker()


# Init databases
db.drop_all()
db.create_all()


# Init mgrs
seg_files_mgr = SegmentFilesMgr()
segs_mgr = SegmentsMgr()
seg_commits_mgr = SegmentsCommitsMgr(segs_mgr, seg_files_mgr)
fields_mgr = FieldsMgr()
field_elements_mgr = FieldElementsMgr()
schema_mgr = SchemaCommitsMgr()
collection_mgr = CollectionsMgr(schema_mgr)
ss_mgr = PartitionCommitsMgr(collection_mgr, seg_commits_mgr, keeps=2)

# Create a collection with status Pending
c1 = Collections(name=FAKER.word())
Commit(c1)

# Create and commit vector field
vf = Fields(name='vector', ftype=VECTOR_FIELD, params={'dimension': 512}, collection=c1,
        num=0)
Commit(vf)

vf_raw = vf.add_element(name='raw', ftype=RAW)

# Register PQ index for previous vector field and then commit
vf_pq = vf.add_element(name='pq', ftype=PQ_INDEX, params={'metric_type': 'IP'})
Commit(vf_raw, vf_pq)

# Freeze previous field to submit a field commit
vf_commit = FieldCommits(field=vf, mappings=[vf_pq.id, vf_raw.id], collection=c1)
Commit(vf_commit)

# Create and commit a str field
idf = Fields(name='id', ftype=STRING_FIELD, collection=c1, num=1)
idf_raw = idf.add_element(name='raw', ftype=RAW)
Commit(idf, idf_raw)

# Freeze above str field to submit a field commit
idf_commit = FieldCommits(field=idf, mappings=[idf_raw.id], collection=c1)
Commit(idf_commit)

# Create collection schema. Bind collection and previous field commits together to that schema
schema1 = SchemaCommits(collection=c1, mappings=[vf_commit.id, idf_commit.id])
Commit(schema1)

# Mark c1 as active
c1.activate()
Commit(c1)

# Submit to collection_mgr
collection_mgr.append(c1)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')


# Create a snapshot
s1 = create_collection_commit(c1)
Commit(s1)


# Append to snapshot manager
ss_mgr.append(s1)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')


# Get latest snapshot
latest = ss_mgr.get(c1.id)


# # Create a snapshot
s2 = create_collection_commit(c1)
Commit(s2)


# # Append to snapshot manager
ss_mgr.append(s2)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

ss_mgr.release(latest)
# segs_mgr.release(latest)
# logger.debug(f'{ss_mgr.active_snapshots(c1)}')

# logger.debug(f'Fields {fields_mgr.resources[c1.id]}')
ss_mgr.drop(c1)
# ss_mgr.drop(c1)
logger.debug(f'Fields {fields_mgr.resources[c1.id]}')
logger.debug(f'Elements {field_elements_mgr.resources[c1.id]}')
logger.debug(f'Collections {collection_mgr.resources}')
logger.debug(f'Segments {segs_mgr.resources}')
logger.debug(f'SegmentFiles {seg_files_mgr.resources}')

# # Drop s2. c1 also will be deleted
# ss_mgr.drop(c1)
# print(f'{ss_mgr.active_snapshots(c1)}')
# print(list(ss_mgr.collections_map.values())[0].id)
