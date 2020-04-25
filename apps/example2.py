import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

from apps.managers import (CollectionsMgr, SnapshotsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db,
        Collections, FieldsMgr, FieldElementsMgr)

import logging
from database.factories import create_snapshot
from database.utils import Commit

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')

# Init Mgr
db.drop_all()
db.create_all()
seg_files_mgr = SegmentFilesMgr()
segs_mgr = SegmentsMgr()
seg_commits_mgr = SegmentsCommitsMgr(segs_mgr, seg_files_mgr)
fields_mgr = FieldsMgr()
field_elements_mgr = FieldElementsMgr()
collection_mgr = CollectionsMgr(fields_mgr, field_elements_mgr)
ss_mgr = SnapshotsMgr(collection_mgr, seg_commits_mgr, keeps=2)

VECTOR_FIELD = 1
INT_FIELD = 2
STRING_FIELD = 3

IVFSQ8 = 1
IVFFLAT = 2

# Create a new Collection
c1 = Collections()
vf = c1.create_field(name='vector', ftype=VECTOR_FIELD, params={'dimension': 512})
vfi = vf.add_index(name='sq8', ftype=IVFSQ8, params={'metric_type': 'L2'})
idf = c1.create_field(name='id', ftype=STRING_FIELD)
Commit(c1, vf, vfi, idf)
# import pdb;pdb.set_trace()
logger.debug(f'Fields: {[ (f.name, f.params) for f in c1.fields.all()]}')

# Submit to collection_mgr
collection_mgr.append(c1)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

# Create a snapshot
s1 = create_snapshot(c1, 3)
Commit(s1)

# Append to snapshot manager
ss_mgr.append(s1)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

# Get latest snapshot
latest = ss_mgr.get(c1.id)

# # Create a snapshot
s2 = create_snapshot(c1, 3)
Commit(s2)

# # Append to snapshot manager
ss_mgr.append(s2)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

segs_mgr.release(latest)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

# logger.debug(f'Fields {fields_mgr.resources[c1.id]}')
ss_mgr.drop(c1)
ss_mgr.drop(c1)
logger.debug(f'Fields {fields_mgr.resources[c1.id]}')
logger.debug(f'Elements {field_elements_mgr.resources[c1.id]}')
logger.debug(f'Collections {collection_mgr.resources}')
logger.debug(f'Segments {segs_mgr.resources}')
logger.debug(f'SegmentFiles {seg_files_mgr.resources}')

# # Drop s2. c1 also will be deleted
# ss_mgr.drop(c1)
# print(f'{ss_mgr.active_snapshots(c1)}')
# print(list(ss_mgr.collections_map.values())[0].id)
