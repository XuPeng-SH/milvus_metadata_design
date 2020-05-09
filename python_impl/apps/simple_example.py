import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

from apps.managers import (CollectionsMgr, PartitionCommitsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db,
        Collections, CollectionFieldsMgr, CollectionFieldIndiceMgr)

import logging
import faker
from database.factories import create_collection_commit
from database.utils import Commit

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')

FAKER = faker.Faker()

# Init Mgr
db.drop_all()
db.create_all()

seg_files_mgr = SegmentFilesMgr()
segs_mgr = SegmentsMgr()
seg_commits_mgr = SegmentsCommitsMgr(segs_mgr, seg_files_mgr)
fields_mgr = CollectionFieldsMgr()
indice_mgr = CollectionFieldIndiceMgr()
collection_mgr = CollectionsMgr(fields_mgr, indice_mgr)
ss_mgr = PartitionCommitsMgr(collection_mgr, seg_commits_mgr, keeps=1)

# Create a new Collection
c1 = Collections(name=FAKER.word())
Commit(c1)

# Submit to collection_mgr
collection_mgr.append(c1)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

# Create a snapshot
s1 = create_collection_commit(c1, 3)
Commit(s1)

# Append to snapshot manager
ss_mgr.append(s1)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

# Get latest snapshot
latest = ss_mgr.get(c1.id)

# # Create a snapshot
s2 = create_collection_commit(c1, 3)
Commit(s2)

# # Append to snapshot manager
ss_mgr.append(s2)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

segs_mgr.release(latest)
logger.debug(f'{ss_mgr.active_snapshots(c1)}')

# # Drop s2. c1 also will be deleted
# ss_mgr.drop(c1)
# print(f'{ss_mgr.active_snapshots(c1)}')
# print(list(ss_mgr.collections_map.values())[0].id)
