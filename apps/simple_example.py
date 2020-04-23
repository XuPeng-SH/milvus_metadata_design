import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

from apps.managers import (CollectionsMgr, SnapshotsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db,
        Collections)

from database.factories import create_snapshot
from database.utils import Commit

# Init Mgr
db.drop_all()
db.create_all()
seg_files_mgr = SegmentFilesMgr()
segs_mgr = SegmentsMgr()
seg_commits_mgr = SegmentsCommitsMgr(segs_mgr, seg_files_mgr)
collection_mgr = CollectionsMgr()
ss_mgr = SnapshotsMgr(collection_mgr, seg_commits_mgr, keeps=1)

# Create a new Collection
c1 = Collections()
Commit(c1)

# Submit to collection_mgr
collection_mgr.append(c1)
print(f'{ss_mgr.active_snapshots(c1)}')

# Create a snapshot
s1 = create_snapshot(c1, 3)
Commit(s1)

# Append to snapshot manager
ss_mgr.append(s1)
print(f'{ss_mgr.active_snapshots(c1)}')

# Get latest snapshot
latest = ss_mgr.get(c1.id)

# # Create a snapshot
s2 = create_snapshot(c1, 3)
Commit(s2)

# # Append to snapshot manager
ss_mgr.append(s2)
print(f'{ss_mgr.active_snapshots(c1)}')

segs_mgr.release(latest)
print(f'{ss_mgr.active_snapshots(c1)}')

# # Drop s2. c1 also will be deleted
# ss_mgr.drop(c1)
# print(f'{ss_mgr.active_snapshots(c1)}')
# print(list(ss_mgr.collections_map.values())[0].id)
