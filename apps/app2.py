import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

from app3 import (CollectionsMgr, SnapshotsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db,
        Collections)

# Init Mgr
db.drop_all()
db.create_all()
seg_files_mgr = SegmentFilesMgr()
segs_mgr = SegmentsMgr()
seg_commits_mgr = SegmentsCommitsMgr(segs_mgr, seg_files_mgr)
collection_mgr = CollectionsMgr()
ss_mgr = SnapshotsMgr(collection_mgr, seg_commits_mgr)

# Create a new Collection
collection = Collections()
db.Session.add(collection)
db.Session.commit()

# Submit to collection_mgr
collection_mgr.manage(collection)

#

print(list(ss_mgr.collections_map.values())[0].id)
