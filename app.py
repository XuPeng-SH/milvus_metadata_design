from data import CollectionsMgr, SnapshotsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db

seg_files_mgr = SegmentFilesMgr()
segs_mgr = SegmentsMgr()
seg_commits_mgr = SegmentsCommitsMgr(segs_mgr, seg_files_mgr)

collection_mgr = CollectionsMgr()
ss_mgr = SnapshotsMgr(collection_mgr, seg_commits_mgr)
# print(list(ss_mgr.collections_map.values())[0].id)
ss_mgr.drop(1)
