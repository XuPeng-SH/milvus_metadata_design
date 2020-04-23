from init_db import db
SQLALCHEMY_DATABASE_URI='sqlite:////tmp/meta_lab/meta.sqlite?check_same_thread=False'
db.init_db(uri=SQLALCHEMY_DATABASE_URI)

from collections import defaultdict, OrderedDict
from models import Segments, SegmentCommits, Collections, CollectionSnapshots, SegmentFiles

class Proxy:
    def __init__(self, node, cleanup=None):
        self.node = node
        self.prev = None
        self.next = None
        self.refcnt = 0
        self.cleanupcb = []
        cleanup and self.cleanupcb.append(cleanup)

    def set_next(self, next_node):
        next_node.prev = self
        self.next = next_node
        if self.refcnt == 0:
            self.cleanup()
        return self

    def set_prev(self, prev_node):
        prev_node.next = self
        self.prev = prev_node

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.node, attr)

    def is_tail(self):
        return self.next == None

    def is_head(self):
        return self.prev == None

    def ref(self):
        self.refcnt += 1

    def unref(self):
        self.refcnt -= 1
        if self.refcnt <= 0:
            self.cleanup()

    def register_cb(self, cb):
        self.cleanupcb.append(cb)

    def cleanup(self):
        # self.cleanupcb and self.cleanupcb(self.node)
        for cb in self.cleanupcb:
            cb(self.node)
        self.do_cleanup()

    def do_cleanup(self):
        pass


class DBProxy(Proxy):
    def __init__(self, node, cleanup=None):
        super().__init__(node, cleanup=cleanup)
        self.model = node.__class__

    def do_cleanup(self):
        print(f'Doing CLEANUP {self.model.__name__} {self.node.id}')
        # db.Session.delete(self.node)
        # db.Session.commit()


class HirachyDBProxy(DBProxy):
    def __init__(self, node, parent, cleanup=None):
        super().__init__(node, cleanup=cleanup)
        self.parent = parent
        self.parent.ref()

    def do_cleanup(self):
        super().do_cleanup()
        self.parent.unref()


class ResourceMgr:
    level_one_model = None
    level_two_model = None
    link_key = ''
    proxy_class = DBProxy
    def __init__(self):
        self.resources = defaultdict(OrderedDict)

    def cleanupcb(self, target):
        level_one_key = getattr(target, self.link_key)
        print(f'Removing l1={level_one_key} l2={target.id}')
        self.resources[level_one_key].pop(target.id, None)

    def load(self, level_one_id):
        lid = level_one_id
        if isinstance(level_one_id, self.level_one_model):
            lid = level_one_id.id

        records = db.Session.query(self.level_two_model).filter(getattr(self.level_two_model,
            self.link_key)==lid).all()

        for record in records:
            proxy = self.proxy_class(record, cleanup=self.cleanupcb)
            self.resources[lid][record.id] = proxy

        # for k, v in self.resources.items():
        #     print(f'{k} --')
        #     for kk, vv in v.items():
        #         print(f'\t{kk}')

    def get(self, level_one_id, level_two_id=None):
        assert level_two_id is not None
        lid = level_one_id
        if isinstance(level_one_id, self.level_one_model):
            lid = level_one_id.id

        level_one_resource = self.resources.get(lid, None)
        if not level_one_resource:
            self.load(lid)
            level_one_resource = self.resources.get(lid, None)
            if not level_one_resource:
                return None

        ss = level_one_resource.get(level_two_id, None)
        # ss and print(f'PRE Get SS {ss.id} ref={ss.refcnt}')
        ss and ss.ref()
        return ss

    def release(self, resource):
        resource and resource.unref()


class SegmentFilesMgr(ResourceMgr):
    level_one_model = Collections
    level_two_model = SegmentFiles
    link_key = 'collection_id'

    def __init__(self, segment_commit_mgr):
        super().__init__()
        self.segment_commit_mgr = segment_commit_mgr


class SegmentsMgr(ResourceMgr):
    level_one_model = Collections
    level_two_model = Segments
    link_key = 'collection_id'


class SegmentsCommitsMgr(ResourceMgr):
    level_one_model = Collections
    level_two_model = SegmentCommits
    link_key = 'collection_id'

    def __init__(self, segment_mgr):
        super().__init__()
        self.segment_mgr = segment_mgr


class SnapshotsMgr:
    def __init__(self):
        self.all_snapshots = defaultdict(OrderedDict)
        self.heads = {}
        self.tails = {}
        self.keeps = 5
        self.stale_sss = defaultdict(OrderedDict)

    def load_snapshots(self, collection):
        cid = collection
        if isinstance(collection, Collections):
            cid = collection.id

        snapshots = db.Session.query(CollectionSnapshots).filter(CollectionSnapshots.collection_id==cid
                ).order_by(CollectionSnapshots.id.desc()).all()

        next_node = None
        num = 0
        for ss in snapshots:
            proxy = DBProxy(ss)
            num += 1
            if num > self.keeps:
                proxy.unref()
                continue

            proxy.ref()
            if next_node is not None:
                proxy.set_next(next_node)
            else:
                self.tails[cid] = proxy
            self.all_snapshots[cid][ss.id] = proxy
            next_node = proxy

        if next_node is not None:
            self.heads[cid] = next_node

        print(f'Len of SS: {len(self.all_snapshots[cid])}')

    def close_snapshots(self, collection):
        cid = collection
        if isinstance(collection, Collections):
            cid = collection.id

        self.all_snapshots.pop(cid)
        self.heads.pop(cid, None)
        self.tails.pop(cid, None)

    def get_snapshot(self, collection, snapshot_id=None):
        # import pdb;pdb.set_trace()
        cid = collection
        if isinstance(cid, Collections):
            cid = cid.id
        collection_sss = self.all_snapshots.get(cid, None)
        if not collection_sss:
            self.load_snapshots(cid)
            collection_sss = self.all_snapshots.get(cid, None)
            if not collection_sss:
                return None
        if not snapshot_id:
            snapshot_id = self.tails[cid].id
            # return self.tails[cid]

        ss = collection_sss.get(snapshot_id, None)
        ss and print(f'PRE Get SS {ss.id} ref={ss.refcnt}')
        ss and ss.ref()
        ss and print(f'POST Get SS {ss.id} ref={ss.refcnt}')
        return ss

    def release_snapshot(self, snapshot):
        snapshot and snapshot.unref()

    def cleanupcb(self, node):
        print(f'CLEANUP: Removing {node.id} from stale snapshots list')
        self.stale_sss[node.collection.id].pop(node.id, None)

    def mark_as_stale(self, ss):
        self.stale_sss[ss.collection.id][ss.id] = ss
        ss.register_cb(self.cleanupcb)
        ss.unref()

    def drop(self, collection):
        cid = collection
        if isinstance(cid, Collections):
            cid = cid.id
        head = self.heads.pop(cid, None)
        if not head:
            return
        self.all_snapshots[cid].pop(head.id, None)
        next_node = head.next
        if not next_node:
            return
        next_node.prev = None
        self.heads[cid] = next_node

        self.mark_as_stale(head)

    def append(self, snapshot):
        cid = snapshot.collection
        if isinstance(cid, Collections):
            cid = cid.id
        proxy = DBProxy(snapshot)
        tail = self.tails.get(cid, None)
        if tail:
            assert proxy.id > tail.id
            proxy.set_prev(tail)
            tail.set_next(proxy)

        self.tails[cid] = proxy
        self.all_snapshots[cid][proxy.id] = proxy

        if len(self.all_snapshots[cid]) > self.keeps:
            self.drop(cid)

    def active_snapshots(self, collection):
        cid = collection
        if isinstance(cid, Collections):
            cid = cid.id
        return sorted(self.all_snapshots[cid].keys()), self.stale_sss[cid].keys()


if __name__ == '__main__':
    import sys
    collection = db.Session.query(Collections).first()
    seg_mgr = SegmentsMgr()
    seg_mgr.load(collection)
    segment = seg_mgr.get(collection, 2)
    print(f'Segment cid={segment.collection.id} sid={segment.id}')
    seg_mgr.release(segment)

    seg_commit_mgr = SegmentsCommitsMgr(seg_mgr)
    seg_commit_mgr.load(collection)
    segment_commit = seg_commit_mgr.get(collection, segment.commits.first().id)
    seg_commit_mgr.release(segment_commit)
    # print(f'Segment cid={segment.collection.id} sid={segment.id}')

    seg_files_mgr = SegmentFilesMgr(seg_commit_mgr)
    seg_files_mgr.load(collection)


    sys.exit(0)

    ss_mgr = SnapshotsMgr()
    ss_mgr.load_snapshots(collection)
    s7 = ss_mgr.get_snapshot(collection, 7)
    s8 = ss_mgr.get_snapshot(collection, 8)

    new_ss = collection.create_snapshot()
    db.Session.add(new_ss)
    db.Session.commit()
    ss_mgr.append(new_ss)

    new_ss = collection.create_snapshot()
    db.Session.add(new_ss)
    db.Session.commit()
    ss_mgr.append(new_ss)

    import time
    time.sleep(0.1)
    print(f'Actives: {ss_mgr.active_snapshots(collection)}')

    ss_mgr.release_snapshot(s7)
    ss_mgr.release_snapshot(s8)

    ss_mgr.close_snapshots(collection)
