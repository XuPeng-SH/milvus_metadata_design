import logging
import enum
import threading
import inspect
from collections import defaultdict, OrderedDict
from functools import partial
from database import db
from database.utils import Commit
from database.models import (Segments, SegmentCommits, Collections, PartitionCommits, SegmentFiles,
        Fields, FieldElements, SchemaCommits, FieldCommits, LogSequenceNumbers, Partitions, CollectionCommits)

from utils import singleton, get_lsn, ReferenceContext, set_lsn
from utils.wrapper import DBInstanceWrapper
from utils.resource_manager import (Level2ResourceMgr, AdvancedLevel2ResourceMgr, MappingsMixin,
        BinUnrefFirstCB, Level1ResourceMgr)

logger = logging.getLogger(__name__)


class SegmentFilesMgr(AdvancedLevel2ResourceMgr):
    level_one_model = Partitions
    level_two_model = SegmentFiles
    link_key = 'partition_id'
    depend_resource_attrs = frozenset(['field_element_id'])

    def update_depend_field_element_id_resource(self, resource, context=None):
        self.default_update_depend_resource(attr='field_element_id', resource=resource,
                level1_id=resource.collection_id,
                level2_id=resource.field_element_id, context=context)


class SegmentsMgr(Level2ResourceMgr):
    level_one_model = Partitions
    level_two_model = Segments
    link_key = 'partition_id'


class SegmentsCommitsMgr(AdvancedLevel2ResourceMgr, MappingsMixin):
    level_one_model = Partitions
    level_two_model = SegmentCommits
    link_key = 'partition_id'
    depend_resource_attrs = frozenset(['segment_id', 'mappings', 'schema_id'])

    def update_depend_schema_id_resource(self, resource, context=None):
        self.default_update_depend_resource(attr='schema_id', resource=resource,
                level1_id=resource.collection_id,
                level2_id=resource.schema_id, context=context)


class FieldsMgr(Level2ResourceMgr):
    level_one_model = Collections
    level_two_model = Fields
    link_key = 'collection_id'

    def ids(self, collection_id, context=None):
        fields = self.resources.get(collection_id, [])
        return [field for field in fields]


class FieldElementsMgr(Level2ResourceMgr):
    level_one_model = Collections
    level_two_model = FieldElements
    link_key = 'collection_id'

    def ids(self, collection_id, context=None):
        elements = self.resources.get(collection_id, [])
        return [element for element in elements]

    def load_level2_resources(self, level1_id, context=None, **kwargs):
        subq = self.db.Session.query(Fields).filter(Fields.collection_id==level1_id).subquery()
        query = self.db.Session.query(self.level_two_model).filter(self.level_two_model.field_id==subq.c.id)
        level2_id = kwargs.pop('level2_id', None)
        if level2_id is not None:
            query = query.filter(getattr(self.level_two_model, self.pk)==level2_id)
        records = query.all()
        return records


class FieldCommitsMgr(AdvancedLevel2ResourceMgr, MappingsMixin):
    level_one_model = Collections
    level_two_model = FieldCommits
    link_key = 'collection_id'
    depend_resource_attrs = frozenset(['field_id', 'mappings'])


class CollectionsMgr(Level1ResourceMgr):
    level_one_model = Collections

    def append(self, new_collection_id, context=None):
        nid = new_collection_id
        if not isinstance(nid, str):
            nid = new_collection_id.id
        self.load(nid, context=context)


class MVMixin:
    def __init__(self, keeps=1, **kwargs):
        self.heads = {}
        self.tails = {}
        self.keeps = keeps
        self.stale_commits = defaultdict(OrderedDict)

    def get_default_level2_id(self, level1_id, context=None):
        return True, max(map(lambda x: x.id, self.resources[level1_id].values()))

    def load_level2_resources(self, level1_id, context=None, **kwargs):
        records = super().load_level2_resources(level1_id, context=context, **kwargs)
        return sorted(records, key=lambda record: record.id, reverse=True)

    def update_level2_resources(self, level1_id, records, context=None):
        next_node = None
        num = 0

        context = context if context else self.context

        for record in records:
            num += 1
            wrapper = self.update_level2_resource(record.collection_id, record.id, record, context=context)

            if num > self.keeps:
                # context.add_tag('UPDATE')
                wrapper.unref(context.clone().add_tag('UPDATE[L2]'))
                continue

            # context.add_tag('UPDATE')
            wrapper.ref(context.clone().add_tag('UPDATE[L2]'))
            if next_node is not None:
                wrapper.set_next(next_node)
            else:
                self.tails[level1_id] = wrapper
            # self.resources[level1_id][record.id] = wrapper
            self.add_resource(level1_id, record.id, wrapper)
            next_node = wrapper

        if next_node is not None:
            self.heads[level1_id] = next_node

    def stale_cb(self, node):
        logger.debug(f'StaleCB Removing {node.id}')
        self.stale_commits[node.collection.id].pop(node.id, None)
        if len(self.stale_commits[node.collection.id]) == 0 and len(self.resources[node.collection.id]) == 0:
            self.stale_commits.pop(node.collection.id, None)
            # self.resources.pop(node.collection.id, None)
            self.remove_resource(node.collection.id)

    def mark_as_stale(self, ss, context=None):
        context = context if context else self.context
        # context.add_tag('STALE')
        self.stale_commits[ss.collection.id][ss.id] = ss
        stale_cb = partial(self.stale_cb)
        ss.register_cb(stale_cb)
        ss.unref(context.clone().add_tag('STATE'))

    def drop(self, collection, context=None):
        while True:
            ret = self._drop(collection, context=context)
            if not ret:
                break

    def _drop(self, collection, context=None):
        cid = collection
        if isinstance(cid, Collections):
            cid = cid.id
        head = self.heads.pop(cid, None)
        if not head:
            return
        # self.resources[cid].pop(head.id, None)
        self.remove_resource(cid, head.id)
        next_node = head.next
        if not next_node:
            tail = self.tails.pop(cid, None)
            assert head == tail
            self.mark_as_stale(head, context=context)
            return
        next_node.prev = None
        self.heads[cid] = next_node

        self.mark_as_stale(head, context=context)

        return self.resources.get(cid, None) is not None

    def append(self, commit, context=None):
        context = context if context else self.context
        cid = commit.collection_id
        wrapper = self.update_level2_resource(cid, commit.id, commit, context=context)
        # context.add_tag('APPEND')
        wrapper.ref(context.clone().add_tag('APPEND'))
        tail = self.tails.get(cid, None)
        if tail:
            assert wrapper.id > tail.id
            wrapper.set_prev(tail)
            tail.set_next(wrapper)

        head = self.heads.get(cid, None)
        if not head:
            self.heads[cid] = wrapper

        self.tails[cid] = wrapper
        # self.resources[cid][wrapper.id] = wrapper
        self.add_resource(cid, wrapper.id, wrapper)

        if len(self.resources[cid]) > self.keeps:
            self._drop(cid)

    def active_commits(self, collection, context=None):
        cid = collection
        if not isinstance(cid, int):
            cid = cid.id
        return sorted(self.resources[cid].keys()), list(self.stale_commits[cid].keys())


class SchemaCommitsMgr(AdvancedLevel2ResourceMgr, MappingsMixin):
    level_one_model = Collections
    level_two_model = SchemaCommits
    link_key = 'collection_id'
    depend_resource_attrs = frozenset(['mappings'])

    def get_default_level2_id(self, level1_id, context=None):
        return True, max(map(lambda x: x.id, self.resources[level1_id].values()))


class PartitionCommitsMgr(AdvancedLevel2ResourceMgr, MappingsMixin):
    level_one_model = Collections
    level_two_model = PartitionCommits
    link_key = 'collection_id'
    view_key = 'partition_id'
    depend_resource_attrs = frozenset(['mappings', 'collection_id', 'partition_id'])

    def update_depend_mappings_resource(self, resource, context=None):
        for mapping_resource_id in resource.mappings:
            self.default_update_depend_resource(attr='mappings', resource=resource,
                    level1_id=resource.partition_id,
                    level2_id=mapping_resource_id, context=context)


class PartitionsMgr(Level2ResourceMgr):
    level_one_model = Collections
    level_two_model = Partitions
    link_key = 'collection_id'


class CollectionCommitsMgr(MVMixin, AdvancedLevel2ResourceMgr, MappingsMixin):
    level_one_model = Collections
    level_two_model = CollectionCommits
    link_key = 'collection_id'
    depend_resource_attrs = frozenset(['mappings', 'collection_id'])

    def __init__(self, db, depend_resource_mgrs, keeps=1, **kwargs):
        MVMixin.__init__(self, keeps, **kwargs)
        AdvancedLevel2ResourceMgr.__init__(self, db, depend_resource_mgrs, **kwargs)


ELEMENT_RAW = 1
ELEMENT_SQ8 = 2

@singleton
class DBDataManager:
    def __init__(self, db, snapshots=1, event_handler=None, **kwargs):
        self.context = ReferenceContext(f'{self.__class__.__name__}')
        self.fields_mgr = FieldsMgr(db, event_handler=event_handler)
        self.field_elements_mgr = FieldElementsMgr(db, event_handler=event_handler)
        self.field_commits_mgr = FieldCommitsMgr(db, {'field_id': self.fields_mgr, 'mappings': self.field_elements_mgr},
                event_handler=event_handler)
        self.schema_mgr = SchemaCommitsMgr(db, {'mappings': self.field_commits_mgr},
                event_handler=event_handler)

        self.seg_files_mgr = SegmentFilesMgr(db, {'field_element_id': self.field_elements_mgr},
                event_handler=event_handler)
        self.segs_mgr = SegmentsMgr(db)
        self.seg_commits_mgr = SegmentsCommitsMgr(db, {'segment_id': self.segs_mgr, 'mappings': self.seg_files_mgr,
            'schema_id': self.schema_mgr}, event_handler=event_handler)
        self.partitions_mgr = PartitionsMgr(db, event_handler=event_handler)

        self.collection_mgr = CollectionsMgr(db, notify_listners=[self.schema_mgr.load], event_handler=event_handler)

        self.partition_commits_mgr = PartitionCommitsMgr(db, {'mappings': self.seg_commits_mgr,
            'collection_id': self.collection_mgr, 'partition_id': self.partitions_mgr}, event_handler=event_handler)

        self.collection_commit_mgr = CollectionCommitsMgr(db, {'mappings': self.partition_commits_mgr,
            'collection_id': self.collection_mgr}, event_handler=event_handler)

        lsn = db.Session.query(LogSequenceNumbers).order_by(LogSequenceNumbers.lsn.desc()).first()
        if not lsn:
            set_lsn(0)
        else:
            set_lsn(lsn.lsn)

        self.db = db
        self.event_handler = event_handler

    def create_collection(self, collection_params, lsn=None, context=None):
        lsn = get_lsn() if lsn is None else lsn
        name = collection_params['name']
        version = collection_params.get('version', {})
        fields = collection_params.get('fields')

        collection = Collections(name=name)
        partition = Partitions(collection=collection, name='_default')
        Commit(collection, partition)

        f_commits = []
        for idx, field in enumerate(fields):
            name = field['name']
            ftype = field['ftype']
            num = idx
            params = field.get('params', {})
            f = Fields(name=name, num=num, ftype=ftype, params=params, collection=collection)
            fe = FieldElements(field=f, ftype=ELEMENT_RAW, name='raw', collection=collection)
            mappings = [fe]
            elements = field.get('elements', [])
            for element in elements:
                ele_name = element['name']
                ele_ftype = element['ftype']
                ele_params = element.get('params', {})
                fee = FieldElements(field=f, ftype=ele_ftype, name=ele_name, params=ele_params, collection=collection)
                mappings.append(fee)
            Commit(f, *mappings)
            f_commit = FieldCommits(field=f, mappings=[ele.id for ele in mappings], collection=collection)
            f_commits.append(f_commit)
            Commit(f_commit)

        schema_commit = SchemaCommits(collection=collection, mappings=[f_commit.id for f_commit in f_commits])
        partition_commit = PartitionCommits(collection=collection, mappings=[], partition=partition)

        Commit(schema_commit, partition_commit)

        lsn_entry = LogSequenceNumbers(lsn=lsn)
        collection.activate()
        collection_commit = CollectionCommits(collection=collection, mappings=[partition_commit.id])
        Commit(lsn_entry, collection, collection_commit)

        # self.partition_commits_mgr.append(partition_commit, context=context)
        self.collection_commit_mgr.append(collection_commit, context=context)
        self.collection_mgr.append(collection, context=context)
        return collection

    def create_partition(self, collection_id, partition_name, lsn=None, context=None, **kwargs):
        lsn = get_lsn() if lsn is None else lsn
        collection = self.collection_mgr.get(collection_id)
        if not collection:
            logger.error(f'Collection {collection_id} not found')
            return

        partition = Partitions(name=partition_name, collection=collection)
        Commit(partition)

        partition_commit = PartitionCommits(collection=collection, mappings=[], partition=partition)
        Commit(partition_commit)

        ids, _ = self.collection_commit_mgr.active_commits(collection)
        if not ids:
            prev_cc = None
        else:
            idx = ids[-1]
            prev_cc = self.collection_commit_mgr.get(collection_id, idx)

        mappings = prev_cc.mappings if prev_cc else []
        mappings.append(partition_commit.id)

        lsn_entry = LogSequenceNumbers(lsn=lsn)
        collection_commit = CollectionCommits(collection=collection, mappings=mappings)
        Commit(lsn_entry, collection_commit)

        prev_cc and self.collection_commit_mgr.release(prev_cc)
        self.collection_mgr.release(collection)

        # self.partition_commits_mgr.append(partition_commit, context=context)
        self.collection_commit_mgr.append(collection_commit, context=context)

        return partition

    def get_snapshot(self, collection_id, context=None):
        # return self.partition_commits_mgr.get(collection_id, context=context)
        return self.collection_commit_mgr.get(collection_id, context=context)

    def release_snapshot(self, snapshot, context=None):
        assert isinstance(snapshot.node, CollectionCommits)
        context = context.clone() if context else self.context.clone()
        context.add_tag('RELEASE[SS]')
        snapshot.unref(context)

    def get_schema(self, collection_id, context=None):
        context = context.clone() if context else self.context.clone()
        context.add_tag('GET[SC]')
        return self.schema_mgr.get(collection_id, context=context)

    def release_schema(self, schema, context=None):
        assert isinstance(schema.node, SchemaCommits)
        context = context if context else self.context
        context.add_tag('RELEASE[SC]')
        schema.unref(context)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')
    f_mgr = FieldsMgr(db=db)
    logger.error(f_mgr.resources[1])

    fe_mgr = FieldElementsMgr(db=db)
    logger.error(fe_mgr.resources[1])

    fc_mgr = FieldCommitsMgr(db, {'field_id': f_mgr, 'mappings': fe_mgr})

    sc_mgr = SchemaCommitsMgr(db, {'mappings': fc_mgr})

    segf_mgr = SegmentFilesMgr(db, {'field_element_id': fe_mgr})
    seg_mgr = SegmentsMgr(db=db)
    sc_mgr = SegmentsCommitsMgr(db, {'segment_id': seg_mgr, 'mappings': segf_mgr})

    cc_mgr = PartitionCommitsMgr(db, {'mappings': sc_mgr})

    c_mgr = CollectionsMgr(db, [cc_mgr.load, sc_mgr.load])
